// authn.go is the bearer-auth middleware. Every protected route calls
// auth.ValidateToken over gRPC; because that would hammer the auth service
// on hot paths, successful validations are cached in memory for 30 s keyed
// by the SHA-256 of the token (the raw token never becomes a map key). The
// cache is bounded and swept by a janitor goroutine.
package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"strings"
	"sync"
	"time"

	genauth "github.com/raven/platform/internal/gen/auth"
	"github.com/raven/platform/pkg/errors"
)

// Cache tuning: entries live 30 s (a revoked token stays accepted for at
// most this long — same trade-off the auth service documents for its own
// caches), the map is capped at 10k entries and swept every minute.
const (
	authCacheTTL     = 30 * time.Second
	authCacheMaxSize = 10_000
	authCacheSweep   = time.Minute
)

// Identity is what the auth service says about a valid token.
type Identity struct {
	UserID string
	Email  string
	Roles  []string
	Perms  []string
}

type identityCtxKey struct{}

// withIdentity stores the authenticated identity in the request context.
func withIdentity(ctx context.Context, id Identity) context.Context {
	return context.WithValue(ctx, identityCtxKey{}, id)
}

// IdentityFrom extracts the identity AuthN stored, or ok=false.
func IdentityFrom(ctx context.Context) (Identity, bool) {
	id, ok := ctx.Value(identityCtxKey{}).(Identity)
	return id, ok
}

// tokenValidator validates a bearer token and returns its identity. The real
// implementation talks to the auth service over gRPC; tests use a fake.
type tokenValidator interface {
	ValidateToken(ctx context.Context, token string) (Identity, error)
}

// grpcTokenValidator validates tokens via auth.ValidateToken. ValidateToken
// is read-only, so it counts as idempotent for retries.
type grpcTokenValidator struct {
	auth   *upstream
	client genauth.AuthServiceClient
}

func newGRPCTokenValidator(auth *upstream) *grpcTokenValidator {
	return &grpcTokenValidator{
		auth:   auth,
		client: genauth.NewAuthServiceClient(auth.conn),
	}
}

func (v *grpcTokenValidator) ValidateToken(ctx context.Context, token string) (Identity, error) {
	var resp *genauth.ValidateTokenResponse
	err := v.auth.call(ctx, "ValidateToken", true, func(ctx context.Context) error {
		var err error
		resp, err = v.client.ValidateToken(ctx, &genauth.ValidateTokenRequest{AccessToken: token})
		return err
	})
	if err != nil {
		return Identity{}, err
	}
	if !resp.GetValid() {
		return Identity{}, errors.E(errors.KindUnauthorized, "token_invalid",
			"access token is invalid or expired", nil)
	}
	return Identity{
		UserID: resp.GetUserId(),
		Email:  resp.GetEmail(),
		Roles:  resp.GetRoles(),
		Perms:  resp.GetPermissions(),
	}, nil
}

// authCacheEntry is one cached validation.
type authCacheEntry struct {
	id        Identity
	expiresAt time.Time
}

// authCache is a mutex-guarded map of token hash → identity with expiry.
// now is a clock injection point for tests.
type authCache struct {
	mu      sync.Mutex
	entries map[string]authCacheEntry
	now     func() time.Time
}

func newAuthCache() *authCache {
	return &authCache{
		entries: make(map[string]authCacheEntry),
		now:     time.Now,
	}
}

// tokenCacheKey hashes the token so the raw credential is never stored.
func tokenCacheKey(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

// get returns the cached identity for token, or ok=false on miss/expiry.
func (c *authCache) get(token string) (Identity, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.entries[tokenCacheKey(token)]
	if !ok || !c.now().Before(entry.expiresAt) {
		return Identity{}, false
	}
	return entry.id, true
}

// put caches the identity for token for authCacheTTL. When the map is full
// it sweeps expired entries first; if there is still no room it skips the
// cache entirely instead of evicting live entries at random — correctness
// never depends on the cache.
func (c *authCache) put(token string, id Identity) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.entries) >= authCacheMaxSize {
		c.sweepLocked()
		if len(c.entries) >= authCacheMaxSize {
			return
		}
	}
	c.entries[tokenCacheKey(token)] = authCacheEntry{
		id:        id,
		expiresAt: c.now().Add(authCacheTTL),
	}
}

// sweepLocked deletes expired entries. Caller holds mu.
func (c *authCache) sweepLocked() {
	now := c.now()
	for key, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, key)
		}
	}
}

// sweep runs the janitor until ctx is cancelled.
func (c *authCache) sweep(ctx context.Context) {
	ticker := time.NewTicker(authCacheSweep)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			c.sweepLocked()
			c.mu.Unlock()
		}
	}
}

// authenticator bundles the validator, cache and metrics for the middleware.
type authenticator struct {
	validator tokenValidator
	cache     *authCache
	metrics   *serviceMetrics
}

// bearerToken parses the "Authorization: Bearer <token>" header.
func bearerToken(header string) (string, error) {
	if header == "" {
		return "", errors.E(errors.KindUnauthorized, "missing_authorization",
			"missing Authorization header", nil)
	}
	scheme, token, ok := strings.Cut(header, " ")
	if !ok || !strings.EqualFold(scheme, "Bearer") {
		return "", errors.E(errors.KindUnauthorized, "invalid_authorization",
			`expected "Authorization: Bearer <token>"`, nil)
	}
	token = strings.TrimSpace(token)
	// A token never contains whitespace; "Bearer a b" is malformed.
	if token == "" || strings.ContainsAny(token, " \t") {
		return "", errors.E(errors.KindUnauthorized, "invalid_authorization",
			`expected "Authorization: Bearer <token>"`, nil)
	}
	return token, nil
}

// middleware enforces authentication: parse the bearer token, answer from
// the cache when possible, otherwise call the auth service.
func (a *authenticator) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := bearerToken(r.Header.Get("Authorization"))
		if err != nil {
			writeError(w, r, err)
			return
		}

		if id, ok := a.cache.get(token); ok {
			a.metrics.authCacheHits.Inc()
			next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), id)))
			return
		}
		a.metrics.authCacheMisses.Inc()

		id, err := a.validator.ValidateToken(r.Context(), token)
		if err != nil {
			writeError(w, r, err)
			return
		}
		// Only successful validations are cached; invalid tokens always go
		// back to the auth service so revocation is noticed quickly.
		a.cache.put(token, id)

		next.ServeHTTP(w, r.WithContext(withIdentity(r.Context(), id)))
	})
}
