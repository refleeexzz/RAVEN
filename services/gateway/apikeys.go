// apikeys.go is the API-key credential store (migration 000007). API keys
// are the machine alternative to JWTs: `Authorization: ApiKey rav_live_...`
// authenticates with the key's scopes as the permission set.
//
// Key format: "rav_live_" + base64url (no padding) of 32 crypto/rand bytes —
// 256 bits of entropy, 52 chars total. Only the hex SHA-256 of the full key
// is stored; the plaintext leaves the edge exactly once, in the create
// response. Keys are high-entropy, so lookup is an indexed hash-equality
// read (a slow password KDF would buy nothing here and cost latency on every
// call). A failed lookup still runs one constant-time compare against a
// dummy hash so "key exists or not" does not leak through response shape —
// the database round-trip dominates the timing either way, but the compare
// keeps the code path uniform.
package gateway

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	stderrors "errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// API key format constants. The prefix is part of the public contract (it
// lets ops and users spot a RAVEN key in logs/configs); keyBodyBytes is the
// raw entropy and keyPrefixLen the cleartext hint stored for display.
const (
	apiKeyPrefix    = "rav_live_"
	apiKeyBodyBytes = 32 // 256 bits, crypto/rand
	apiKeyPrefixLen = 8
)

// APIKey is one row of api_keys. The key hash is deliberately NOT part of
// this struct: nothing outside the store ever needs it.
type APIKey struct {
	ID         string
	OwnerID    string
	Name       string
	Prefix     string // first 8 chars of the key body, cleartext display hint
	Scopes     []string
	CreatedAt  time.Time
	LastUsedAt *time.Time
}

// apiKeyStore is the slice of persistence the middleware and handlers need.
// The real implementation is pgxAPIKeyStore; tests use a fake.
type apiKeyStore interface {
	// Create validates nothing (the handler owns validation); it generates
	// the key, stores the hash and returns the row plus the plaintext key —
	// the ONLY place the plaintext ever exists outside the client.
	Create(ctx context.Context, ownerID, name string, scopes []string) (*APIKey, string, error)
	// List returns the owner's ACTIVE keys, newest first. Revoked keys are
	// gone from every listing (the row stays for the audit trail only).
	List(ctx context.Context, ownerID string) ([]*APIKey, error)
	// Resolve authenticates a presented key: active row matching the hash,
	// or KindUnauthorized (api_key_invalid) for unknown/revoked/malformed.
	Resolve(ctx context.Context, key string) (*APIKey, error)
	// Revoke soft-deletes the key (sets revoked_at). KindNotFound when the
	// key does not exist, is already revoked, or belongs to someone else and
	// the requester is not an admin — a foreign key must be
	// indistinguishable from a missing one.
	Revoke(ctx context.Context, id, requesterID string, requesterAdmin bool) error
	// Touch best-effort-updates last_used_at. Called asynchronously; errors
	// are only logged.
	Touch(ctx context.Context, id string) error
}

// ---------------------------------------------------------------------------
// Key generation and hashing
// ---------------------------------------------------------------------------

// generateAPIKey mints a new key and returns it with its display prefix and
// storage hash. The plaintext key is the only secret here; prefix and hash
// are safe to persist.
func generateAPIKey() (key, prefix, hash string, err error) {
	raw := make([]byte, apiKeyBodyBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", "", "", errors.E(errors.KindUnknown, "api_key_generate_failed",
			"could not generate the api key", err)
	}
	body := base64.RawURLEncoding.EncodeToString(raw)
	key = apiKeyPrefix + body
	return key, body[:apiKeyPrefixLen], apiKeyHash(key), nil
}

// apiKeyHash is the storage form of a key: hex SHA-256 of the full string.
func apiKeyHash(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])
}

// wellFormedAPIKey reports whether key has the exact rav_live_<43 base64url>
// shape. Malformed keys are rejected before touching the database.
func wellFormedAPIKey(key string) bool {
	body, ok := strings.CutPrefix(key, apiKeyPrefix)
	if !ok || len(body) != base64.RawURLEncoding.EncodedLen(apiKeyBodyBytes) {
		return false
	}
	_, err := base64.RawURLEncoding.DecodeString(body)
	return err == nil
}

// errAPIKeyInvalid is the single answer for every failed key presentation:
// unknown, revoked and malformed keys are indistinguishable (no oracle).
func errAPIKeyInvalid() error {
	return errors.E(errors.KindUnauthorized, "api_key_invalid",
		"api key is invalid or revoked", nil)
}

// dummyKeyHash is compared against the presented hash when the lookup finds
// nothing, so every failure path performs the same compare work.
var dummyKeyHash = func() []byte {
	sum := sha256.Sum256([]byte(apiKeyPrefix + "dummy"))
	return sum[:]
}()

// ---------------------------------------------------------------------------
// Validation (handler-side)
// ---------------------------------------------------------------------------

// issuableScopes is the permission vocabulary a key may carry: the exact
// platform permissions, WITHOUT the admin:* wildcard. A key with admin:*
// would be a root credential that never expires — that is exactly what API
// keys exist to avoid.
var issuableScopes = map[string]bool{
	ravenauth.PermUsersRead:   true,
	ravenauth.PermUsersWrite:  true,
	ravenauth.PermUsersDelete: true,
	ravenauth.PermJobsCreate:  true,
	ravenauth.PermJobsRead:    true,
	ravenauth.PermJobsCancel:  true,
}

// validateScopes enforces: at least one scope, no duplicates, every scope a
// known exact permission, never the admin wildcard.
func validateScopes(scopes []string) error {
	if len(scopes) == 0 {
		return errors.E(errors.KindInvalid, "scopes_required",
			"scopes must name at least one permission (e.g. jobs:read)", nil)
	}
	seen := make(map[string]bool, len(scopes))
	for _, s := range scopes {
		s = strings.TrimSpace(s)
		if s == ravenauth.PermAdminAll {
			return errors.E(errors.KindInvalid, "scope_not_issuable",
				"admin:* cannot be issued to an api key", nil)
		}
		if !issuableScopes[s] {
			return errors.E(errors.KindInvalid, "scope_unknown",
				"unknown scope "+s, nil)
		}
		if seen[s] {
			return errors.E(errors.KindInvalid, "scope_duplicate",
				"duplicate scope "+s, nil)
		}
		seen[s] = true
	}
	return nil
}

// validateKeyName bounds the display name. 100 chars matches the console's
// rendering budget; empty names make key lists useless.
func validateKeyName(name string) error {
	n := strings.TrimSpace(name)
	if n == "" {
		return errors.E(errors.KindInvalid, "name_required", "name is required", nil)
	}
	if len(n) > 100 {
		return errors.E(errors.KindInvalid, "name_too_long",
			"name must be at most 100 characters", nil)
	}
	return nil
}

// ---------------------------------------------------------------------------
// pgx store
// ---------------------------------------------------------------------------

// pgxAPIKeyStore is the Postgres-backed apiKeyStore.
type pgxAPIKeyStore struct {
	pool *pgxpool.Pool
}

func newAPIKeyStore(pool *pgxpool.Pool) *pgxAPIKeyStore {
	return &pgxAPIKeyStore{pool: pool}
}

const apiKeyColumns = `id::text, owner_id::text, name, key_prefix, scopes, created_at, last_used_at`

func scanAPIKey(row pgx.Row) (*APIKey, error) {
	var k APIKey
	if err := row.Scan(&k.ID, &k.OwnerID, &k.Name, &k.Prefix, &k.Scopes,
		&k.CreatedAt, &k.LastUsedAt); err != nil {
		return nil, err
	}
	return &k, nil
}

func (s *pgxAPIKeyStore) Create(ctx context.Context, ownerID, name string, scopes []string) (*APIKey, string, error) {
	key, prefix, hash, err := generateAPIKey()
	if err != nil {
		return nil, "", err
	}
	k, err := scanAPIKey(s.pool.QueryRow(ctx, `
		INSERT INTO api_keys (owner_id, name, key_prefix, key_hash, scopes)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING `+apiKeyColumns,
		ownerID, name, prefix, hash, scopes))
	if err != nil {
		return nil, "", errors.E(errors.KindUnknown, "api_key_create_failed",
			"could not store the api key", err)
	}
	return k, key, nil
}

func (s *pgxAPIKeyStore) List(ctx context.Context, ownerID string) ([]*APIKey, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT `+apiKeyColumns+`
		FROM api_keys
		WHERE owner_id = $1 AND revoked_at IS NULL
		ORDER BY created_at DESC`, ownerID)
	if err != nil {
		return nil, errors.E(errors.KindUnknown, "api_key_list_failed",
			"could not list api keys", err)
	}
	defer rows.Close()

	out := make([]*APIKey, 0)
	for rows.Next() {
		k, err := scanAPIKey(rows)
		if err != nil {
			return nil, errors.E(errors.KindUnknown, "api_key_list_failed",
				"could not read an api key row", err)
		}
		out = append(out, k)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.E(errors.KindUnknown, "api_key_list_failed",
			"could not list api keys", err)
	}
	return out, nil
}

func (s *pgxAPIKeyStore) Resolve(ctx context.Context, key string) (*APIKey, error) {
	if !wellFormedAPIKey(key) {
		// Same compare work as the found-and-checked path: keep the failure
		// shape uniform.
		subtle.ConstantTimeCompare([]byte(key), []byte(apiKeyPrefix+"dummy"))
		return nil, errAPIKeyInvalid()
	}
	hash := apiKeyHash(key)
	k, err := scanAPIKey(s.pool.QueryRow(ctx, `
		SELECT `+apiKeyColumns+`
		FROM api_keys
		WHERE key_hash = $1 AND revoked_at IS NULL`, hash))
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			subtle.ConstantTimeCompare([]byte(hash), dummyKeyHash)
			return nil, errAPIKeyInvalid()
		}
		return nil, errors.E(errors.KindUnknown, "api_key_resolve_failed",
			"could not resolve the api key", err)
	}
	return k, nil
}

func (s *pgxAPIKeyStore) Revoke(ctx context.Context, id, requesterID string, requesterAdmin bool) error {
	// One round trip: revoke only when the row is active AND (requester owns
	// it OR requester is an admin). Affected 0 then means missing / already
	// revoked / foreign — all answered 404, never an ownership oracle.
	tag, err := s.pool.Exec(ctx, `
		UPDATE api_keys SET revoked_at = now()
		WHERE id = $1 AND revoked_at IS NULL
		  AND (owner_id = $2 OR $3::bool)`,
		id, requesterID, requesterAdmin)
	if err != nil {
		return errors.E(errors.KindUnknown, "api_key_revoke_failed",
			"could not revoke the api key", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.E(errors.KindNotFound, "api_key_not_found",
			"api key does not exist", nil)
	}
	return nil
}

func (s *pgxAPIKeyStore) Touch(ctx context.Context, id string) error {
	_, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET last_used_at = now() WHERE id = $1 AND revoked_at IS NULL`, id)
	if err != nil {
		return errors.E(errors.KindUnknown, "api_key_touch_failed",
			"could not update last_used_at", err)
	}
	return nil
}
