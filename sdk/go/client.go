package raven

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// DefaultBaseURL is where a local RAVEN stack serves its public API.
const DefaultBaseURL = "http://localhost:8080"

// DefaultTimeout bounds every HTTP call unless overridden per client.
const DefaultTimeout = 10 * time.Second

// refreshSkew is how early before the stated expiry an access token is
// considered stale, so a request never flies with a token that dies
// mid-flight.
const refreshSkew = 30 * time.Second

// Client talks to the RAVEN gateway's public REST API. It is safe for
// concurrent use; token refreshes are serialized internally so a burst of
// goroutines triggers at most one rotation.
type Client struct {
	baseURL    string
	httpClient *http.Client
	timeout    time.Duration

	// Exactly one credential mode is active: a static API key, or a JWT
	// token pair that auto-refreshes.
	apiKey string

	mu     sync.Mutex // guards tokens
	tokens *TokenPair

	refreshMu sync.Mutex // serializes token rotations per client

	idemKeyFn func() string
}

// Option configures a Client at construction.
type Option func(*Client)

// WithBaseURL points the client at a non-default gateway. A trailing
// slash is trimmed.
func WithBaseURL(u string) Option {
	return func(c *Client) {
		c.baseURL = strings.TrimRight(u, "/")
	}
}

// WithAPIKey authenticates every request with `Authorization: ApiKey <key>`.
// API keys are static machine credentials; nothing is refreshed.
func WithAPIKey(key string) Option {
	return func(c *Client) {
		c.apiKey = key
	}
}

// WithTokenPair seeds the client with an existing token pair, for example
// one persisted from an earlier session. The pair is refreshed
// automatically when the access token nears expiry.
func WithTokenPair(t TokenPair) Option {
	return func(c *Client) {
		cp := t
		c.tokens = &cp
	}
}

// WithTimeout changes the default 10 s per-request timeout. Context
// deadlines always win over this value when tighter.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithHTTPClient swaps the underlying *http.Client (custom transports,
// tracing wrappers, test fakes). When set, WithTimeout is ignored — the
// provided client is used as-is.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			c.httpClient = hc
		}
	}
}

// WithIdempotencyKeyFunc overrides how Idempotency-Key values are
// generated for mutating requests. Mostly useful in tests.
func WithIdempotencyKeyFunc(fn func() string) Option {
	return func(c *Client) {
		if fn != nil {
			c.idemKeyFn = fn
		}
	}
}

// New builds a Client. With no options it targets DefaultBaseURL with a
// 10 s timeout and no credentials (only public routes will work until
// Login or Register is called).
func New(opts ...Option) *Client {
	c := &Client{
		baseURL:   DefaultBaseURL,
		timeout:   DefaultTimeout,
		idemKeyFn: newIdempotencyKey,
	}
	for _, o := range opts {
		o(c)
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: c.timeout}
	}
	return c
}

// newIdempotencyKey returns a random 128-bit hex key (32 chars), well
// under the API's 255-char cap.
func newIdempotencyKey() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; fall back to time
		// so a request is never blocked by entropy trouble.
		return fmt.Sprintf("idem-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b[:])
}

// Tokens returns a copy of the current token pair, or nil when the client
// authenticates with an API key or has not logged in yet. Use it to
// persist the session (and its rotated refresh tokens) across restarts.
func (c *Client) Tokens() *TokenPair {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.tokens == nil {
		return nil
	}
	cp := *c.tokens
	return &cp
}

// setTokens replaces the stored pair.
func (c *Client) setTokens(t *TokenPair) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp := *t
	c.tokens = &cp
}

// ---------------------------------------------------------------------------
// Request plumbing
// ---------------------------------------------------------------------------

// requestOption customizes a single call.
type requestOption func(*requestConfig)

type requestConfig struct {
	idempotencyKey string
	query          url.Values
}

// WithIdempotencyKey pins the Idempotency-Key header for one mutating
// call. Use it when retrying a create after a network timeout: the same
// key always returns the same job.
func WithIdempotencyKey(key string) requestOption {
	return func(rc *requestConfig) {
		rc.idempotencyKey = key
	}
}

// do performs one HTTP request and decodes the JSON body into out (which
// may be nil for empty bodies). mutating controls the automatic
// Idempotency-Key header. Auth is attached, and an expired JWT pair is
// refreshed transparently before the request — plus once more on a 401,
// in case the server clock disagrees.
func (c *Client) do(ctx context.Context, method, path string, body, out any, mutating bool, opts ...requestOption) error {
	var rc requestConfig
	for _, o := range opts {
		o(&rc)
	}

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return fmt.Errorf("raven: encode request: %w", err)
		}
	}

	idemKey := rc.idempotencyKey
	if mutating && idemKey == "" {
		idemKey = c.idemKeyFn()
	}

	retried := false
	for {
		if err := c.ensureFreshToken(ctx); err != nil {
			return err
		}

		req, err := c.newRequest(ctx, method, path, bodyBytes, idemKey, rc.query)
		if err != nil {
			return err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("raven: %s %s: %w", method, path, err)
		}
		respBody, readErr := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
		_ = resp.Body.Close()
		if readErr != nil {
			return fmt.Errorf("raven: read %s %s response: %w", method, path, readErr)
		}

		// One silent retry: the access token died between the freshness
		// check and the server validation.
		if resp.StatusCode == http.StatusUnauthorized && !retried && c.canRefresh() {
			retried = true
			if err := c.refreshNow(ctx); err != nil {
				return err
			}
			continue
		}

		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return decodeError(resp.StatusCode, respBody)
		}

		if out == nil || len(respBody) == 0 {
			return nil
		}
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("raven: decode %s %s response: %w", method, path, err)
		}
		return nil
	}
}

// newRequest builds the raw HTTP request with auth and JSON headers.
func (c *Client) newRequest(ctx context.Context, method, path string, body []byte, idemKey string, query url.Values) (*http.Request, error) {
	u := c.baseURL + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}

	var rdr io.Reader
	if body != nil {
		rdr = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, fmt.Errorf("raven: build request: %w", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if idemKey != "" {
		req.Header.Set("Idempotency-Key", idemKey)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "raven-go-sdk/1.0")

	c.mu.Lock()
	switch {
	case c.apiKey != "":
		req.Header.Set("Authorization", "ApiKey "+c.apiKey)
	case c.tokens != nil && c.tokens.AccessToken != "":
		req.Header.Set("Authorization", "Bearer "+c.tokens.AccessToken)
	}
	c.mu.Unlock()

	return req, nil
}

// canRefresh reports whether a refresh token is available.
func (c *Client) canRefresh() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.tokens != nil && c.tokens.RefreshToken != ""
}

// ensureFreshToken refreshes the pair when the access token is expired or
// within refreshSkew of expiring. No-op for API-key auth.
func (c *Client) ensureFreshToken(ctx context.Context) error {
	c.mu.Lock()
	stale := c.tokens != nil &&
		c.tokens.RefreshToken != "" &&
		time.Now().Add(refreshSkew).After(c.tokens.accessExpiry())
	c.mu.Unlock()

	if !stale {
		return nil
	}
	return c.refreshNow(ctx)
}

// refreshNow rotates the token pair. Concurrent callers block on the
// per-client mutex and the second one notices the pair is already fresh.
func (c *Client) refreshNow(ctx context.Context) error {
	refreshMu := &c.refreshMu
	refreshMu.Lock()
	defer refreshMu.Unlock()

	c.mu.Lock()
	if c.tokens == nil || c.tokens.RefreshToken == "" {
		c.mu.Unlock()
		return nil
	}
	// Another goroutine may have refreshed while we waited.
	if time.Now().Add(refreshSkew).Before(c.tokens.accessExpiry()) {
		c.mu.Unlock()
		return nil
	}
	refreshToken := c.tokens.RefreshToken
	c.mu.Unlock()

	req, err := c.newRequest(ctx, http.MethodPost, "/api/auth/refresh",
		[]byte(fmt.Sprintf(`{"refresh_token":%q}`, refreshToken)), "", nil)
	if err != nil {
		return err
	}
	// The refresh call itself must not carry the (stale) bearer token.
	req.Header.Del("Authorization")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("raven: refresh token: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeError(resp.StatusCode, body)
	}
	var pair TokenPair
	if err := json.Unmarshal(body, &pair); err != nil {
		return fmt.Errorf("raven: decode refresh response: %w", err)
	}
	c.setTokens(&pair)
	return nil
}
