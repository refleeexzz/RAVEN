package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// fakeAPIKeyStore is an in-memory apiKeyStore. It stores hashes only, like
// the real one, so tests exercise the same resolve-by-hash contract.
type fakeAPIKeyStore struct {
	mu      sync.Mutex
	byHash  map[string]*APIKey
	byID    map[string]*APIKey
	touched []string

	createErr  error
	resolveErr error // injected infrastructure error (not "key invalid")
}

func newFakeAPIKeyStore() *fakeAPIKeyStore {
	return &fakeAPIKeyStore{
		byHash: make(map[string]*APIKey),
		byID:   make(map[string]*APIKey),
	}
}

func (f *fakeAPIKeyStore) Create(_ context.Context, ownerID, name string, scopes []string) (*APIKey, string, error) {
	if f.createErr != nil {
		return nil, "", f.createErr
	}
	key, prefix, hash, err := generateAPIKey()
	if err != nil {
		return nil, "", err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k := &APIKey{
		ID:        "key-" + prefix,
		OwnerID:   ownerID,
		Name:      name,
		Prefix:    prefix,
		Scopes:    append([]string(nil), scopes...),
		CreatedAt: time.Now().UTC(),
	}
	f.byHash[hash] = k
	f.byID[k.ID] = k
	return k, key, nil
}

func (f *fakeAPIKeyStore) List(_ context.Context, ownerID string) ([]*APIKey, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]*APIKey, 0)
	for _, k := range f.byID {
		if k.OwnerID == ownerID {
			out = append(out, k)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (f *fakeAPIKeyStore) Resolve(_ context.Context, key string) (*APIKey, error) {
	if f.resolveErr != nil {
		return nil, f.resolveErr
	}
	if !wellFormedAPIKey(key) {
		return nil, errAPIKeyInvalid()
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.byHash[apiKeyHash(key)]
	if !ok {
		return nil, errAPIKeyInvalid()
	}
	return k, nil
}

func (f *fakeAPIKeyStore) Revoke(_ context.Context, id, requesterID string, requesterAdmin bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	k, ok := f.byID[id]
	if !ok || (k.OwnerID != requesterID && !requesterAdmin) {
		return errors.E(errors.KindNotFound, "api_key_not_found", "api key does not exist", nil)
	}
	delete(f.byID, id)
	delete(f.byHash, apiKeyHashForTest(f, k))
	return nil
}

// apiKeyHashForTest finds k's hash (the fake keeps no plaintext either).
func apiKeyHashForTest(f *fakeAPIKeyStore, k *APIKey) string {
	for h, v := range f.byHash {
		if v == k {
			return h
		}
	}
	return ""
}

func (f *fakeAPIKeyStore) Touch(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.touched = append(f.touched, id)
	return nil
}

func (f *fakeAPIKeyStore) touchedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.touched...)
}

// ---------------------------------------------------------------------------
// Key format
// ---------------------------------------------------------------------------

func TestGenerateAPIKeyFormat(t *testing.T) {
	t.Parallel()

	key, prefix, hash, err := generateAPIKey()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.HasPrefix(key, apiKeyPrefix) {
		t.Errorf("key must start with %q, got %q", apiKeyPrefix, key)
	}
	// rav_live_ (9) + base64url-no-pad of 32 bytes (43) = 52 chars.
	if len(key) != 52 {
		t.Errorf("key length: got %d, want 52", len(key))
	}
	if len(prefix) != apiKeyPrefixLen {
		t.Errorf("prefix length: got %d, want %d", len(prefix), apiKeyPrefixLen)
	}
	if !strings.Contains(key, prefix) {
		t.Errorf("prefix %q must be visible inside the key", prefix)
	}
	if len(hash) != 64 {
		t.Errorf("hash must be hex sha256 (64 chars), got %d", len(hash))
	}
	if hash != apiKeyHash(key) {
		t.Error("hash must be the sha256 of the full key")
	}
	if !wellFormedAPIKey(key) {
		t.Error("generated key must pass wellFormedAPIKey")
	}
}

func TestGenerateAPIKeyUnique(t *testing.T) {
	t.Parallel()

	seen := map[string]bool{}
	for i := 0; i < 100; i++ {
		key, _, _, err := generateAPIKey()
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		if seen[key] {
			t.Fatal("duplicate key generated")
		}
		seen[key] = true
	}
}

func TestWellFormedAPIKeyRejectsJunk(t *testing.T) {
	t.Parallel()

	good, _, _, _ := generateAPIKey()
	body := strings.TrimPrefix(good, apiKeyPrefix)
	cases := map[string]string{
		"valid":         good,
		"no prefix":     body,
		"wrong prefix":  "rav_test_" + body,
		"truncated":     good[:len(good)-1],
		"padded base64": good + "=",
		"bad charset":   apiKeyPrefix + strings.Repeat("!", 43),
		"empty":         "",
		"prefix only":   apiKeyPrefix,
	}
	for name, key := range cases {
		want := name == "valid"
		if got := wellFormedAPIKey(key); got != want {
			t.Errorf("%s: wellFormedAPIKey = %v, want %v", name, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// Scope and name validation
// ---------------------------------------------------------------------------

func TestValidateScopes(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		scopes  []string
		wantErr string // "" = valid
	}{
		{"single", []string{"jobs:read"}, ""},
		{"several", []string{"jobs:read", "jobs:create", "users:read"}, ""},
		{"empty list", nil, "scopes_required"},
		{"admin wildcard", []string{"admin:*"}, "scope_not_issuable"},
		{"unknown", []string{"jobs:fly"}, "scope_unknown"},
		{"namespace wildcard", []string{"jobs:*"}, "scope_unknown"},
		{"duplicate", []string{"jobs:read", "jobs:read"}, "scope_duplicate"},
		{"empty string", []string{""}, "scope_unknown"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := validateScopes(tt.scopes)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || errors.CodeOf(err) != tt.wantErr {
				t.Fatalf("got %v, want code %q", err, tt.wantErr)
			}
			if errors.KindOf(err) != errors.KindInvalid {
				t.Errorf("kind: got %v, want Invalid", errors.KindOf(err))
			}
		})
	}
}

func TestValidateKeyName(t *testing.T) {
	t.Parallel()

	if err := validateKeyName("deploy bot"); err != nil {
		t.Errorf("normal name: %v", err)
	}
	if err := validateKeyName("  "); err == nil {
		t.Error("blank name must be rejected")
	}
	if err := validateKeyName(strings.Repeat("a", 101)); err == nil {
		t.Error("101-char name must be rejected")
	}
	if err := validateKeyName(strings.Repeat("a", 100)); err != nil {
		t.Errorf("100-char name must pass: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Middleware: the ApiKey scheme
// ---------------------------------------------------------------------------

// newKeysAuthnRig wires an authenticator with a fake keys store (no token
// validator needed — these tests never take the bearer path).
func newKeysAuthnRig(t *testing.T, store apiKeyStore) (*authenticator, chan string) {
	t.Helper()
	touchCh := make(chan string, 8)
	a := &authenticator{
		keys:    store,
		cache:   newAuthCache(),
		metrics: testMetrics(t),
		touchCh: touchCh,
	}
	return a, touchCh
}

func apiKeyRequest(cred string) (*httptest.ResponseRecorder, *http.Request) {
	r := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	r.Header.Set("Authorization", "ApiKey "+cred)
	return httptest.NewRecorder(), r
}

func TestAuthNAPIKeySuccess(t *testing.T) {
	t.Parallel()

	store := newFakeAPIKeyStore()
	owner := "11111111-1111-1111-1111-111111111111"
	k, plaintext, err := store.Create(context.Background(), owner, "ci", []string{"jobs:read"})
	if err != nil {
		t.Fatalf("seed key: %v", err)
	}

	a, touchCh := newKeysAuthnRig(t, store)
	var seen Identity
	var seenOK bool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen, seenOK = IdentityFrom(r.Context())
		w.WriteHeader(http.StatusOK)
	})

	rec, req := apiKeyRequest(plaintext)
	a.middleware(next).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200 (%s)", rec.Code, rec.Body.String())
	}
	if !seenOK {
		t.Fatal("identity must be in context")
	}
	if seen.UserID != owner {
		t.Errorf("UserID: got %q, want %q", seen.UserID, owner)
	}
	if seen.AuthVia != authViaAPIKey {
		t.Errorf("AuthVia: got %q, want %q", seen.AuthVia, authViaAPIKey)
	}
	if seen.KeyID != k.ID {
		t.Errorf("KeyID: got %q, want %q", seen.KeyID, k.ID)
	}
	if len(seen.Perms) != 1 || seen.Perms[0] != "jobs:read" {
		t.Errorf("Perms: got %v, want [jobs:read]", seen.Perms)
	}

	// The touch is queued asynchronously, never blocking the request.
	select {
	case id := <-touchCh:
		if id != k.ID {
			t.Errorf("touch id: got %q, want %q", id, k.ID)
		}
	case <-time.After(time.Second):
		t.Error("touch was not queued")
	}
}

func TestAuthNAPIKeyRejected(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		store      func() apiKeyStore
		cred       string
		wantStatus int
		wantCode   string
	}{
		{
			name: "unknown key",
			store: func() apiKeyStore {
				return newFakeAPIKeyStore()
			},
			cred:       apiKeyPrefix + strings.Repeat("a", 43),
			wantStatus: http.StatusUnauthorized,
			wantCode:   "api_key_invalid",
		},
		{
			name: "malformed key",
			store: func() apiKeyStore {
				return newFakeAPIKeyStore()
			},
			cred:       "not-a-key",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "api_key_invalid",
		},
		{
			name: "empty credential",
			store: func() apiKeyStore {
				return newFakeAPIKeyStore()
			},
			cred:       "",
			wantStatus: http.StatusUnauthorized,
			wantCode:   "invalid_authorization",
		},
		{
			name: "store unavailable",
			store: func() apiKeyStore {
				return nil // keys not configured
			},
			cred:       apiKeyPrefix + strings.Repeat("a", 43),
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "api_keys_unavailable",
		},
		{
			name: "store error is a 500, never a 401 oracle",
			store: func() apiKeyStore {
				f := newFakeAPIKeyStore()
				f.resolveErr = errors.E(errors.KindUnknown, "db_down", "postgres is gone", nil)
				return f
			},
			cred:       apiKeyPrefix + strings.Repeat("a", 43),
			wantStatus: http.StatusInternalServerError,
			wantCode:   "db_down",
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, _ := newKeysAuthnRig(t, tt.store())
			next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				t.Error("handler must not run on failed key auth")
				w.WriteHeader(http.StatusOK)
			})

			rec, req := apiKeyRequest(tt.cred)
			a.middleware(next).ServeHTTP(rec, req)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status: got %d, want %d (%s)", rec.Code, tt.wantStatus, rec.Body.String())
			}
			var env errorEnvelope
			if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
				t.Fatalf("decode envelope: %v", err)
			}
			if env.Error.Code != tt.wantCode {
				t.Errorf("code: got %q, want %q", env.Error.Code, tt.wantCode)
			}
		})
	}
}

func TestAuthNAPIKeyRevokedStopsWorking(t *testing.T) {
	t.Parallel()

	store := newFakeAPIKeyStore()
	owner := "11111111-1111-1111-1111-111111111111"
	k, plaintext, _ := store.Create(context.Background(), owner, "ci", []string{"jobs:read"})

	a, _ := newKeysAuthnRig(t, store)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec, req := apiKeyRequest(plaintext)
	a.middleware(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("before revoke: got %d, want 200", rec.Code)
	}

	if err := store.Revoke(context.Background(), k.ID, owner, false); err != nil {
		t.Fatalf("revoke: %v", err)
	}

	// No cache on the key path: revocation is effective immediately.
	rec, req = apiKeyRequest(plaintext)
	a.middleware(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("after revoke: got %d, want 401", rec.Code)
	}
}

func TestAuthNSchemeSelection(t *testing.T) {
	t.Parallel()

	// Bearer still takes the JWT path; ApiKey takes the keys path; other
	// schemes keep the historical invalid_authorization error.
	fv := &fakeValidator{id: Identity{UserID: "u-jwt"}}
	store := newFakeAPIKeyStore()
	touchCh := make(chan string, 8)
	a := &authenticator{
		validator: fv,
		keys:      store,
		cache:     newAuthCache(),
		metrics:   testMetrics(t),
		touchCh:   touchCh,
	}
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	r := httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	r.Header.Set("Authorization", "Bearer some-jwt")
	rec := httptest.NewRecorder()
	a.middleware(next).ServeHTTP(rec, r)
	if rec.Code != http.StatusOK {
		t.Fatalf("bearer path: got %d, want 200", rec.Code)
	}
	if fv.callCount() != 1 {
		t.Errorf("bearer must hit the token validator, calls = %d", fv.callCount())
	}

	r = httptest.NewRequest(http.MethodGet, "/api/jobs", nil)
	r.Header.Set("Authorization", "Basic abc123")
	rec = httptest.NewRecorder()
	a.middleware(next).ServeHTTP(rec, r)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("basic scheme: got %d, want 401", rec.Code)
	}
	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Error.Code != "invalid_authorization" {
		t.Errorf("basic scheme code: got %q, want invalid_authorization", env.Error.Code)
	}
}

// TestAuthNAPIKeyRateLimitIdentity pins the interaction between ApiKey auth
// and the limiter: a keyed identity gets a per-user bucket, not the IP one.
func TestAuthNAPIKeyRateLimitIdentity(t *testing.T) {
	t.Parallel()

	store := newFakeAPIKeyStore()
	owner := "11111111-1111-1111-1111-111111111111"
	_, plaintext, _ := store.Create(context.Background(), owner, "ci", []string{"jobs:read"})

	a, _ := newKeysAuthnRig(t, store)
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := rateLimitKey(r); got != "user:"+owner {
			t.Errorf("rateLimitKey: got %q, want user:%s", got, owner)
		}
		w.WriteHeader(http.StatusOK)
	})

	rec, req := apiKeyRequest(plaintext)
	a.middleware(next).ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// Handlers
// ---------------------------------------------------------------------------

// withAuthIdentity builds a request carrying an identity (JWT by default).
func withAuthIdentity(r *http.Request, userID string, perms []string) *http.Request {
	return r.WithContext(withIdentity(r.Context(), Identity{
		UserID: userID, Perms: perms, AuthVia: authViaJWT,
	}))
}

func TestKeysCreateReturnsPlaintextOnce(t *testing.T) {
	t.Parallel()

	store := newFakeAPIKeyStore()
	h := newKeysHandlers(store)

	r := httptest.NewRequest(http.MethodPost, "/api/keys",
		strings.NewReader(`{"name":"ci bot","scopes":["jobs:read","jobs:create"]}`))
	r = withAuthIdentity(r, "user-1", nil)
	rec := httptest.NewRecorder()
	h.create(rec, r)

	if rec.Code != http.StatusCreated {
		t.Fatalf("got %d, want 201 (%s)", rec.Code, rec.Body.String())
	}
	var body struct {
		Key    string     `json:"key"`
		APIKey apiKeyJSON `json:"api_key"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !wellFormedAPIKey(body.Key) {
		t.Errorf("key %q must be a well-formed rav_live_ key", body.Key)
	}
	if body.APIKey.Name != "ci bot" {
		t.Errorf("name: got %q", body.APIKey.Name)
	}
	if body.APIKey.Prefix != body.Key[:len(apiKeyPrefix)+apiKeyPrefixLen] {
		t.Errorf("prefix: got %q, want the key's first %d chars", body.APIKey.Prefix, len(apiKeyPrefix)+apiKeyPrefixLen)
	}
	// The hash must never leave the store boundary.
	if strings.Contains(rec.Body.String(), apiKeyHash(body.Key)) {
		t.Error("response must not contain the key hash")
	}
}

func TestKeysCreateIsJWTOnly(t *testing.T) {
	t.Parallel()

	store := newFakeAPIKeyStore()
	h := newKeysHandlers(store)

	r := httptest.NewRequest(http.MethodPost, "/api/keys",
		strings.NewReader(`{"name":"x","scopes":["jobs:read"]}`))
	r = r.WithContext(withIdentity(r.Context(), Identity{
		UserID: "user-1", AuthVia: authViaAPIKey, KeyID: "key-1",
	}))
	rec := httptest.NewRecorder()
	h.create(rec, r)

	if rec.Code != http.StatusForbidden {
		t.Fatalf("got %d, want 403", rec.Code)
	}
	var env errorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("decode envelope: %v", err)
	}
	if env.Error.Code != "api_key_cannot_create_keys" {
		t.Errorf("code: got %q, want api_key_cannot_create_keys", env.Error.Code)
	}
}

func TestKeysCreateValidation(t *testing.T) {
	t.Parallel()

	store := newFakeAPIKeyStore()
	h := newKeysHandlers(store)

	cases := []struct {
		name     string
		body     string
		wantCode string
	}{
		{"no name", `{"scopes":["jobs:read"]}`, "name_required"},
		{"no scopes", `{"name":"x"}`, "scopes_required"},
		{"admin scope", `{"name":"x","scopes":["admin:*"]}`, "scope_not_issuable"},
		{"unknown scope", `{"name":"x","scopes":["jobs:fly"]}`, "scope_unknown"},
		{"unknown field", `{"name":"x","scopes":["jobs:read"],"admin":true}`, "invalid_json"},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "/api/keys", strings.NewReader(tt.body))
			r = withAuthIdentity(r, "user-1", nil)
			rec := httptest.NewRecorder()
			h.create(rec, r)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("got %d, want 400 (%s)", rec.Code, rec.Body.String())
			}
			if !strings.Contains(rec.Body.String(), tt.wantCode) {
				t.Errorf("body must carry code %q, got %s", tt.wantCode, rec.Body.String())
			}
		})
	}
}

func TestKeysListIsOwnerScopedAndHashless(t *testing.T) {
	t.Parallel()

	store := newFakeAPIKeyStore()
	h := newKeysHandlers(store)
	for _, name := range []string{"one", "two"} {
		if _, _, err := store.Create(context.Background(), "user-1", name, []string{"jobs:read"}); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	if _, _, err := store.Create(context.Background(), "user-2", "other", []string{"jobs:read"}); err != nil {
		t.Fatalf("seed: %v", err)
	}

	r := httptest.NewRequest(http.MethodGet, "/api/keys", nil)
	r = withAuthIdentity(r, "user-1", nil)
	rec := httptest.NewRecorder()
	h.list(rec, r)

	if rec.Code != http.StatusOK {
		t.Fatalf("got %d, want 200", rec.Code)
	}
	var body struct {
		APIKeys []apiKeyJSON `json:"api_keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.APIKeys) != 2 {
		t.Fatalf("got %d keys, want 2 (owner-scoped)", len(body.APIKeys))
	}
	for _, k := range body.APIKeys {
		if !strings.HasPrefix(k.Prefix, apiKeyPrefix) {
			t.Errorf("prefix %q must render with the rav_live_ prefix", k.Prefix)
		}
	}
}

func TestKeysRevokeRules(t *testing.T) {
	t.Parallel()

	setup := func() (*fakeAPIKeyStore, *keysHandlers, *APIKey) {
		store := newFakeAPIKeyStore()
		k, _, _ := store.Create(context.Background(), "user-1", "mine", []string{"jobs:read"})
		return store, newKeysHandlers(store), k
	}
	revoke := func(h *keysHandlers, keyID, asUser string, perms []string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodDelete, "/api/keys/"+keyID, nil)
		r.SetPathValue("id", keyID)
		r = withAuthIdentity(r, asUser, perms)
		rec := httptest.NewRecorder()
		h.revoke(rec, r)
		return rec
	}

	t.Run("owner revokes", func(t *testing.T) {
		store, h, k := setup()
		if rec := revoke(h, k.ID, "user-1", nil); rec.Code != http.StatusOK {
			t.Fatalf("got %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
		if keys, _ := store.List(context.Background(), "user-1"); len(keys) != 0 {
			t.Error("revoked key must leave the active list")
		}
	})

	t.Run("foreign key is a 404, not a 403 oracle", func(t *testing.T) {
		_, h, k := setup()
		if rec := revoke(h, k.ID, "user-2", nil); rec.Code != http.StatusNotFound {
			t.Fatalf("got %d, want 404", rec.Code)
		}
	})

	t.Run("admin revokes anyone", func(t *testing.T) {
		_, h, k := setup()
		rec := revoke(h, k.ID, "admin-1", []string{ravenauth.PermUsersDelete})
		if rec.Code != http.StatusOK {
			t.Fatalf("got %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("admin wildcard revokes anyone", func(t *testing.T) {
		_, h, k := setup()
		rec := revoke(h, k.ID, "admin-1", []string{ravenauth.PermAdminAll})
		if rec.Code != http.StatusOK {
			t.Fatalf("got %d, want 200 (%s)", rec.Code, rec.Body.String())
		}
	})

	t.Run("missing key is 404", func(t *testing.T) {
		_, h, _ := setup()
		if rec := revoke(h, "no-such-key", "user-1", nil); rec.Code != http.StatusNotFound {
			t.Fatalf("got %d, want 404", rec.Code)
		}
	})
}

func TestKeysHandlers503WhenDisabled(t *testing.T) {
	t.Parallel()

	h := newKeysHandlers(nil)

	for name, call := range map[string]func(http.ResponseWriter, *http.Request){
		"create": h.create,
		"list":   h.list,
		"revoke": h.revoke,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, "/api/keys", strings.NewReader(`{}`))
			r = withAuthIdentity(r, "user-1", nil)
			rec := httptest.NewRecorder()
			call(rec, r)
			if rec.Code != http.StatusServiceUnavailable {
				t.Fatalf("got %d, want 503", rec.Code)
			}
			if !strings.Contains(rec.Body.String(), "api_keys_unavailable") {
				t.Errorf("want api_keys_unavailable envelope, got %s", rec.Body.String())
			}
		})
	}
}
