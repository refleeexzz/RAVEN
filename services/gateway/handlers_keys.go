// handlers_keys.go serves the API-key self-service endpoints:
// POST /api/keys (create, JWT-only — the plaintext key is shown exactly
// once), GET /api/keys (list the caller's active keys, never the hash) and
// DELETE /api/keys/{id} (revoke; owner or admin).
package gateway

import (
	"net/http"
	"strings"
	"time"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// keysHandlers serves /api/keys/*. store is nil when the gateway runs
// without a keys database — every endpoint then answers 503.
type keysHandlers struct {
	store apiKeyStore
}

func newKeysHandlers(store apiKeyStore) *keysHandlers {
	return &keysHandlers{store: store}
}

// errKeysUnavailable is the degraded-mode answer when no keys database is
// configured (same philosophy as the audit trail: the API keeps serving,
// the optional feature is loudly absent).
func errKeysUnavailable() error {
	return errors.E(errors.KindUnavailable, "api_keys_unavailable",
		"api keys are not configured on this gateway", nil)
}

// apiKeyJSON is the public wire shape of a key — everything except the
// hash. The plaintext key only ever appears in the create response.
type apiKeyJSON struct {
	ID         string     `json:"id"`
	Name       string     `json:"name"`
	Prefix     string     `json:"prefix"` // "rav_live_" + 8 visible chars
	Scopes     []string   `json:"scopes"`
	CreatedAt  time.Time  `json:"created_at"`
	LastUsedAt *time.Time `json:"last_used_at"`
}

func apiKeyToJSON(k *APIKey) apiKeyJSON {
	scopes := k.Scopes
	if scopes == nil {
		scopes = []string{}
	}
	return apiKeyJSON{
		ID:         k.ID,
		Name:       k.Name,
		Prefix:     apiKeyPrefix + k.Prefix,
		Scopes:     scopes,
		CreatedAt:  k.CreatedAt.UTC(),
		LastUsedAt: k.LastUsedAt,
	}
}

// createKeyRequest is the body of POST /api/keys.
type createKeyRequest struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

// create handles POST /api/keys. JWT-only: an API key must not mint more
// API keys (a leaked key would become permanent self-renewing access).
// The route only requires authentication; the JWT-only rule lives here.
func (h *keysHandlers) create(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, r, errKeysUnavailable())
		return
	}
	id, ok := IdentityFrom(r.Context())
	if !ok || id.UserID == "" {
		writeError(w, r, errors.E(errors.KindUnauthorized, "unauthenticated",
			"authentication required", nil))
		return
	}
	if id.AuthVia == authViaAPIKey {
		writeError(w, r, errors.E(errors.KindForbidden, "api_key_cannot_create_keys",
			"api keys cannot create api keys, use a JWT", nil))
		return
	}

	var req createKeyRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateKeyName(req.Name); err != nil {
		writeError(w, r, err)
		return
	}
	if err := validateScopes(req.Scopes); err != nil {
		writeError(w, r, err)
		return
	}

	k, plaintext, err := h.store.Create(r.Context(), id.UserID, strings.TrimSpace(req.Name), req.Scopes)
	if err != nil {
		writeError(w, r, err)
		return
	}
	// The plaintext key is returned exactly once. It is never stored, never
	// logged and never listed — lose it and you revoke + re-issue.
	writeJSON(w, http.StatusCreated, map[string]any{
		"key":     plaintext,
		"api_key": apiKeyToJSON(k),
	})
}

// list handles GET /api/keys: the caller's active keys, newest first.
// Owner-scoped for everyone — even admins manage their own keys here.
func (h *keysHandlers) list(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, r, errKeysUnavailable())
		return
	}
	id, ok := IdentityFrom(r.Context())
	if !ok || id.UserID == "" {
		writeError(w, r, errors.E(errors.KindUnauthorized, "unauthenticated",
			"authentication required", nil))
		return
	}

	keys, err := h.store.List(r.Context(), id.UserID)
	if err != nil {
		writeError(w, r, err)
		return
	}
	out := make([]apiKeyJSON, 0, len(keys))
	for _, k := range keys {
		out = append(out, apiKeyToJSON(k))
	}
	writeJSON(w, http.StatusOK, map[string]any{"api_keys": out})
}

// revoke handles DELETE /api/keys/{id}. The owner revokes their own keys;
// an admin (users:delete) revokes anyone's — that is the incident-response
// path for a leaked key. Everything else is 404: a foreign key must be
// indistinguishable from a missing one.
func (h *keysHandlers) revoke(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, r, errKeysUnavailable())
		return
	}
	id, ok := IdentityFrom(r.Context())
	if !ok || id.UserID == "" {
		writeError(w, r, errors.E(errors.KindUnauthorized, "unauthenticated",
			"authentication required", nil))
		return
	}
	keyID := r.PathValue("id")
	if strings.TrimSpace(keyID) == "" {
		writeError(w, r, errors.E(errors.KindInvalid, "api_key_id_required",
			"api key id is required", nil))
		return
	}

	admin := ravenauth.HasPermission(id.Perms, ravenauth.PermUsersDelete)
	if err := h.store.Revoke(r.Context(), keyID, id.UserID, admin); err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}
