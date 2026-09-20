package raven

import (
	"context"
	"net/http"
	"net/url"
)

// CreateAPIKeyRequest is the body of POST /api/keys. Scopes must be a
// subset of the platform permissions (users:read, users:write,
// users:delete, jobs:create, jobs:read, jobs:cancel); admin:* is never
// issuable.
type CreateAPIKeyRequest struct {
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
}

// CreateAPIKey mints a new API key. The route is JWT-only: an API key
// cannot create more keys (403 api_key_cannot_create_keys). Save
// CreateAPIKeyResponse.Key immediately — it is shown exactly once.
func (c *Client) CreateAPIKey(ctx context.Context, req CreateAPIKeyRequest, opts ...requestOption) (*CreateAPIKeyResponse, error) {
	var out CreateAPIKeyResponse
	if err := c.do(ctx, http.MethodPost, "/api/keys", req, &out, true, opts...); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListAPIKeys returns the caller's active keys, newest first. Revoked keys
// disappear from the list; the secret hash is never returned.
func (c *Client) ListAPIKeys(ctx context.Context) ([]APIKey, error) {
	var out struct {
		APIKeys []APIKey `json:"api_keys"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/keys", nil, &out, false); err != nil {
		return nil, err
	}
	return out.APIKeys, nil
}

// RevokeAPIKey soft-revokes a key, effective immediately. A key belonging
// to someone else answers 404 api_key_not_found (never 403 — no ownership
// oracle). Admins (users:delete) can revoke anyone's key.
func (c *Client) RevokeAPIKey(ctx context.Context, id string, opts ...requestOption) error {
	return c.do(ctx, http.MethodDelete, "/api/keys/"+url.PathEscape(id), nil, nil, true, opts...)
}
