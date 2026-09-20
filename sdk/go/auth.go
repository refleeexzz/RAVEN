package raven

import (
	"context"
	"fmt"
	"net/http"
)

// RegisterRequest is the body of POST /api/auth/register. Password must be
// 8–72 characters; DisplayName is optional.
type RegisterRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name,omitempty"`
}

// RegisterResponse is the answer to a successful registration.
type RegisterResponse struct {
	UserID string `json:"user_id"`
	Email  string `json:"email"`
}

// Register creates a USER-role account. It does not log the user in —
// call Login next to get a token pair.
func (c *Client) Register(ctx context.Context, req RegisterRequest) (*RegisterResponse, error) {
	var out RegisterResponse
	if err := c.do(ctx, http.MethodPost, "/api/auth/register", req, &out, false); err != nil {
		return nil, err
	}
	return &out, nil
}

// Login exchanges email+password for a token pair and stores it on the
// client: every later call is authenticated and auto-refreshed.
func (c *Client) Login(ctx context.Context, email, password string) (*TokenPair, error) {
	body := map[string]string{"email": email, "password": password}
	var pair TokenPair
	if err := c.do(ctx, http.MethodPost, "/api/auth/login", body, &pair, false); err != nil {
		return nil, err
	}
	c.setTokens(&pair)
	return &pair, nil
}

// Refresh rotates the stored refresh token for a fresh pair and stores it.
// Normally you never call this — the client refreshes on its own — but it
// is exposed for explicit session management.
func (c *Client) Refresh(ctx context.Context) (*TokenPair, error) {
	if !c.canRefresh() {
		return nil, &Error{Code: "no_refresh_token", Message: "client has no refresh token"}
	}
	if err := c.refreshNow(ctx); err != nil {
		return nil, err
	}
	return c.Tokens(), nil
}

// Logout kills the session behind the stored refresh token and clears the
// local pair. The API is idempotent: unknown or already-revoked sessions
// still return ok.
func (c *Client) Logout(ctx context.Context) error {
	c.mu.Lock()
	refreshToken := ""
	if c.tokens != nil {
		refreshToken = c.tokens.RefreshToken
	}
	c.mu.Unlock()

	if refreshToken != "" {
		body := map[string]string{"refresh_token": refreshToken}
		if err := c.do(ctx, http.MethodPost, "/api/auth/logout", body, nil, false); err != nil {
			return fmt.Errorf("raven: logout: %w", err)
		}
	}
	c.mu.Lock()
	c.tokens = nil
	c.mu.Unlock()
	return nil
}
