// handlers_auth.go translates the public auth REST endpoints into
// auth.AuthService gRPC calls. These routes are the only ones a client can
// reach without a token (logout still requires one — see the route table).
package gateway

import (
	"context"
	"encoding/json"
	"net/http"

	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// maxJSONBody caps request bodies at 1 MiB. Everything the API accepts is
// tiny; this is just a safety net against junk payloads.
const maxJSONBody = 1 << 20

// writeJSON renders body with the given status.
func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// decodeJSON reads a single JSON object from the request body. Unknown
// fields are rejected so typos fail loudly instead of being ignored.
func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, maxJSONBody))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return errors.E(errors.KindInvalid, "invalid_json",
			"request body must be valid JSON", err)
	}
	if dec.More() {
		return errors.E(errors.KindInvalid, "invalid_json",
			"request body must contain a single JSON document", nil)
	}
	return nil
}

// authHandlers serves /api/auth/*.
type authHandlers struct {
	auth   *upstream
	client genauth.AuthServiceClient
}

func newAuthHandlers(auth *upstream) *authHandlers {
	return &authHandlers{
		auth:   auth,
		client: genauth.NewAuthServiceClient(auth.conn),
	}
}

// tokenPairJSON is the wire shape for login and refresh responses.
type tokenPairJSON struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	AccessExpiresAt  int64  `json:"access_expires_at"`
	RefreshExpiresAt int64  `json:"refresh_expires_at"`
}

func tokenPairToJSON(p *genauth.TokenPair) tokenPairJSON {
	return tokenPairJSON{
		AccessToken:      p.GetAccessToken(),
		RefreshToken:     p.GetRefreshToken(),
		AccessExpiresAt:  p.GetAccessExpiresAt(),
		RefreshExpiresAt: p.GetRefreshExpiresAt(),
	}
}

type registerRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
}

// register handles POST /api/auth/register.
func (h *authHandlers) register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	var resp *genauth.RegisterResponse
	err := h.auth.call(r.Context(), "Register", false, func(ctx context.Context) error {
		var err error
		resp, err = h.client.Register(ctx, &genauth.RegisterRequest{
			Email:       req.Email,
			Password:    req.Password,
			DisplayName: req.DisplayName,
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{
		"user_id": resp.GetUserId(),
		"email":   resp.GetEmail(),
	})
}

type loginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// login handles POST /api/auth/login.
func (h *authHandlers) login(w http.ResponseWriter, r *http.Request) {
	var req loginRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	var pair *genauth.TokenPair
	err := h.auth.call(r.Context(), "Login", false, func(ctx context.Context) error {
		var err error
		pair, err = h.client.Login(ctx, &genauth.LoginRequest{
			Email:    req.Email,
			Password: req.Password,
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, tokenPairToJSON(pair))
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

// refresh handles POST /api/auth/refresh.
func (h *authHandlers) refresh(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	var pair *genauth.TokenPair
	err := h.auth.call(r.Context(), "Refresh", false, func(ctx context.Context) error {
		var err error
		pair, err = h.client.Refresh(ctx, &genauth.RefreshRequest{RefreshToken: req.RefreshToken})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, tokenPairToJSON(pair))
}

// logout handles POST /api/auth/logout. The route requires a valid access
// token; the body carries the refresh token whose session should die.
func (h *authHandlers) logout(w http.ResponseWriter, r *http.Request) {
	var req refreshRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	var resp *genauth.LogoutResponse
	err := h.auth.call(r.Context(), "Logout", false, func(ctx context.Context) error {
		var err error
		resp, err = h.client.Logout(ctx, &genauth.LogoutRequest{RefreshToken: req.RefreshToken})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": resp.GetOk()})
}
