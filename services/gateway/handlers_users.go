// handlers_users.go translates the users REST endpoints into
// users.UserService gRPC calls. All routes here require a token plus a
// users:* permission (enforced by the route table, not by these handlers).
package gateway

import (
	"context"
	"net/http"
	"strconv"

	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"
)

// usersHandlers serves /api/users/*.
type usersHandlers struct {
	users  *upstream
	client genusers.UserServiceClient
}

func newUsersHandlers(users *upstream) *usersHandlers {
	return &usersHandlers{
		users:  users,
		client: genusers.NewUserServiceClient(users.conn),
	}
}

// userJSON is the public wire shape of a user.
type userJSON struct {
	ID          string `json:"id"`
	Email       string `json:"email"`
	DisplayName string `json:"display_name"`
	Bio         string `json:"bio"`
	AvatarURL   string `json:"avatar_url"`
	Deleted     bool   `json:"deleted"`
	CreatedAt   int64  `json:"created_at"`
	UpdatedAt   int64  `json:"updated_at"`
}

func userToJSON(u *genusers.User) userJSON {
	return userJSON{
		ID:          u.GetId(),
		Email:       u.GetEmail(),
		DisplayName: u.GetDisplayName(),
		Bio:         u.GetBio(),
		AvatarURL:   u.GetAvatarUrl(),
		Deleted:     u.GetDeleted(),
		CreatedAt:   u.GetCreatedAt(),
		UpdatedAt:   u.GetUpdatedAt(),
	}
}

// pageJSON is the public wire shape of pagination metadata.
type pageJSON struct {
	Page     int32 `json:"page"`
	PageSize int32 `json:"page_size"`
	Total    int64 `json:"total"`
}

// parseIntDefault parses a query param int with a fallback.
func parseIntDefault(raw string, def int) int {
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return def
	}
	return n
}

// pageRequest reads ?page / ?page_size with sane defaults and the 100-item
// cap from the proto contract.
func pageRequest(r *http.Request) *gencommon.PageRequest {
	q := r.URL.Query()
	page := parseIntDefault(q.Get("page"), 1)
	size := parseIntDefault(q.Get("page_size"), 20)
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 20
	}
	if size > 100 {
		size = 100
	}
	return &gencommon.PageRequest{Page: int32(page), PageSize: int32(size)}
}

// list handles GET /api/users?page=&page_size=&email_filter=&include_deleted=.
func (h *usersHandlers) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	includeDeleted, _ := strconv.ParseBool(q.Get("include_deleted"))

	var resp *genusers.ListUsersResponse
	err := h.users.call(r.Context(), "ListUsers", true, func(ctx context.Context) error {
		var err error
		resp, err = h.client.ListUsers(ctx, &genusers.ListUsersRequest{
			Page:           pageRequest(r),
			EmailFilter:    q.Get("email_filter"),
			IncludeDeleted: includeDeleted,
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}

	users := make([]userJSON, 0, len(resp.GetUsers()))
	for _, u := range resp.GetUsers() {
		users = append(users, userToJSON(u))
	}
	p := resp.GetPage()
	writeJSON(w, http.StatusOK, map[string]any{
		"users": users,
		"page":  pageJSON{Page: p.GetPage(), PageSize: p.GetPageSize(), Total: p.GetTotal()},
	})
}

// get handles GET /api/users/{id}.
func (h *usersHandlers) get(w http.ResponseWriter, r *http.Request) {
	var user *genusers.User
	err := h.users.call(r.Context(), "GetUser", true, func(ctx context.Context) error {
		var err error
		user, err = h.client.GetUser(ctx, &genusers.GetUserRequest{Id: r.PathValue("id")})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, userToJSON(user))
}

type createUserRequest struct {
	Email       string `json:"email"`
	Password    string `json:"password"`
	DisplayName string `json:"display_name"`
}

// create handles POST /api/users.
func (h *usersHandlers) create(w http.ResponseWriter, r *http.Request) {
	var req createUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	var user *genusers.User
	err := h.users.call(r.Context(), "CreateUser", false, func(ctx context.Context) error {
		var err error
		user, err = h.client.CreateUser(ctx, &genusers.CreateUserRequest{
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
	writeJSON(w, http.StatusCreated, userToJSON(user))
}

type updateUserRequest struct {
	DisplayName string `json:"display_name"`
	Bio         string `json:"bio"`
	AvatarURL   string `json:"avatar_url"`
}

// update handles PUT /api/users/{id}.
func (h *usersHandlers) update(w http.ResponseWriter, r *http.Request) {
	var req updateUserRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, r, err)
		return
	}

	var user *genusers.User
	err := h.users.call(r.Context(), "UpdateUser", false, func(ctx context.Context) error {
		var err error
		user, err = h.client.UpdateUser(ctx, &genusers.UpdateUserRequest{
			Id:          r.PathValue("id"),
			DisplayName: req.DisplayName,
			Bio:         req.Bio,
			AvatarUrl:   req.AvatarURL,
		})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, userToJSON(user))
}

// remove handles DELETE /api/users/{id}.
func (h *usersHandlers) remove(w http.ResponseWriter, r *http.Request) {
	var resp *genusers.DeleteUserResponse
	err := h.users.call(r.Context(), "DeleteUser", false, func(ctx context.Context) error {
		var err error
		resp, err = h.client.DeleteUser(ctx, &genusers.DeleteUserRequest{Id: r.PathValue("id")})
		return err
	})
	if err != nil {
		writeError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]bool{"ok": resp.GetOk()})
}
