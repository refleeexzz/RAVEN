// authz.go enforces per-route permissions after AuthN. The required
// permission comes from the route table; the identity's permission list
// comes from the auth service (it is embedded in the validated token data).
package gateway

import (
	"net/http"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/internal/middleware"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// permAuthenticated marks routes that need a valid token but no specific
// permission (today: POST /api/auth/logout).
const permAuthenticated = "<authenticated>"

// authorize builds the AuthZ middleware for one route. perm is the required
// permission ("users:read"), or permAuthenticated for "any logged-in user".
func (s *server) authorize(perm string) middleware.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id, ok := IdentityFrom(r.Context())
			if !ok {
				// AuthN should have rejected the request already; this is a
				// safety net for a miswired chain.
				writeError(w, r, errors.E(errors.KindUnauthorized, "unauthenticated",
					"authentication required", nil))
				return
			}
			if perm != "" && perm != permAuthenticated && !ravenauth.HasPermission(id.Perms, perm) {
				writeError(w, r, errors.E(errors.KindForbidden, "forbidden",
					"missing permission "+perm, nil))
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
