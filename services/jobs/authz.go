package jobs

import (
	"context"
	"strings"

	"github.com/google/uuid"
	"google.golang.org/grpc/metadata"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// Caller is the authenticated identity behind a jobs RPC (JOBS-02).
//
// The gateway terminates user auth and forwards the identity as gRPC
// metadata: x-user-id (UUID) and x-user-perms (comma-separated). The jobs
// gRPC port is internal, so a caller that presents NO identity at all is
// service-to-service traffic and gets full access (System). A caller that
// presents an identity gets owner-scoped access: users see and manage only
// their own jobs; admin:* bypasses (CheckPermission-style).
//
// Security rule that matters: an identity header that is present but
// malformed, or permissions presented without a user id, is an error
// (Unauthenticated) — never a silent downgrade to System, and never a free
// privilege upgrade.
type Caller struct {
	UserID string // authenticated end user ("" only when System)
	Admin  bool   // x-user-perms contains admin:*
	System bool   // no identity presented: internal traffic on the gRPC port
}

// CallerFromContext resolves the caller from incoming gRPC metadata.
func CallerFromContext(ctx context.Context) (Caller, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Caller{System: true}, nil
	}
	return CallerFromMetadata(md)
}

// CallerFromMetadata resolves the caller from raw metadata (test-friendly).
func CallerFromMetadata(md metadata.MD) (Caller, error) {
	ids := md.Get("x-user-id")
	perms := md.Get("x-user-perms")

	if len(ids) == 0 {
		if len(perms) > 0 {
			// Permissions with no identity: somebody is trying to mint
			// authority out of thin air. Refuse loudly.
			return Caller{}, errors.E(errors.KindUnauthorized, "identity_invalid",
				"permissions were presented without a user identity", nil)
		}
		return Caller{System: true}, nil
	}

	id, err := uuid.Parse(strings.TrimSpace(ids[0]))
	if err != nil {
		return Caller{}, errors.E(errors.KindUnauthorized, "identity_invalid",
			"x-user-id must be a valid user UUID", nil)
	}

	c := Caller{UserID: id.String()}
	if len(perms) > 0 {
		list := strings.Split(perms[0], ",")
		for i := range list {
			list[i] = strings.TrimSpace(list[i])
		}
		c.Admin = ravenauth.HasPermission(list, ravenauth.PermAdminAll)
	}
	return c, nil
}

// CanAccessJob reports whether the caller may see or manage a job with this
// owner. Ownerless jobs (created by system callers) are system property:
// only admins and system callers can reach them.
func (c Caller) CanAccessJob(ownerID *string) bool {
	if c.System || c.Admin {
		return true
	}
	if c.UserID == "" {
		return false
	}
	return ownerID != nil && *ownerID == c.UserID
}

// AuthorizeJob enforces CanAccessJob. Denials are KindNotFound — a foreign
// job must be indistinguishable from a missing one, otherwise the API is an
// existence oracle for other users' work.
func (c Caller) AuthorizeJob(j *Job) error {
	if c.CanAccessJob(j.OwnerID) {
		return nil
	}
	return errors.E(errors.KindNotFound, "job_not_found", "job does not exist", nil)
}

// OwnerScope returns the ListJobs owner filter: "" means "all owners"
// (system/admin); anything else narrows the listing to that owner.
func (c Caller) OwnerScope() string {
	if c.System || c.Admin {
		return ""
	}
	return c.UserID
}
