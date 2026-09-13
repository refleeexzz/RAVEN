// Package users implements the RAVEN users service: profile CRUD over the
// shared users table plus the user_profiles table it owns. Transport is
// gRPC (:9082); ops HTTP (:8082) serves /health, /ready and /metrics.
// Credentials stay with the auth service — this service never reads
// password hashes beyond writing them at creation time.
package users

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/internal/database"
	gencommon "github.com/refleeexzz/RAVEN/internal/gen/common"
	genusers "github.com/refleeexzz/RAVEN/internal/gen/users"
	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// Server implements genusers.UserServiceServer.
type Server struct {
	genusers.UnimplementedUserServiceServer

	pool       *pgxpool.Pool
	log        *slog.Logger
	bcryptCost int
	metrics    *ServiceMetrics
}

// NewServer wires the gRPC service implementation.
func NewServer(pool *pgxpool.Pool, log *slog.Logger, bcryptCost int, m *ServiceMetrics) *Server {
	return &Server{pool: pool, log: log, bcryptCost: bcryptCost, metrics: m}
}

// GetUser returns one user. Soft-deleted users look identical to missing
// ones (both NotFound) unless internal tooling opts into includeDeleted.
func (s *Server) GetUser(ctx context.Context, req *genusers.GetUserRequest) (resp *genusers.User, err error) {
	defer func() { s.metrics.observe("get_user", err) }()

	if _, perr := uuid.Parse(req.GetId()); perr != nil {
		err = errors.E(errors.KindInvalid, "user_id_invalid", "user id must be a uuid", perr)
		return nil, toStatus(err)
	}

	u, err := getUserByID(ctx, s.pool, req.GetId(), false)
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(u), nil
}

// ListUsers pages over users with an optional case-insensitive email
// substring filter. page_size is capped at 100 (platform contract).
func (s *Server) ListUsers(ctx context.Context, req *genusers.ListUsersRequest) (resp *genusers.ListUsersResponse, err error) {
	defer func() { s.metrics.observe("list_users", err) }()

	page, pageSize := normalizePage(req.GetPage())
	records, total, err := listUsers(ctx, s.pool,
		emailPattern(req.GetEmailFilter()), req.GetIncludeDeleted(),
		pageSize, (page-1)*pageSize)
	if err != nil {
		return nil, toStatus(err)
	}

	out := &genusers.ListUsersResponse{
		Users: make([]*genusers.User, 0, len(records)),
		Page: &gencommon.PageResponse{
			Page:     int32(page),
			PageSize: int32(pageSize),
			Total:    total,
		},
	}
	for _, u := range records {
		out.Users = append(out.Users, toProto(u))
	}
	return out, nil
}

// CreateUser applies the same validation and hashing rules as auth
// registration, grants the USER role and creates the empty profile — all in
// one transaction.
func (s *Server) CreateUser(ctx context.Context, req *genusers.CreateUserRequest) (resp *genusers.User, err error) {
	defer func() { s.metrics.observe("create_user", err) }()

	if err := ravenauth.ValidateEmail(req.GetEmail()); err != nil {
		return nil, toStatus(err)
	}
	if err := ravenauth.ValidatePassword(req.GetPassword()); err != nil {
		return nil, toStatus(err)
	}

	hash, err := ravenauth.HashPassword(req.GetPassword(), s.bcryptCost)
	if err != nil {
		return nil, toStatus(err)
	}

	var created userRecord
	err = database.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		id, err := insertUser(ctx, tx, req.GetEmail(), req.GetDisplayName(), hash)
		if err != nil {
			return err
		}
		if err := assignRole(ctx, tx, id, ravenauth.RoleUser); err != nil {
			return err
		}
		if err := updateProfile(ctx, tx, id, "", ""); err != nil {
			return err
		}
		if err := insertAudit(ctx, tx, id, "user_created"); err != nil {
			return err
		}
		u, err := getUserByID(ctx, tx, id, false)
		if err != nil {
			return err
		}
		created = u
		return nil
	})
	if err != nil {
		return nil, toStatus(err)
	}

	s.log.InfoContext(ctx, "user created", slog.String("user_id", created.ID))
	return toProto(created), nil
}

// UpdateUser touches only profile-level fields: display_name (users table),
// bio and avatar_url (user_profiles table). Email, password and roles are
// owned by the auth service and are not updatable here.
func (s *Server) UpdateUser(ctx context.Context, req *genusers.UpdateUserRequest) (resp *genusers.User, err error) {
	defer func() { s.metrics.observe("update_user", err) }()

	if _, perr := uuid.Parse(req.GetId()); perr != nil {
		err = errors.E(errors.KindInvalid, "user_id_invalid", "user id must be a uuid", perr)
		return nil, toStatus(err)
	}

	var updated userRecord
	err = database.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if err := updateDisplayName(ctx, tx, req.GetId(), req.GetDisplayName()); err != nil {
			return err
		}
		if err := updateProfile(ctx, tx, req.GetId(), req.GetBio(), req.GetAvatarUrl()); err != nil {
			return err
		}
		u, err := getUserByID(ctx, tx, req.GetId(), false)
		if err != nil {
			return err
		}
		updated = u
		return nil
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return toProto(updated), nil
}

// DeleteUser soft-deletes via deleted_at. Hard delete is out of scope.
func (s *Server) DeleteUser(ctx context.Context, req *genusers.DeleteUserRequest) (resp *genusers.DeleteUserResponse, err error) {
	defer func() { s.metrics.observe("delete_user", err) }()

	if _, perr := uuid.Parse(req.GetId()); perr != nil {
		err = errors.E(errors.KindInvalid, "user_id_invalid", "user id must be a uuid", perr)
		return nil, toStatus(err)
	}

	if err := softDeleteUser(ctx, s.pool, req.GetId()); err != nil {
		return nil, toStatus(err)
	}
	s.log.InfoContext(ctx, "user soft-deleted", slog.String("user_id", req.GetId()))
	return &genusers.DeleteUserResponse{Ok: true}, nil
}

// toProto converts a store record to the wire type.
func toProto(u userRecord) *genusers.User {
	return &genusers.User{
		Id:          u.ID,
		Email:       u.Email,
		DisplayName: u.DisplayName,
		Bio:         u.Bio,
		AvatarUrl:   u.AvatarURL,
		Deleted:     u.Deleted,
		CreatedAt:   u.CreatedAt.Unix(),
		UpdatedAt:   u.UpdatedAt.Unix(),
	}
}
