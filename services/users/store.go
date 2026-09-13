package users

import (
	"context"
	stderrors "errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so store functions
// work standalone or inside a WithTx transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// userRecord joins the users row with its profile. The users service owns
// profiles and reads users; it never touches password_hash.
type userRecord struct {
	ID          string
	Email       string
	DisplayName string
	Bio         string
	AvatarURL   string
	Deleted     bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// userColumns is the shared projection for single- and multi-row reads.
const userColumns = `
	u.id::text, u.email::text, COALESCE(u.display_name, ''),
	COALESCE(p.bio, ''), COALESCE(p.avatar_url, ''),
	(u.deleted_at IS NOT NULL), u.created_at, u.updated_at
FROM users u
LEFT JOIN user_profiles p ON p.user_id = u.id`

func scanUser(row pgx.Row) (userRecord, error) {
	var u userRecord
	err := row.Scan(&u.ID, &u.Email, &u.DisplayName, &u.Bio, &u.AvatarURL,
		&u.Deleted, &u.CreatedAt, &u.UpdatedAt)
	return u, err
}

// getUserByID loads one user. Soft-deleted users are hidden unless
// includeDeleted is set (used by tests and future admin tooling).
func getUserByID(ctx context.Context, q querier, id string, includeDeleted bool) (userRecord, error) {
	u, err := scanUser(q.QueryRow(ctx, `
		SELECT `+userColumns+`
		WHERE u.id = $1::uuid AND ($2::bool OR u.deleted_at IS NULL)`,
		id, includeDeleted,
	))
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return userRecord{}, errors.E(errors.KindNotFound, "user_not_found",
				"user does not exist", err)
		}
		return userRecord{}, errors.E(errors.KindUnknown, "user_lookup_failed",
			"could not load the user", err)
	}
	return u, nil
}

// listUsers returns one page plus the total matching row count. The filter
// is a pre-escaped ILIKE pattern (see emailPattern); "%%" matches all.
func listUsers(ctx context.Context, q querier, pattern string, includeDeleted bool, limit, offset int) ([]userRecord, int64, error) {
	var total int64
	err := q.QueryRow(ctx, `
		SELECT count(*) FROM users u
		WHERE u.email ILIKE $1 ESCAPE '\' AND ($2::bool OR u.deleted_at IS NULL)`,
		pattern, includeDeleted,
	).Scan(&total)
	if err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "user_count_failed",
			"could not count users", err)
	}

	rows, err := q.Query(ctx, `
		SELECT `+userColumns+`
		WHERE u.email ILIKE $1 ESCAPE '\' AND ($2::bool OR u.deleted_at IS NULL)
		ORDER BY u.created_at, u.id
		LIMIT $3 OFFSET $4`,
		pattern, includeDeleted, limit, offset,
	)
	if err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "user_list_failed",
			"could not list users", err)
	}
	defer rows.Close()

	var out []userRecord
	for rows.Next() {
		u, err := scanUser(rows)
		if err != nil {
			return nil, 0, errors.E(errors.KindUnknown, "user_list_failed",
				"could not read a user row", err)
		}
		out = append(out, u)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, errors.E(errors.KindUnknown, "user_list_failed",
			"could not list users", err)
	}
	return out, total, nil
}

// insertUser creates the identity row. Same rules as auth registration;
// duplicate emails surface as KindConflict/email_taken.
func insertUser(ctx context.Context, q querier, email, displayName, passwordHash string) (string, error) {
	var id string
	err := q.QueryRow(ctx, `
		INSERT INTO users (email, display_name, password_hash)
		VALUES ($1, NULLIF($2, ''), $3)
		RETURNING id::text`,
		email, displayName, passwordHash,
	).Scan(&id)
	if err != nil {
		var pgErr *pgconn.PgError
		if stderrors.As(err, &pgErr) && pgErr.Code == "23505" {
			return "", errors.E(errors.KindConflict, "email_taken",
				"a user with this email already exists", err)
		}
		return "", errors.E(errors.KindUnknown, "user_insert_failed",
			"could not create the user", err)
	}
	return id, nil
}

// assignRole grants a named role to a user.
func assignRole(ctx context.Context, q querier, userID, role string) error {
	_, err := q.Exec(ctx, `
		INSERT INTO user_roles (user_id, role_id)
		SELECT $1::uuid, id FROM roles WHERE name = $2`,
		userID, role,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "role_assign_failed",
			"could not assign the default role", err)
	}
	return nil
}

// updateProfile upserts the profile fields owned by this service.
func updateProfile(ctx context.Context, q querier, userID, bio, avatarURL string) error {
	_, err := q.Exec(ctx, `
		INSERT INTO user_profiles (user_id, bio, avatar_url, updated_at)
		VALUES ($1::uuid, NULLIF($2, ''), NULLIF($3, ''), now())
		ON CONFLICT (user_id) DO UPDATE
		SET bio = EXCLUDED.bio, avatar_url = EXCLUDED.avatar_url, updated_at = now()`,
		userID, bio, avatarURL,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "profile_update_failed",
			"could not update the profile", err)
	}
	return nil
}

// updateDisplayName updates the one users-table column this service owns.
func updateDisplayName(ctx context.Context, q querier, userID, displayName string) error {
	tag, err := q.Exec(ctx, `
		UPDATE users SET display_name = NULLIF($2, ''), updated_at = now()
		WHERE id = $1::uuid AND deleted_at IS NULL`,
		userID, displayName,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "user_update_failed",
			"could not update the user", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.E(errors.KindNotFound, "user_not_found",
			"user does not exist", nil)
	}
	return nil
}

// softDeleteUser marks a user deleted. Hard delete is out of scope: rows
// stay so audit trails and job history keep their references.
func softDeleteUser(ctx context.Context, q querier, userID string) error {
	tag, err := q.Exec(ctx, `
		UPDATE users SET deleted_at = now(), updated_at = now()
		WHERE id = $1::uuid AND deleted_at IS NULL`,
		userID,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "user_delete_failed",
			"could not delete the user", err)
	}
	if tag.RowsAffected() == 0 {
		return errors.E(errors.KindNotFound, "user_not_found",
			"user does not exist", nil)
	}
	return nil
}

// insertAudit records administrative user creation in the shared audit log.
func insertAudit(ctx context.Context, q querier, userID, action string) error {
	_, err := q.Exec(ctx, `
		INSERT INTO audit_logs (user_id, action) VALUES ($1::uuid, $2)`,
		userID, action,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "audit_insert_failed",
			"could not write the audit log", err)
	}
	return nil
}
