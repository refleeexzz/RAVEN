package auth

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/raven/platform/pkg/errors"
)

// querier is satisfied by both *pgxpool.Pool and pgx.Tx, so store functions
// work standalone or inside a WithTx transaction.
type querier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
}

// userRecord mirrors a row of the users table (id/email as plain text).
type userRecord struct {
	ID           string
	Email        string
	DisplayName  string
	PasswordHash string
	Deleted      bool
	CreatedAt    time.Time
}

// sessionRecord mirrors the security-relevant columns of sessions.
type sessionRecord struct {
	ID        string
	UserID    string
	ExpiresAt time.Time
	RevokedAt *time.Time
}

// insertUser creates the user row and returns its id. A duplicate email is
// reported as KindConflict/email_taken via the unique constraint.
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

// insertProfile creates the empty profile row owned by the users service.
func insertProfile(ctx context.Context, q querier, userID string) error {
	_, err := q.Exec(ctx, `INSERT INTO user_profiles (user_id) VALUES ($1::uuid)`, userID)
	if err != nil {
		return errors.E(errors.KindUnknown, "profile_insert_failed",
			"could not create the user profile", err)
	}
	return nil
}

// userByEmail loads an active (non-deleted) user including its password
// hash. Only the auth service may read password hashes.
func userByEmail(ctx context.Context, q querier, email string) (userRecord, error) {
	var u userRecord
	err := q.QueryRow(ctx, `
		SELECT id::text, email::text, COALESCE(display_name, ''), password_hash,
		       created_at
		FROM users
		WHERE email = $1 AND deleted_at IS NULL`,
		email,
	).Scan(&u.ID, &u.Email, &u.DisplayName, &u.PasswordHash, &u.CreatedAt)
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return userRecord{}, errors.E(errors.KindNotFound, "user_not_found",
				"user does not exist", err)
		}
		return userRecord{}, errors.E(errors.KindUnknown, "user_lookup_failed",
			"could not look up the user", err)
	}
	return u, nil
}

// rolesAndPerms loads the role names and the distinct permission names a
// user has through its roles.
func rolesAndPerms(ctx context.Context, q querier, userID string) (roles, perms []string, err error) {
	roles, err = loadStrings(ctx, q, `
		SELECT r.name
		FROM roles r
		JOIN user_roles ur ON ur.role_id = r.id
		WHERE ur.user_id = $1::uuid
		ORDER BY r.name`, userID)
	if err != nil {
		return nil, nil, err
	}
	perms, err = loadStrings(ctx, q, `
		SELECT DISTINCT p.name
		FROM permissions p
		JOIN role_permissions rp ON rp.permission_id = p.id
		JOIN user_roles ur ON ur.role_id = rp.role_id
		WHERE ur.user_id = $1::uuid
		ORDER BY p.name`, userID)
	if err != nil {
		return nil, nil, err
	}
	return roles, perms, nil
}

func loadStrings(ctx context.Context, q querier, sql string, args ...any) ([]string, error) {
	rows, err := q.Query(ctx, sql, args...)
	if err != nil {
		return nil, errors.E(errors.KindUnknown, "query_failed",
			"could not load user access data", err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, errors.E(errors.KindUnknown, "scan_failed",
				"could not read user access data", err)
		}
		out = append(out, s)
	}
	if err := rows.Err(); err != nil {
		return nil, errors.E(errors.KindUnknown, "query_failed",
			"could not load user access data", err)
	}
	return out, nil
}

// insertSession persists a new refresh-token session and returns its id.
func insertSession(ctx context.Context, q querier, userID, tokenHash string, expiresAt time.Time, userAgent, ip string) (string, error) {
	var id string
	err := q.QueryRow(ctx, `
		INSERT INTO sessions (user_id, refresh_token_hash, expires_at, user_agent, ip)
		VALUES ($1::uuid, $2, $3, NULLIF($4, ''), NULLIF($5, ''))
		RETURNING id::text`,
		userID, tokenHash, expiresAt, userAgent, ip,
	).Scan(&id)
	if err != nil {
		return "", errors.E(errors.KindUnknown, "session_insert_failed",
			"could not persist the session", err)
	}
	return id, nil
}

// sessionByHashForUpdate loads a session row, locking it for the enclosing
// transaction. Refresh rotation and reuse detection depend on this lock.
func sessionByHashForUpdate(ctx context.Context, q querier, tokenHash string) (sessionRecord, error) {
	var s sessionRecord
	err := q.QueryRow(ctx, `
		SELECT id::text, user_id::text, expires_at, revoked_at
		FROM sessions
		WHERE refresh_token_hash = $1
		FOR UPDATE`,
		tokenHash,
	).Scan(&s.ID, &s.UserID, &s.ExpiresAt, &s.RevokedAt)
	if err != nil {
		if stderrors.Is(err, pgx.ErrNoRows) {
			return sessionRecord{}, errors.E(errors.KindNotFound, "session_not_found",
				"refresh token is not recognised", err)
		}
		return sessionRecord{}, errors.E(errors.KindUnknown, "session_lookup_failed",
			"could not look up the session", err)
	}
	return s, nil
}

// revokeSession marks one session revoked. It is idempotent.
func revokeSession(ctx context.Context, q querier, sessionID string) error {
	_, err := q.Exec(ctx, `
		UPDATE sessions SET revoked_at = now()
		WHERE id = $1::uuid AND revoked_at IS NULL`,
		sessionID,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "session_revoke_failed",
			"could not revoke the session", err)
	}
	return nil
}

// revokeAllSessions marks every live session of a user revoked. It runs when
// a refresh-token reuse is detected (the token family may be compromised).
func revokeAllSessions(ctx context.Context, q querier, userID string) (int64, error) {
	tag, err := q.Exec(ctx, `
		UPDATE sessions SET revoked_at = now()
		WHERE user_id = $1::uuid AND revoked_at IS NULL`,
		userID,
	)
	if err != nil {
		return 0, errors.E(errors.KindUnknown, "sessions_revoke_failed",
			"could not revoke the user sessions", err)
	}
	return tag.RowsAffected(), nil
}

// audit actions written to audit_logs.
const (
	auditRegister     = "register"
	auditLogin        = "login"
	auditLoginFailed  = "login_failed"
	auditLogout       = "logout"
	auditRefresh      = "refresh"
	auditRevokeToken  = "revoke_token"
	auditRefreshReuse = "security_refresh_reuse"
)

// insertAudit appends a security event. userID may be empty (unknown actor,
// e.g. a failed login for a non-existent email) and is then stored as NULL.
func insertAudit(ctx context.Context, q querier, userID, action, ip, userAgent string, metadata map[string]any) error {
	var uid any
	if userID != "" {
		uid = userID
	}
	var meta any
	if metadata != nil {
		raw, err := json.Marshal(metadata)
		if err != nil {
			return errors.E(errors.KindUnknown, "audit_marshal_failed",
				"could not encode audit metadata", err)
		}
		meta = string(raw)
	}
	_, err := q.Exec(ctx, `
		INSERT INTO audit_logs (user_id, action, ip, user_agent, metadata)
		VALUES ($1::uuid, $2, NULLIF($3, ''), NULLIF($4, ''), $5::jsonb)`,
		uid, action, ip, userAgent, meta,
	)
	if err != nil {
		return errors.E(errors.KindUnknown, "audit_insert_failed",
			"could not write the audit log", err)
	}
	return nil
}
