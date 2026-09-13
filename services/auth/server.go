// Package auth implements the RAVEN auth service: registration, login,
// refresh-token rotation with reuse detection, token validation/revocation
// and RBAC permission checks. Transport is gRPC (:9081); ops HTTP (:8081)
// serves /health, /ready and /metrics.
package auth

import (
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
	"github.com/refleeexzz/RAVEN/internal/database"
	genauth "github.com/refleeexzz/RAVEN/internal/gen/auth"
	"github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/logger"
)

// Redis keys (documented in the service README section of the code):
const (
	// revokedJTIKey prefixes the access-token denylist entries. A present
	// key means the jti must be treated as invalid even if the JWT verifies.
	revokedJTIKey = "revoked:jti:"
	// permsCacheKey prefixes the 30-second permission cache used by
	// CheckPermission.
	permsCacheKey = "perms:"
	// permsCacheTTL bounds how long a permission change can take to
	// propagate through the cache.
	permsCacheTTL = 30 * time.Second
)

// Server implements genauth.AuthServiceServer.
type Server struct {
	genauth.UnimplementedAuthServiceServer

	pool       *pgxpool.Pool
	rdb        redis.UniversalClient // may be nil in unit tests
	log        *slog.Logger
	secret     string
	bcryptCost int
	metrics    *ServiceMetrics
}

// NewServer wires the gRPC service implementation. rdb may be nil; every
// Redis use degrades gracefully (see ValidateToken for the trade-off).
func NewServer(pool *pgxpool.Pool, rdb redis.UniversalClient, log *slog.Logger, jwtSecret string, bcryptCost int, m *ServiceMetrics) *Server {
	return &Server{
		pool:       pool,
		rdb:        rdb,
		log:        log,
		secret:     jwtSecret,
		bcryptCost: bcryptCost,
		metrics:    m,
	}
}

// ---------------------------------------------------------------------------
// Register
// ---------------------------------------------------------------------------

func (s *Server) Register(ctx context.Context, req *genauth.RegisterRequest) (*genauth.RegisterResponse, error) {
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

	ua, ip := clientInfo(ctx)
	var userID string
	err = database.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		id, err := insertUser(ctx, tx, req.GetEmail(), req.GetDisplayName(), hash)
		if err != nil {
			return err
		}
		userID = id
		// New users get the USER role by default (platform rule).
		if err := assignRole(ctx, tx, userID, ravenauth.RoleUser); err != nil {
			return err
		}
		if err := insertProfile(ctx, tx, userID); err != nil {
			return err
		}
		return insertAudit(ctx, tx, userID, auditRegister, ip, ua, nil)
	})
	if err != nil {
		return nil, toStatus(err)
	}

	s.log.InfoContext(ctx, "user registered", slog.String("user_id", userID))
	return &genauth.RegisterResponse{UserId: userID, Email: req.GetEmail()}, nil
}

// ---------------------------------------------------------------------------
// Login
// ---------------------------------------------------------------------------

func (s *Server) Login(ctx context.Context, req *genauth.LoginRequest) (*genauth.TokenPair, error) {
	if err := ravenauth.ValidateEmail(req.GetEmail()); err != nil {
		return nil, toStatus(err)
	}
	if req.GetPassword() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "password_required",
			"password is required", nil))
	}

	ua, ip := clientInfo(ctx)

	// Deliberately identical errors for "unknown email" and "wrong password":
	// distinguishing them leaks which emails are registered.
	invalidCreds := errors.E(errors.KindUnauthorized, "invalid_credentials",
		"invalid email or password", nil)

	user, err := userByEmail(ctx, s.pool, req.GetEmail())
	if err != nil {
		if errors.KindOf(err) == errors.KindNotFound {
			s.metrics.logins.WithLabelValues("failure").Inc()
			_ = insertAudit(ctx, s.pool, "", auditLoginFailed, ip, ua,
				map[string]any{"email": req.GetEmail(), "reason": "unknown_email"})
			return nil, toStatus(invalidCreds)
		}
		return nil, toStatus(err)
	}
	if err := ravenauth.CheckPassword(user.PasswordHash, req.GetPassword()); err != nil {
		s.metrics.logins.WithLabelValues("failure").Inc()
		_ = insertAudit(ctx, s.pool, user.ID, auditLoginFailed, ip, ua,
			map[string]any{"reason": "bad_password"})
		return nil, toStatus(invalidCreds)
	}

	pair, err := s.mintTokenPair(ctx, s.pool, user, ua, ip)
	if err != nil {
		return nil, toStatus(err)
	}
	if err := insertAudit(ctx, s.pool, user.ID, auditLogin, ip, ua, nil); err != nil {
		// The login itself succeeded; a lost audit row must not fail it.
		s.log.WarnContext(ctx, "login audit write failed", slog.Any("error", err))
	}

	s.metrics.logins.WithLabelValues("success").Inc()
	return pair, nil
}

// mintTokenPair issues a fresh access token + refresh session for user.
func (s *Server) mintTokenPair(ctx context.Context, q querier, user userRecord, ua, ip string) (*genauth.TokenPair, error) {
	roles, perms, err := rolesAndPerms(ctx, q, user.ID)
	if err != nil {
		return nil, err
	}

	accessExp := time.Now().Add(ravenauth.AccessTokenTTL)
	access, err := ravenauth.MintAccessToken(s.secret, ravenauth.Claims{
		Email:            user.Email,
		Roles:            roles,
		Perms:            perms,
		RegisteredClaims: registeredClaims(user.ID, accessExp),
	})
	if err != nil {
		return nil, err
	}

	refresh, refreshHash, err := newRefreshToken()
	if err != nil {
		return nil, err
	}
	refreshExp := time.Now().Add(RefreshTokenTTL)
	if _, err := insertSession(ctx, q, user.ID, refreshHash, refreshExp, ua, ip); err != nil {
		return nil, err
	}

	return &genauth.TokenPair{
		AccessToken:      access,
		RefreshToken:     refresh,
		AccessExpiresAt:  accessExp.Unix(),
		RefreshExpiresAt: refreshExp.Unix(),
	}, nil
}

// ---------------------------------------------------------------------------
// Refresh (rotation + reuse detection)
// ---------------------------------------------------------------------------

func (s *Server) Refresh(ctx context.Context, req *genauth.RefreshRequest) (*genauth.TokenPair, error) {
	if req.GetRefreshToken() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "refresh_token_required",
			"refresh token is required", nil))
	}

	ua, ip := clientInfo(ctx)
	hash := HashRefreshToken(req.GetRefreshToken())

	var pair *genauth.TokenPair
	var reuseErr error
	err := database.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		sess, err := sessionByHashForUpdate(ctx, tx, hash)
		if err != nil {
			if errors.KindOf(err) == errors.KindNotFound {
				return errors.E(errors.KindUnauthorized, "refresh_token_invalid",
					"refresh token is not recognised", err)
			}
			return err
		}

		// Reuse detection: a presented token whose session was already
		// revoked means the token family may be stolen. Revoke ALL sessions
		// of that user and log a security audit entry. Crucially these
		// writes must COMMIT even though the RPC fails — so the closure
		// returns nil here and the client error travels via reuseErr,
		// outside the transaction.
		if sess.RevokedAt != nil {
			revoked, revokeErr := revokeAllSessions(ctx, tx, sess.UserID)
			if revokeErr != nil {
				return revokeErr
			}
			if err := insertAudit(ctx, tx, sess.UserID, auditRefreshReuse, ip, ua,
				map[string]any{"sessions_revoked": revoked}); err != nil {
				return err
			}
			s.log.WarnContext(ctx, "refresh token reuse detected, all sessions revoked",
				slog.String("user_id", sess.UserID),
				slog.Int64("sessions_revoked", revoked))
			reuseErr = errors.E(errors.KindUnauthorized, "refresh_token_reused",
				"refresh token was already used; all sessions were revoked", nil)
			return nil
		}

		if time.Now().After(sess.ExpiresAt) {
			return errors.E(errors.KindUnauthorized, "refresh_token_expired",
				"refresh token has expired", nil)
		}

		var user userRecord
		err = tx.QueryRow(ctx, `
			SELECT id::text, email::text, COALESCE(display_name, ''), created_at
			FROM users WHERE id = $1::uuid AND deleted_at IS NULL`,
			sess.UserID,
		).Scan(&user.ID, &user.Email, &user.DisplayName, &user.CreatedAt)
		if err != nil {
			return errors.E(errors.KindUnauthorized, "refresh_token_invalid",
				"session owner is not available", err)
		}

		// Rotate: revoke the presented session, issue a fresh pair.
		if err := revokeSession(ctx, tx, sess.ID); err != nil {
			return err
		}
		newPair, err := s.mintTokenPair(ctx, tx, user, ua, ip)
		if err != nil {
			return err
		}
		pair = newPair
		return insertAudit(ctx, tx, user.ID, auditRefresh, ip, ua, nil)
	})
	if err != nil {
		return nil, toStatus(err)
	}
	if reuseErr != nil {
		return nil, toStatus(reuseErr)
	}

	s.metrics.refreshRotated.Inc()
	return pair, nil
}

// ---------------------------------------------------------------------------
// Logout
// ---------------------------------------------------------------------------

func (s *Server) Logout(ctx context.Context, req *genauth.LogoutRequest) (*genauth.LogoutResponse, error) {
	if req.GetRefreshToken() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "refresh_token_required",
			"refresh token is required", nil))
	}

	ua, ip := clientInfo(ctx)
	hash := HashRefreshToken(req.GetRefreshToken())

	// Logout is idempotent: revoking an unknown or already-revoked session
	// still reports success so clients can retry safely.
	var sess sessionRecord
	err := s.pool.QueryRow(ctx, `
		SELECT id::text, user_id::text FROM sessions WHERE refresh_token_hash = $1`,
		hash,
	).Scan(&sess.ID, &sess.UserID)
	if err == nil {
		if err := revokeSession(ctx, s.pool, sess.ID); err != nil {
			return nil, toStatus(err)
		}
		if err := insertAudit(ctx, s.pool, sess.UserID, auditLogout, ip, ua, nil); err != nil {
			s.log.WarnContext(ctx, "logout audit write failed", slog.Any("error", err))
		}
	}
	return &genauth.LogoutResponse{Ok: true}, nil
}

// ---------------------------------------------------------------------------
// ValidateToken / RevokeToken
// ---------------------------------------------------------------------------

func (s *Server) ValidateToken(ctx context.Context, req *genauth.ValidateTokenRequest) (*genauth.ValidateTokenResponse, error) {
	claims, err := ravenauth.ParseAccessToken(s.secret, req.GetAccessToken())
	if err != nil {
		s.metrics.tokensValidated.WithLabelValues("invalid").Inc()
		return &genauth.ValidateTokenResponse{Valid: false}, nil
	}

	revoked, err := s.isRevoked(ctx, claims.ID)
	if err != nil {
		// Trade-off, documented: the denylist check FAILS OPEN. If Redis is
		// down we accept cryptographically valid tokens rather than taking
		// the whole platform down with the cache. Revoked tokens live at
		// most AccessTokenTTL (15 min), which bounds the exposure window.
		s.log.WarnContext(ctx, "revocation denylist unavailable, failing open",
			slog.Any("error", err))
	}
	if revoked {
		s.metrics.tokensValidated.WithLabelValues("invalid").Inc()
		return &genauth.ValidateTokenResponse{Valid: false}, nil
	}

	s.metrics.tokensValidated.WithLabelValues("valid").Inc()
	return &genauth.ValidateTokenResponse{
		Valid:       true,
		UserId:      claims.Subject,
		Email:       claims.Email,
		Roles:       claims.Roles,
		Permissions: claims.Perms,
	}, nil
}

func (s *Server) RevokeToken(ctx context.Context, req *genauth.RevokeTokenRequest) (*genauth.RevokeTokenResponse, error) {
	claims, err := ravenauth.ParseAccessToken(s.secret, req.GetAccessToken())
	if err != nil {
		// An unparseable or already-expired token needs no revocation.
		return &genauth.RevokeTokenResponse{Ok: true}, nil
	}
	if s.rdb == nil {
		s.log.WarnContext(ctx, "redis not configured, token revocation skipped")
		return &genauth.RevokeTokenResponse{Ok: true}, nil
	}

	ttl := time.Until(claims.ExpiresAt.Time)
	if ttl > 0 {
		if err := s.rdb.Set(ctx, revokedJTIKey+claims.ID, "1", ttl).Err(); err != nil {
			return nil, toStatus(errors.E(errors.KindUnavailable, "revoke_failed",
				"could not revoke the token", err))
		}
	}

	ua, ip := clientInfo(ctx)
	if err := insertAudit(ctx, s.pool, claims.Subject, auditRevokeToken, ip, ua,
		map[string]any{"jti": claims.ID}); err != nil {
		s.log.WarnContext(ctx, "revoke audit write failed", slog.Any("error", err))
	}
	return &genauth.RevokeTokenResponse{Ok: true}, nil
}

func (s *Server) isRevoked(ctx context.Context, jti string) (bool, error) {
	if s.rdb == nil || jti == "" {
		return false, nil
	}
	n, err := s.rdb.Exists(ctx, revokedJTIKey+jti).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// ---------------------------------------------------------------------------
// CheckPermission
// ---------------------------------------------------------------------------

func (s *Server) CheckPermission(ctx context.Context, req *genauth.CheckPermissionRequest) (*genauth.CheckPermissionResponse, error) {
	if _, err := uuid.Parse(req.GetUserId()); err != nil {
		return nil, toStatus(errors.E(errors.KindInvalid, "user_id_invalid",
			"user id must be a uuid", err))
	}
	if req.GetPermission() == "" {
		return nil, toStatus(errors.E(errors.KindInvalid, "permission_required",
			"permission is required", nil))
	}

	perms, err := s.loadPermsCached(ctx, req.GetUserId())
	if err != nil {
		return nil, toStatus(err)
	}
	return &genauth.CheckPermissionResponse{
		Allowed: ravenauth.HasPermission(perms, req.GetPermission()),
	}, nil
}

// loadPermsCached serves permissions from Redis when possible and falls
// back to Postgres. Cache errors are logged, never fatal: correctness lives
// in the database, Redis is only a latency optimisation.
func (s *Server) loadPermsCached(ctx context.Context, userID string) ([]string, error) {
	key := permsCacheKey + userID
	if s.rdb != nil {
		if raw, err := s.rdb.Get(ctx, key).Result(); err == nil {
			var perms []string
			if err := json.Unmarshal([]byte(raw), &perms); err == nil {
				return perms, nil
			}
		} else if err != redis.Nil {
			s.log.WarnContext(ctx, "permission cache read failed, using database",
				slog.Any("error", err))
		}
	}

	_, perms, err := rolesAndPerms(ctx, s.pool, userID)
	if err != nil {
		return nil, err
	}

	if s.rdb != nil {
		if raw, err := json.Marshal(perms); err == nil {
			if err := s.rdb.Set(ctx, key, raw, permsCacheTTL).Err(); err != nil {
				s.log.WarnContext(ctx, "permission cache write failed",
					slog.Any("error", err))
			}
		}
	}
	return perms, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// registeredClaims builds the jwt.RegisteredClaims for an access token.
// The jti is left empty: MintAccessToken generates one.
func registeredClaims(userID string, exp time.Time) jwt.RegisteredClaims {
	return jwt.RegisteredClaims{
		Subject:   userID,
		IssuedAt:  jwt.NewNumericDate(time.Now()),
		ExpiresAt: jwt.NewNumericDate(exp),
	}
}

// clientInfo extracts a best-effort user agent and client IP from the gRPC
// call context for audit logging.
func clientInfo(ctx context.Context) (userAgent, ip string) {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if v := md.Get("user-agent"); len(v) > 0 {
			userAgent = v[0]
		}
		if v := md.Get("x-forwarded-for"); len(v) > 0 {
			ip = v[0]
		}
	}
	if ip == "" {
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			if host, _, err := net.SplitHostPort(p.Addr.String()); err == nil {
				ip = host
			} else {
				ip = p.Addr.String()
			}
		}
	}
	return userAgent, ip
}

// redisChecker pings Redis for the readiness probe.
func redisChecker(rdb redis.UniversalClient) func(ctx context.Context) error {
	return func(ctx context.Context) error {
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		return rdb.Ping(pingCtx).Err()
	}
}

// unaryLoggingInterceptor logs one line per finished RPC. Kept tiny on
// purpose; heavy observability belongs to metrics + tracing.
func unaryLoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		logger.WithContext(ctx, log).Debug("grpc call",
			slog.String("method", info.FullMethod),
			slog.Duration("duration", time.Since(start)),
			slog.Any("error", err),
		)
		return resp, err
	}
}

// unaryRecoveryInterceptor converts panics into Internal errors.
func unaryRecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error("grpc panic recovered",
					slog.String("method", info.FullMethod),
					slog.Any("panic", rec),
				)
				err = toStatus(errors.E(errors.KindUnknown, "internal", "internal error", nil))
			}
		}()
		return handler(ctx, req)
	}
}
