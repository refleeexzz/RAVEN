package server

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net"
	"sync/atomic"
	"time"

	"github.com/refleeexzz/RAVEN/internal/broker/protocol"
)

// ErrBadCredentials is the only error an Authenticator should return
// for a failed login; the server maps it to UNAUTHENTICATED and counts
// it against the connection's failure budget. Any other error is
// treated as internal.
var ErrBadCredentials = errors.New("server: bad credentials")

// Principal is an authenticated connection identity plus its ACL
// grants. Topic lists hold exact topic names or wildcard patterns
// ("*" matches everything, "jobs.*" style prefixes end in "*"). Admin
// short-circuits every check.
type Principal struct {
	ID          string
	TopicsRead  []string
	TopicsWrite []string
	Admin       bool
}

// Authenticator verifies AUTH credentials. Returning (nil, nil) or
// ErrBadCredentials means "no such credentials" (counted, connection
// budget); a different error is logged as internal and also fails the
// attempt. Implementations must compare secrets in constant time.
type Authenticator interface {
	Authenticate(ctx context.Context, id, secret string) (*Principal, error)
}

// SecurityHooks are optional callbacks for metrics. Each runs at most
// once per rejected frame; they must be cheap and non-blocking.
type SecurityHooks struct {
	// OnAuthFailure fires on every rejected AUTH and every frame sent
	// before authentication completed.
	OnAuthFailure func()
	// OnACLDenied fires when an authenticated principal is denied by
	// the topic ACL.
	OnACLDenied func()
}

func (h SecurityHooks) authFailure() {
	if h.OnAuthFailure != nil {
		h.OnAuthFailure()
	}
}

func (h SecurityHooks) aclDenied() {
	if h.OnACLDenied != nil {
		h.OnACLDenied()
	}
}

// WithAuthenticator turns on connection authentication. nil keeps the
// server in open mode (the default, for dev compatibility): every
// opcode is served without AUTH and no ACL is enforced.
func WithAuthenticator(a Authenticator) Option {
	return func(s *Server) {
		s.authenticator = a
	}
}

// WithMaxAuthFailures sets how many failed AUTH attempts (or pre-auth
// frames) one connection gets before the broker answers
// UNAUTHENTICATED and closes it (0 keeps the default of 5).
func WithMaxAuthFailures(n int) Option {
	return func(s *Server) {
		if n > 0 {
			s.maxAuthFailures = n
		}
	}
}

// WithAuthTimeout sets how long a connection may stay unauthenticated
// before the read deadline reaps it (0 keeps the default of 10s).
// Only used when an Authenticator is set; it replaces the (much
// longer) idle timeout for the pre-auth phase so half-open unauth
// connections cannot hold slots.
func WithAuthTimeout(d time.Duration) Option {
	return func(s *Server) {
		if d > 0 {
			s.authTimeout = d
		}
	}
}

// WithSecurityHooks installs the metrics callbacks.
func WithSecurityHooks(h SecurityHooks) Option {
	return func(s *Server) {
		s.hooks = h
	}
}

// handlePreAuth processes one frame on a not-yet-authenticated
// connection. It returns the response frame to write and whether the
// connection may stay open: after maxAuthFailures rejections the
// caller flushes the final error and closes. AUTH frames are handled
// synchronously in the read loop (never dispatched to the pipelined
// handler goroutines), which keeps authentication strictly ordered
// before every later frame on the connection.
//
// failures is the connection's running count; principal is set exactly
// once, on success.
func (s *Server) handlePreAuth(ctx context.Context, conn net.Conn, f *protocol.Frame,
	failures *int, principal *atomic.Pointer[Principal]) (*protocol.Frame, bool) {

	reject := func(err *protocol.Error) (*protocol.Frame, bool) {
		*failures++
		s.hooks.authFailure()
		return protocol.ErrorFrame(f.CorrelationID, err), *failures < s.maxAuthFailures
	}

	if f.Opcode != protocol.OpAuth {
		s.log.Warn("frame before AUTH, rejecting",
			slog.String("remote", conn.RemoteAddr().String()),
			slog.String("op", f.Opcode.String()),
			slog.Int("failures", *failures+1),
			slog.Int("max", s.maxAuthFailures))
		return reject(protocol.NewError(protocol.CodeUnauthenticated,
			"authentication required: send AUTH first"))
	}

	var req protocol.AuthRequest
	if err := json.Unmarshal(f.Payload, &req); err != nil {
		return reject(protocol.NewError(protocol.CodeBadRequest, "malformed AUTH payload: "+err.Error()))
	}
	if req.ID == "" || req.Secret == "" {
		return reject(protocol.NewError(protocol.CodeBadRequest, "AUTH requires id and secret"))
	}
	p, err := s.authenticator.Authenticate(ctx, req.ID, req.Secret)
	if err != nil || p == nil {
		// Never log the secret; the id alone is enough to audit.
		if err != nil && !errors.Is(err, ErrBadCredentials) {
			s.log.Error("authenticator error",
				slog.String("remote", conn.RemoteAddr().String()), slog.Any("err", err))
		} else {
			s.log.Warn("authentication failed",
				slog.String("remote", conn.RemoteAddr().String()),
				slog.String("key_id", req.ID),
				slog.Int("failures", *failures+1),
				slog.Int("max", s.maxAuthFailures))
		}
		return reject(protocol.NewError(protocol.CodeUnauthenticated, "invalid credentials"))
	}

	principal.Store(p)
	s.log.Info("connection authenticated",
		slog.String("remote", conn.RemoteAddr().String()),
		slog.String("key_id", p.ID),
		slog.Bool("admin", p.Admin))
	return &protocol.Frame{
		Opcode:        protocol.OpAuth,
		CorrelationID: f.CorrelationID,
		Payload: mustJSON(protocol.AuthResponse{
			ID:          p.ID,
			TopicsRead:  p.TopicsRead,
			TopicsWrite: p.TopicsWrite,
			Admin:       p.Admin,
		}),
	}, true
}
