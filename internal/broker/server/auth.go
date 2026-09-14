package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

// ---- topic ACLs ----

// access classifies an operation for ACL checks.
type access int

const (
	accessNone  access = iota // any authenticated principal
	accessRead                // needs topics_read on the topic
	accessWrite               // needs topics_write on the topic
	accessAdmin               // needs the admin flag
)

// topicAllowed reports whether patterns grant access to topic: "*"
// matches everything, a trailing "*" is a prefix wildcard ("jobs.*"
// matches "jobs.p1"), anything else must match exactly.
func topicAllowed(patterns []string, topic string) bool {
	for _, pat := range patterns {
		switch {
		case pat == "*":
			return true
		case pat == topic:
			return true
		case len(pat) > 1 && pat[len(pat)-1] == '*' && len(topic) >= len(pat)-1 && topic[:len(pat)-1] == pat[:len(pat)-1]:
			return true
		}
	}
	return false
}

// authorize enforces the per-frame ACL when authentication is on. In
// open mode (no authenticator) it is a no-op, so dispatch stays a
// single code path. Denials are counted (OnACLDenied) and returned as
// UNAUTHORIZED; the message names the access and topic but never the
// grant lists — those are server-side configuration.
func (s *Server) authorize(p *Principal, acc access, topic string) error {
	if s.authenticator == nil {
		return nil
	}
	if p == nil {
		// The read-loop gate makes this unreachable; stay fail-closed.
		return protocol.NewError(protocol.CodeUnauthenticated, "authentication required")
	}
	if p.Admin {
		return nil
	}
	ok := false
	switch acc {
	case accessNone:
		ok = true
	case accessRead:
		ok = topicAllowed(p.TopicsRead, topic)
	case accessWrite:
		ok = topicAllowed(p.TopicsWrite, topic)
	case accessAdmin:
		ok = false
	}
	if ok {
		return nil
	}
	s.hooks.aclDenied()
	s.log.Warn("ACL denied",
		slog.String("key_id", p.ID),
		slog.String("access", accessName(acc)),
		slog.String("topic", topic))
	return protocol.NewError(protocol.CodeUnauthorized,
		fmt.Sprintf("%s on topic %q is not allowed for key %q", accessName(acc), topic, p.ID))
}

func accessName(acc access) string {
	switch acc {
	case accessRead:
		return "read"
	case accessWrite:
		return "write"
	case accessAdmin:
		return "admin"
	default:
		return "access"
	}
}
