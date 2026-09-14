// store.go is the read side of the audit trail: a small filtered keyset
// paginator over audit_events for GET /api/audit.
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultLimit and MaxLimit bound the read API page size.
const (
	DefaultLimit = 50
	MaxLimit     = 500
)

// Filter narrows a List call. Empty fields match everything.
type Filter struct {
	Action   string // exact match, e.g. "auth.login.failure"
	Actor    string // exact actor_id match
	BeforeID int64  // keyset cursor: rows with id < BeforeID; 0 starts at the newest
	Limit    int    // clamped to [1, MaxLimit]; DefaultLimit when <= 0
}

func (f Filter) limit() int {
	if f.Limit <= 0 {
		return DefaultLimit
	}
	if f.Limit > MaxLimit {
		return MaxLimit
	}
	return f.Limit
}

// Store reads audit_events. It shares the writer's pool; reads are cheap
// (indexed, keyset-paginated) and admin-only at the edge.
type Store struct {
	pool *pgxpool.Pool
}

// NewStore builds a Store over pool.
func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

// List returns the newest events matching f, newest first. Scanning an
// unreadable row fails the whole call: a partial audit page that looks
// complete is worse than an error.
func (s *Store) List(ctx context.Context, f Filter) ([]Event, error) {
	var (
		where []string
		args  []any
	)
	if f.Action != "" {
		args = append(args, f.Action)
		where = append(where, fmt.Sprintf("action = $%d", len(args)))
	}
	if f.Actor != "" {
		args = append(args, f.Actor)
		where = append(where, fmt.Sprintf("actor_id = $%d", len(args)))
	}
	if f.BeforeID > 0 {
		args = append(args, f.BeforeID)
		where = append(where, fmt.Sprintf("id < $%d", len(args)))
	}

	query := `SELECT id, ts, actor_id, action, resource_type, resource_id,
		outcome, ip, user_agent, trace_id, detail
		FROM audit_events`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	args = append(args, f.limit())
	query += fmt.Sprintf(" ORDER BY id DESC LIMIT $%d", len(args))

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("audit: query events: %w", err)
	}
	defer rows.Close()

	var events []Event
	for rows.Next() {
		var e Event
		var outcome string
		var detail []byte
		if err := rows.Scan(&e.ID, &e.TS, &e.ActorID, &e.Action,
			&e.ResourceType, &e.ResourceID, &outcome, &e.IP, &e.UserAgent,
			&e.TraceID, &detail); err != nil {
			return nil, fmt.Errorf("audit: scan event: %w", err)
		}
		e.Outcome = Outcome(outcome)
		if len(detail) > 0 {
			if err := json.Unmarshal(detail, &e.Detail); err != nil {
				return nil, fmt.Errorf("audit: decode detail of event %d: %w", e.ID, err)
			}
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("audit: read events: %w", err)
	}
	return events, nil
}
