// audit_handler.go serves GET /api/audit, the admin-only read API over the
// audit trail, and builds the raven_audit_* collectors the writer reports
// to. The route requires users:delete — held only by ADMIN (via admin:*)
// and SERVICE in the seed RBAC, which makes it the de-facto admin gate.
package gateway

import (
	"context"
	"net/http"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/refleeexzz/RAVEN/internal/audit"
	"github.com/refleeexzz/RAVEN/pkg/errors"
	"github.com/refleeexzz/RAVEN/pkg/metrics"
)

// auditLister is the slice of *audit.Store the handler needs; an
// interface so handler tests can fake the database.
type auditLister interface {
	List(ctx context.Context, f audit.Filter) ([]audit.Event, error)
}

// auditHandlers serves /api/audit. store is nil when auditing is disabled
// (no database configured): the route stays registered and answers 503,
// which is honest instead of silently empty.
type auditHandlers struct {
	store auditLister
}

func newAuditHandlers(store auditLister) *auditHandlers {
	return &auditHandlers{store: store}
}

// auditListResponse is the wire shape of GET /api/audit.
type auditListResponse struct {
	Events []audit.Event `json:"events"`
	// NextBeforeID is the keyset cursor for the next (older) page; present
	// only when this page was full. Zero/omitted means: end of the trail.
	NextBeforeID int64 `json:"next_before_id,omitempty"`
}

// list handles GET /api/audit?limit=N&action=X&actor=Y&before_id=Z.
func (h *auditHandlers) list(w http.ResponseWriter, r *http.Request) {
	if h.store == nil {
		writeError(w, r, errors.E(errors.KindUnavailable, "audit_disabled",
			"the audit trail is not configured on this gateway", nil))
		return
	}

	q := r.URL.Query()
	filter := audit.Filter{
		Action: q.Get("action"),
		Actor:  q.Get("actor"),
	}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, r, errors.E(errors.KindInvalid, "invalid_limit",
				"limit must be a positive integer", nil))
			return
		}
		filter.Limit = n
	}
	if raw := q.Get("before_id"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 1 {
			writeError(w, r, errors.E(errors.KindInvalid, "invalid_before_id",
				"before_id must be a positive integer", nil))
			return
		}
		filter.BeforeID = n
	}

	events, err := h.store.List(r.Context(), filter)
	if err != nil {
		writeError(w, r, errors.E(errors.KindUnavailable, "audit_query_failed",
			"could not read the audit trail", err))
		return
	}
	if events == nil {
		events = []audit.Event{} // render [] instead of null
	}

	resp := auditListResponse{Events: events}
	// A full page means there may be older rows; hand back the cursor.
	effectiveLimit := filter.Limit
	if effectiveLimit <= 0 {
		effectiveLimit = audit.DefaultLimit
	}
	if effectiveLimit > audit.MaxLimit {
		effectiveLimit = audit.MaxLimit
	}
	if len(events) == effectiveLimit {
		resp.NextBeforeID = events[len(events)-1].ID
	}
	writeJSON(w, http.StatusOK, resp)
}

// newAuditMetrics builds the writer's collectors, registered in the
// gateway registry next to the other gateway metrics.
func newAuditMetrics(reg *metrics.Registry) audit.Metrics {
	m := audit.Metrics{
		Dropped: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "audit",
			Name:      "dropped_total",
			Help:      "Audit events dropped because the in-memory buffer was full.",
		}),
		Written: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "audit",
			Name:      "written_total",
			Help:      "Audit events successfully inserted into Postgres.",
		}),
		InsertErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "raven",
			Subsystem: "audit",
			Name:      "insert_errors_total",
			Help:      "Audit batch inserts that failed (the batch was dropped).",
		}),
	}
	reg.Register(m.Dropped, m.Written, m.InsertErrors)
	return m
}
