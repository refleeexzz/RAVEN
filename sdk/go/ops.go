package raven

import (
	"context"
	"net/http"
	"net/url"
	"strconv"
)

// ListWorkers reads the live worker registry straight from Redis: every
// listed worker heartbeat within the last 15 seconds.
func (c *Client) ListWorkers(ctx context.Context) ([]Worker, error) {
	var out struct {
		Workers []Worker `json:"workers"`
	}
	if err := c.do(ctx, http.MethodGet, "/api/workers", nil, &out, false); err != nil {
		return nil, err
	}
	return out.Workers, nil
}

// HealthServices returns the aggregated health grid the gateway computes
// by probing every service. The route is public.
func (c *Client) HealthServices(ctx context.Context) (*HealthReport, error) {
	var out HealthReport
	if err := c.do(ctx, http.MethodGet, "/api/health/services", nil, &out, false); err != nil {
		return nil, err
	}
	return &out, nil
}

// AuditFilter narrows ListAuditEvents. Action and Actor are exact-match
// filters; Limit caps the page (server default and max apply); BeforeID is
// the keyset cursor from a previous AuditList.NextBeforeID.
type AuditFilter struct {
	Action   string
	Actor    string
	Limit    int
	BeforeID int64
}

// ListAuditEvents pages the platform audit trail, newest first. The route
// requires users:delete — in the seed RBAC only ADMIN and SERVICE hold it,
// which makes it the de-facto admin gate.
func (c *Client) ListAuditEvents(ctx context.Context, f AuditFilter) (*AuditList, error) {
	q := url.Values{}
	if f.Action != "" {
		q.Set("action", f.Action)
	}
	if f.Actor != "" {
		q.Set("actor", f.Actor)
	}
	if f.Limit > 0 {
		q.Set("limit", strconv.Itoa(f.Limit))
	}
	if f.BeforeID > 0 {
		q.Set("before_id", strconv.FormatInt(f.BeforeID, 10))
	}
	var out AuditList
	if err := c.do(ctx, http.MethodGet, "/api/audit", nil, &out, false,
		func(rc *requestConfig) { rc.query = q }); err != nil {
		return nil, err
	}
	return &out, nil
}
