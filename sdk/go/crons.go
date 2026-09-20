package raven

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"
)

// CreateCronRequest is the body of POST /api/crons. CronExpr is the
// classic 5-field form "min hour dom month dow" (numbers, * , - /; no
// names). Job fields follow the same rules as CreateJobRequest. Enabled
// defaults to true server-side when nil.
type CreateCronRequest struct {
	Name     string          `json:"name"`
	CronExpr string          `json:"cron_expr"`
	Type     string          `json:"type"`
	Payload  json.RawMessage `json:"payload"`
	Priority int             `json:"priority,omitempty"`
	Enabled  *bool           `json:"enabled,omitempty"`
}

// CreateCron registers a recurring schedule. The response includes the
// computed NextRunAt.
func (c *Client) CreateCron(ctx context.Context, req CreateCronRequest, opts ...requestOption) (*Cron, error) {
	var out Cron
	if err := c.do(ctx, http.MethodPost, "/api/crons", req, &out, true, opts...); err != nil {
		return nil, err
	}
	return &out, nil
}

// ListCrons pages through the caller's schedules.
func (c *Client) ListCrons(ctx context.Context, page, pageSize int) (*CronList, error) {
	q := url.Values{}
	if page > 0 {
		q.Set("page", strconv.Itoa(page))
	}
	if pageSize > 0 {
		q.Set("page_size", strconv.Itoa(pageSize))
	}
	var out CronList
	if err := c.do(ctx, http.MethodGet, "/api/crons", nil, &out, false,
		func(rc *requestConfig) { rc.query = q }); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteCron hard-deletes a schedule: it stops firing immediately. Unknown
// or foreign ids answer 404 cron_not_found.
func (c *Client) DeleteCron(ctx context.Context, id string, opts ...requestOption) error {
	return c.do(ctx, http.MethodDelete, "/api/crons/"+url.PathEscape(id), nil, nil, true, opts...)
}
