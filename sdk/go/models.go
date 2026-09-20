package raven

import (
	"encoding/json"
	"time"
)

// TokenPair is the credential bundle returned by Login and Refresh.
// AccessExpiresAt and RefreshExpiresAt are Unix seconds.
type TokenPair struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	AccessExpiresAt  int64  `json:"access_expires_at"`
	RefreshExpiresAt int64  `json:"refresh_expires_at"`
}

// accessExpiry returns the access-token expiry as a time.Time.
func (t *TokenPair) accessExpiry() time.Time {
	return time.Unix(t.AccessExpiresAt, 0)
}

// Job is one unit of work on the platform. Timestamps are Unix seconds;
// 0 means "not set".
type Job struct {
	ID           string          `json:"id"`
	Type         string          `json:"type"`
	Payload      json.RawMessage `json:"payload"`
	Status       string          `json:"status"`
	Priority     int             `json:"priority"`
	Attempts     int             `json:"attempts"`
	MaxAttempts  int             `json:"max_attempts"`
	CreatedAt    int64           `json:"created_at"`
	StartedAt    int64           `json:"started_at"`
	FinishedAt   int64           `json:"finished_at"`
	Error        string          `json:"error"`
	WorkerID     string          `json:"worker_id"`
	ScheduledAt  int64           `json:"scheduled_at"`
	ReplayedFrom string          `json:"replayed_from,omitempty"`
}

// Terminal reports whether the job is in a state it will never leave on
// its own: SUCCESS, FAILED, CANCELLED or DEAD.
func (j *Job) Terminal() bool {
	switch j.Status {
	case "SUCCESS", "FAILED", "CANCELLED", "DEAD":
		return true
	default:
		return false
	}
}

// Page is the pagination envelope returned by every list endpoint.
type Page struct {
	Page     int `json:"page"`
	PageSize int `json:"page_size"`
	Total    int `json:"total"`
}

// JobList is the response of ListJobs.
type JobList struct {
	Jobs []Job `json:"jobs"`
	Page Page  `json:"page"`
}

// Delivery is one webhook delivery attempt recorded for a job.
// StatusCode and LatencyMs are nil when no response ever came back
// (transport errors and egress-guard refusals).
type Delivery struct {
	ID              int64  `json:"id"`
	JobID           string `json:"job_id"`
	Attempt         int    `json:"attempt"`
	URL             string `json:"url"`
	StatusCode      *int   `json:"status_code"`
	LatencyMs       *int64 `json:"latency_ms"`
	ResponseSnippet string `json:"response_snippet"`
	Blocked         bool   `json:"blocked"`
	Error           string `json:"error"`
	TS              int64  `json:"ts"`
}

// DeliveryList is the response of JobDeliveries.
type DeliveryList struct {
	Deliveries []Delivery `json:"deliveries"`
	Page       Page       `json:"page"`
}

// Cron is a recurring job schedule. NextRunAt / LastRunAt / CreatedAt are
// Unix seconds.
type Cron struct {
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	CronExpr  string          `json:"cron_expr"`
	Type      string          `json:"type"`
	Payload   json.RawMessage `json:"payload"`
	Priority  int             `json:"priority"`
	Enabled   bool            `json:"enabled"`
	NextRunAt int64           `json:"next_run_at"`
	LastRunAt int64           `json:"last_run_at"`
	CreatedAt int64           `json:"created_at"`
}

// CronList is the response of ListCrons.
type CronList struct {
	Crons []Cron `json:"crons"`
	Page  Page   `json:"page"`
}

// APIKey is the metadata of an API key. The secret itself is only ever
// returned once, at creation, in CreateAPIKeyResponse.Key.
type APIKey struct {
	ID         string   `json:"id"`
	Name       string   `json:"name"`
	Prefix     string   `json:"prefix"`
	Scopes     []string `json:"scopes"`
	CreatedAt  string   `json:"created_at"`
	LastUsedAt *string  `json:"last_used_at"`
}

// CreateAPIKeyResponse is the one and only time the raw key material is
// shown. Store it; the platform keeps only its SHA-256 hash.
type CreateAPIKeyResponse struct {
	Key    string `json:"key"`
	APIKey APIKey `json:"api_key"`
}

// Worker is one live worker from the Redis registry. Counters come from a
// Redis hash, so they arrive as strings.
type Worker struct {
	ID            string `json:"id"`
	StartedAt     string `json:"started_at"`
	LastHeartbeat string `json:"last_heartbeat"`
	JobsProcessed string `json:"jobs_processed"`
	InFlight      string `json:"in_flight"`
}

// ServiceStatus is one row of the aggregated health grid.
type ServiceStatus struct {
	Name      string `json:"name"`
	Status    string `json:"status"` // ok | degraded | down
	LatencyMs int64  `json:"latency_ms"`
	Detail    string `json:"detail,omitempty"`
}

// HealthReport is the response of HealthServices.
type HealthReport struct {
	CheckedAt time.Time       `json:"checked_at"`
	Services  []ServiceStatus `json:"services"`
}

// AuditEvent is one row of the platform audit trail (admin only).
type AuditEvent struct {
	ID           int64          `json:"id,omitempty"`
	TS           time.Time      `json:"ts"`
	ActorID      string         `json:"actor_id"`
	Action       string         `json:"action"`
	ResourceType string         `json:"resource_type,omitempty"`
	ResourceID   string         `json:"resource_id,omitempty"`
	Outcome      string         `json:"outcome"`
	IP           string         `json:"ip,omitempty"`
	UserAgent    string         `json:"user_agent,omitempty"`
	TraceID      string         `json:"trace_id,omitempty"`
	Detail       map[string]any `json:"detail,omitempty"`
}

// AuditList is the response of ListAuditEvents. NextBeforeID is the keyset
// cursor for the next (older) page; zero means the end of the trail.
type AuditList struct {
	Events       []AuditEvent `json:"events"`
	NextBeforeID int64        `json:"next_before_id,omitempty"`
}
