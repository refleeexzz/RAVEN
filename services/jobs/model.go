// Package jobs implements the RAVEN jobs service: it accepts work over gRPC
// (:9083), stores it in Postgres, publishes it to the broker and tracks the
// lifecycle until a worker reports the outcome. Ops HTTP (:8083) serves
// /health, /ready and /metrics.
//
// The worker service imports this package for the shared job model and the
// jobs-table SQL: one table, one owner, fewer ways to drift apart.
package jobs

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"

	genjobs "github.com/raven/platform/internal/gen/jobs"
	"github.com/raven/platform/pkg/errors"
)

// Job statuses. These exact strings live in the database CHECK constraint
// and travel in events and broker messages.
type Status string

const (
	StatusQueued     Status = "QUEUED"
	StatusProcessing Status = "PROCESSING"
	StatusSuccess    Status = "SUCCESS"
	StatusFailed     Status = "FAILED"
	StatusRetrying   Status = "RETRYING"
	StatusCancelled  Status = "CANCELLED"
	StatusDead       Status = "DEAD"
)

// Broker topics (docs/contracts/ports-and-env.md §Job model). TopicRetry is
// created so a future broker-side delay queue can use it; today the worker
// itself schedules retries with a timer and republishes to TopicJobs.
const (
	TopicJobs  = "jobs"
	TopicDLQ   = "jobs.dlq"
	TopicRetry = "jobs.retry"
)

// Redis channel carrying job_status events to the websocket service.
const EventsChannel = "raven:events:jobs"

// KnownTypes is the accepted set of job types at create time. The worker has
// a handler for each of these; anything else would die in the worker anyway,
// so we reject it early with a clear error.
var KnownTypes = map[string]bool{
	"send_email":   true,
	"resize_image": true,
	"webhook":      true,
}

const (
	defaultMaxAttempts = 4
	maxMaxAttempts     = 25
	defaultPriority    = 5
)

// Job mirrors a row of the jobs table. Nullable columns use pointers.
type Job struct {
	ID             string
	Type           string
	Payload        string // raw JSON, validated before insert
	Status         Status
	Priority       int
	Attempts       int
	MaxAttempts    int
	IdempotencyKey *string
	OwnerID        *string
	CreatedAt      time.Time
	StartedAt      *time.Time
	FinishedAt     *time.Time
	Error          string
	WorkerID       string
}

// NewID builds a job id: "job_" + uuid. The prefix makes ids greppable in
// logs and dashboards.
func NewID() string { return "job_" + uuid.NewString() }

// ---------------------------------------------------------------------------
// State machine guards. The SQL updates enforce these rules atomically
// (UPDATE ... WHERE status IN (...)); these functions are the same rules in
// pure form so handlers can fail fast and tests can pin the table down.
// ---------------------------------------------------------------------------

// startable reports whether a worker may pick the job up (the cancel/duplicate
// fence): only work still waiting to run.
func startable(s Status) bool { return s == StatusQueued || s == StatusRetrying }

// cancellable reports whether CancelJob may move the job to CANCELLED.
func cancellable(s Status) bool { return s == StatusQueued || s == StatusRetrying }

// requeueable reports whether RequeueJob may resurrect the job: only DEAD
// jobs, per the contract (POST /api/jobs/{id}/requeue — DLQ requeue).
func requeueable(s Status) bool { return s == StatusDead }

// terminal reports whether the job will never change state again.
func terminal(s Status) bool {
	switch s {
	case StatusSuccess, StatusFailed, StatusCancelled, StatusDead:
		return true
	}
	return false
}

// legalTransition encodes the whole state machine in one table.
func legalTransition(from, to Status) bool {
	switch from {
	case StatusQueued:
		// PROCESSING: worker fence. CANCELLED: user cancel.
		// FAILED: broker unreachable right after create.
		return to == StatusProcessing || to == StatusCancelled || to == StatusFailed
	case StatusProcessing:
		return to == StatusSuccess || to == StatusRetrying || to == StatusDead
	case StatusRetrying:
		return to == StatusProcessing || to == StatusCancelled
	case StatusDead:
		return to == StatusQueued // RequeueJob
	}
	return false
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// validateCreate checks a CreateJob request and returns the normalized values
// (defaults applied). Pure: no I/O, so unit tests drive it directly.
func validateCreate(jobType, payloadJSON string, priority, maxAttempts int32) (prio, maxA int, err error) {
	if !KnownTypes[jobType] {
		return 0, 0, errors.E(errors.KindInvalid, "job_type_unknown",
			"type must be one of: send_email, resize_image, webhook", nil)
	}
	if payloadJSON == "" {
		return 0, 0, errors.E(errors.KindInvalid, "payload_required",
			"payload_json is required", nil)
	}
	if !json.Valid([]byte(payloadJSON)) {
		return 0, 0, errors.E(errors.KindInvalid, "payload_invalid_json",
			"payload_json must be valid JSON", nil)
	}
	if priority == 0 {
		priority = defaultPriority
	}
	if priority < 1 || priority > 10 {
		return 0, 0, errors.E(errors.KindInvalid, "priority_out_of_range",
			"priority must be between 1 and 10", nil)
	}
	if maxAttempts == 0 {
		maxAttempts = defaultMaxAttempts
	}
	if maxAttempts < 1 || maxAttempts > maxMaxAttempts {
		return 0, 0, errors.E(errors.KindInvalid, "max_attempts_out_of_range",
			"max_attempts must be between 1 and 25", nil)
	}
	return int(priority), int(maxAttempts), nil
}

// ---------------------------------------------------------------------------
// Proto mapping
// ---------------------------------------------------------------------------

var statusToProto = map[Status]genjobs.JobStatus{
	StatusQueued:     genjobs.JobStatus_JOB_STATUS_QUEUED,
	StatusProcessing: genjobs.JobStatus_JOB_STATUS_PROCESSING,
	StatusSuccess:    genjobs.JobStatus_JOB_STATUS_SUCCESS,
	StatusFailed:     genjobs.JobStatus_JOB_STATUS_FAILED,
	StatusRetrying:   genjobs.JobStatus_JOB_STATUS_RETRYING,
	StatusCancelled:  genjobs.JobStatus_JOB_STATUS_CANCELLED,
	StatusDead:       genjobs.JobStatus_JOB_STATUS_DEAD,
}

var protoToStatus = map[genjobs.JobStatus]Status{
	genjobs.JobStatus_JOB_STATUS_QUEUED:     StatusQueued,
	genjobs.JobStatus_JOB_STATUS_PROCESSING: StatusProcessing,
	genjobs.JobStatus_JOB_STATUS_SUCCESS:    StatusSuccess,
	genjobs.JobStatus_JOB_STATUS_FAILED:     StatusFailed,
	genjobs.JobStatus_JOB_STATUS_RETRYING:   StatusRetrying,
	genjobs.JobStatus_JOB_STATUS_CANCELLED:  StatusCancelled,
	genjobs.JobStatus_JOB_STATUS_DEAD:       StatusDead,
}

func unixOrZero(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.Unix()
}

// toProto converts the stored row to the wire type.
func (j *Job) toProto() *genjobs.Job {
	return &genjobs.Job{
		Id:          j.ID,
		Type:        j.Type,
		PayloadJson: j.Payload,
		Status:      statusToProto[j.Status],
		Priority:    int32(j.Priority),
		Attempts:    int32(j.Attempts),
		MaxAttempts: int32(j.MaxAttempts),
		CreatedAt:   j.CreatedAt.Unix(),
		StartedAt:   unixOrZero(j.StartedAt),
		FinishedAt:  unixOrZero(j.FinishedAt),
		Error:       j.Error,
		WorkerId:    j.WorkerID,
	}
}

// ---------------------------------------------------------------------------
// Broker message (contract §Job model). owner_id rides along so the DLQ is
// self-describing; workers still treat Postgres as the source of truth.
// ---------------------------------------------------------------------------

type jobMessage struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      Status          `json:"status"`
	Priority    int             `json:"priority"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	OwnerID     string          `json:"owner_id,omitempty"`
	CreatedAt   time.Time       `json:"created_at"`
	StartedAt   *time.Time      `json:"started_at,omitempty"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
	Error       string          `json:"error,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
}

// message renders the job as the broker payload.
func (j *Job) message() ([]byte, error) {
	m := jobMessage{
		ID:          j.ID,
		Type:        j.Type,
		Payload:     json.RawMessage(j.Payload),
		Status:      j.Status,
		Priority:    j.Priority,
		Attempts:    j.Attempts,
		MaxAttempts: j.MaxAttempts,
		CreatedAt:   j.CreatedAt.UTC(),
		StartedAt:   j.StartedAt,
		FinishedAt:  j.FinishedAt,
		Error:       j.Error,
		WorkerID:    j.WorkerID,
	}
	if j.OwnerID != nil {
		m.OwnerID = *j.OwnerID
	}
	return json.Marshal(m)
}

// ParseJobMessageID extracts just the job id from a broker message. Workers
// only need the id: after the fence update they trust the Postgres row, not
// the (possibly stale) message contents.
func ParseJobMessageID(raw []byte) (string, error) {
	var m struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", err
	}
	if m.ID == "" {
		return "", errors.E(errors.KindInvalid, "job_message_no_id",
			"broker message has no job id", nil)
	}
	return m.ID, nil
}

// MarshalMessage renders the job as the broker payload. Exported so the
// worker can republish retries without duplicating the schema.
func (j *Job) MarshalMessage() ([]byte, error) { return j.message() }
