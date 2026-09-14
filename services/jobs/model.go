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
	"strconv"
	"time"

	"github.com/google/uuid"

	genjobs "github.com/refleeexzz/RAVEN/internal/gen/jobs"
	"github.com/refleeexzz/RAVEN/pkg/errors"
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
	// StatusScheduled is a delayed job waiting for its scheduled_at time
	// (migration 000004). It holds no lease and is invisible to the
	// sweeper and to workers until the dispatcher flips it to QUEUED.
	StatusScheduled Status = "SCHEDULED"
)

// Broker topics (docs/contracts/ports-and-env.md §Job model). TopicRetry is
// created so a future broker-side delay queue can use it; today the worker
// itself schedules retries with a timer and republishes to the routed
// execution topics.
const (
	TopicJobs  = "jobs"
	TopicDLQ   = "jobs.dlq"
	TopicRetry = "jobs.retry"

	// TopicPriorityPrefix is the priority-queue topic family pinned with the
	// worker agent: every execution publish goes to
	// "jobs.p<priority>" (p1 = most urgent, p9 = least). The legacy "jobs"
	// topic stays for compatibility while workers migrate (see
	// Producer.PublishExecution).
	TopicPriorityPrefix = "jobs.p"
)

// TopicForPriority maps a validated priority (1-9) to its execution topic.
// Priorities outside 1-9 are clamped to the nearest valid topic so a buggy
// caller can never escape the pinned topic family.
func TopicForPriority(priority int) string {
	if priority < 1 {
		priority = 1
	}
	if priority > MaxPriority {
		priority = MaxPriority
	}
	return TopicPriorityPrefix + strconv.Itoa(priority)
}

// executionTopics returns the topics an execution publish must hit, in
// order: the pinned priority topic first, then the legacy "jobs" topic when
// the compatibility fanout is enabled (JOBS_LEGACY_TOPIC_FANOUT, default on
// until workers subscribe to the priority topics directly). Duplicates are
// safe: the worker claim fence ignores the second copy.
func executionTopics(priority int, legacyFanout bool) []string {
	topics := []string{TopicForPriority(priority)}
	if legacyFanout {
		topics = append(topics, TopicJobs)
	}
	return topics
}

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

	// MaxPriority is the highest accepted priority value. The priority-queue
	// contract pinned with the worker agent defines topics jobs.p1..jobs.p9
	// (p1 = most urgent), so CreateJob validates 1-9. The database CHECK
	// still allows 1-10 (a compatible superset kept for older rows).
	MaxPriority = 9

	// MaxPayloadBytes caps CreateJob payload_json (JOBS-03). The payload is
	// stored as jsonb, copied into broker messages and re-read on every
	// attempt — unbounded payloads would be a storage/bandwidth/memory DoS.
	MaxPayloadBytes = 64 << 10 // 64 KiB

	// initialGeneration is the fencing token a job starts with. It matches
	// the execution_generation column default in migration 000003; the
	// sweeper bumps it every time it takes a stranded job over.
	initialGeneration = 1

	// scheduledAtPastTolerance is how far in the past scheduled_at may sit
	// before CreateJob rejects it. Anything inside the tolerance is treated
	// as "run now": clocks skew, and a user re-sending a just-due time
	// wants execution, not an error.
	scheduledAtPastTolerance = time.Minute
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

	// Lease fields (migration 000003). HeartbeatAt is the last sign of life
	// from the executing worker; LeaseUntil is the moment after which the
	// sweeper may declare the worker dead and take the job over.
	// ExecutionGeneration is the fencing token: every write that mutates an
	// execution must carry the current generation or it is rejected.
	HeartbeatAt         *time.Time
	LeaseUntil          *time.Time
	ExecutionGeneration int

	// Scheduling fields (migration 000004). ScheduledAt is set only while
	// the job waits in SCHEDULED (and stays on the row afterwards as a
	// record of when it was meant to run). ReplayedFrom is the source job
	// id when the job was created by ReplayJob — the audit link back to
	// the original.
	ScheduledAt  *time.Time
	ReplayedFrom *string
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
// SCHEDULED jobs are cancellable too: the schedule is simply dropped before
// the dispatcher ever publishes the work.
func cancellable(s Status) bool {
	return s == StatusQueued || s == StatusRetrying || s == StatusScheduled
}

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
	case StatusScheduled:
		// QUEUED: the dispatcher released the job when it came due.
		// CANCELLED: user cancel before it ever ran.
		return to == StatusQueued || to == StatusCancelled
	}
	return false
}

// ---------------------------------------------------------------------------
// Validation
// ---------------------------------------------------------------------------

// ValidateCreate checks a CreateJob request and returns the normalized values
// (defaults applied). Pure: no I/O, so unit tests drive it directly.
func ValidateCreate(jobType, payloadJSON string, priority, maxAttempts int32) (prio, maxA int, err error) {
	if !KnownTypes[jobType] {
		return 0, 0, errors.E(errors.KindInvalid, "job_type_unknown",
			"type must be one of: send_email, resize_image, webhook", nil)
	}
	if payloadJSON == "" {
		return 0, 0, errors.E(errors.KindInvalid, "payload_required",
			"payload_json is required", nil)
	}
	// Size check before the JSON scan: cheap reject for oversized payloads.
	if len(payloadJSON) > MaxPayloadBytes {
		return 0, 0, errors.E(errors.KindInvalid, "payload_too_large",
			"payload_json must be at most 64 KiB", nil)
	}
	if !json.Valid([]byte(payloadJSON)) {
		return 0, 0, errors.E(errors.KindInvalid, "payload_invalid_json",
			"payload_json must be valid JSON", nil)
	}
	if priority == 0 {
		priority = defaultPriority
	}
	if priority < 1 || priority > MaxPriority {
		return 0, 0, errors.E(errors.KindInvalid, "priority_out_of_range",
			"priority must be between 1 and 9", nil)
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

// ValidateScheduledAt normalizes the optional CreateJob scheduled_at field
// (unix seconds). Pure, like ValidateCreate; now is injected for testable
// tables. Rules (docs/scheduling.md):
//
//   - 0 means "run now" and returns nil.
//   - Negative is rejected outright.
//   - More than scheduledAtPastTolerance in the past is rejected: a distant
//     past time is almost always a client bug (wrong unit, wrong zone).
//   - Inside the tolerance the job runs now (nil — no schedule recorded).
//   - Anything else returns the time the job should fire, in UTC.
func ValidateScheduledAt(unix int64, now time.Time) (*time.Time, error) {
	if unix == 0 {
		return nil, nil
	}
	if unix < 0 {
		return nil, errors.E(errors.KindInvalid, "scheduled_at_invalid",
			"scheduled_at must be a unix timestamp in seconds", nil)
	}
	t := time.Unix(unix, 0).UTC()
	if t.Before(now.Add(-scheduledAtPastTolerance)) {
		return nil, errors.E(errors.KindInvalid, "scheduled_at_in_past",
			"scheduled_at is more than a minute in the past", nil)
	}
	if !t.After(now) {
		return nil, nil // inside the past tolerance: treat as "run now"
	}
	return &t, nil
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
	StatusScheduled:  genjobs.JobStatus_JOB_STATUS_SCHEDULED,
}

var protoToStatus = map[genjobs.JobStatus]Status{
	genjobs.JobStatus_JOB_STATUS_QUEUED:     StatusQueued,
	genjobs.JobStatus_JOB_STATUS_PROCESSING: StatusProcessing,
	genjobs.JobStatus_JOB_STATUS_SUCCESS:    StatusSuccess,
	genjobs.JobStatus_JOB_STATUS_FAILED:     StatusFailed,
	genjobs.JobStatus_JOB_STATUS_RETRYING:   StatusRetrying,
	genjobs.JobStatus_JOB_STATUS_CANCELLED:  StatusCancelled,
	genjobs.JobStatus_JOB_STATUS_DEAD:       StatusDead,
	genjobs.JobStatus_JOB_STATUS_SCHEDULED:  StatusScheduled,
}

func unixOrZero(t *time.Time) int64 {
	if t == nil {
		return 0
	}
	return t.Unix()
}

// toProto converts the stored row to the wire type.
func (j *Job) toProto() *genjobs.Job {
	p := &genjobs.Job{
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
		ScheduledAt: unixOrZero(j.ScheduledAt),
	}
	if j.ReplayedFrom != nil {
		p.ReplayedFrom = *j.ReplayedFrom
	}
	return p
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
	// ExecutionGeneration is the fencing token the worker must present back
	// to the database. Messages written before leases existed decode with 0,
	// which the claim fence treats as "unknown, accept the row's current
	// generation" — the upgrade path stays safe.
	ExecutionGeneration int `json:"execution_generation"`
}

// message renders the job as the broker payload.
func (j *Job) message() ([]byte, error) {
	m := jobMessage{
		ID:                  j.ID,
		Type:                j.Type,
		Payload:             json.RawMessage(j.Payload),
		Status:              j.Status,
		Priority:            j.Priority,
		Attempts:            j.Attempts,
		MaxAttempts:         j.MaxAttempts,
		CreatedAt:           j.CreatedAt.UTC(),
		StartedAt:           j.StartedAt,
		FinishedAt:          j.FinishedAt,
		Error:               j.Error,
		WorkerID:            j.WorkerID,
		ExecutionGeneration: j.ExecutionGeneration,
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
	id, _, err := ParseJobMessage(raw)
	return id, err
}

// ParseJobMessage extracts the job id and the execution generation from a
// broker message. Generation 0 means "the producer did not know the
// generation" (pre-lease messages, hand-built payloads); the claim fence
// accepts the row's current generation in that case. A non-zero generation
// that no longer matches the row means the message is stale — the sweeper
// already bumped the row and republished a newer message.
func ParseJobMessage(raw []byte) (id string, generation int, err error) {
	var m struct {
		ID                  string `json:"id"`
		ExecutionGeneration int    `json:"execution_generation"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		return "", 0, err
	}
	if m.ID == "" {
		return "", 0, errors.E(errors.KindInvalid, "job_message_no_id",
			"broker message has no job id", nil)
	}
	return m.ID, m.ExecutionGeneration, nil
}

// MarshalMessage renders the job as the broker payload. Exported so the
// worker can republish retries without duplicating the schema.
func (j *Job) MarshalMessage() ([]byte, error) { return j.message() }
