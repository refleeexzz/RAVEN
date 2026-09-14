// jobs.go implements the `raven jobs ...` family: submit, list, get,
// cancel, requeue and watch. Wire shapes mirror services/gateway's public
// jobJSON — keep them in sync with handlers_jobs.go.
package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

// jobJSON is the public wire shape of a job (fixed by the API contract).
type jobJSON struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Priority    int32           `json:"priority"`
	Attempts    int32           `json:"attempts"`
	MaxAttempts int32           `json:"max_attempts"`
	CreatedAt   int64           `json:"created_at"`
	StartedAt   int64           `json:"started_at"`
	FinishedAt  int64           `json:"finished_at"`
	Error       string          `json:"error"`
	WorkerID    string          `json:"worker_id"`
}

// jobListJSON is the GET /api/jobs response.
type jobListJSON struct {
	Jobs []jobJSON `json:"jobs"`
	Page struct {
		Page     int32 `json:"page"`
		PageSize int32 `json:"page_size"`
		Total    int64 `json:"total"`
	} `json:"page"`
}

// terminalJobStatuses are the states after which a job never moves again.
var terminalJobStatuses = map[string]bool{
	"SUCCESS":   true,
	"FAILED":    true,
	"CANCELLED": true,
	"DEAD":      true,
}

// errJobUnsuccessful is returned by watch when the job ends in anything but
// SUCCESS; it carries the final status for the error message.
type errJobUnsuccessful struct{ status string }

func (e *errJobUnsuccessful) Error() string {
	return "job finished with status " + e.status
}

// cmdJobsSubmit handles `raven jobs submit --type T --payload P [flags]`.
func cmdJobsSubmit(e *env, args []string) error {
	fs := newFlagSet("jobs submit")
	var g globals
	g.register(fs)
	jtype := fs.String("type", "", "job type (send_email, resize_image, webhook, ...)")
	payload := fs.String("payload", "{}", "job payload as a JSON document")
	priority := fs.Int("priority", 0, "job priority (higher runs sooner)")
	maxAttempts := fs.Int("max-attempts", 0, "retry budget (0 = service default)")
	idemKey := fs.String("idempotency-key", "", "dedup key (default: a fresh random key per submission)")
	pos, err := parseAll(fs, args)
	if err != nil {
		return &usageError{msg: err.Error()}
	}
	if len(pos) != 0 {
		return &usageError{msg: "unexpected argument " + pos[0]}
	}
	if *jtype == "" {
		return &usageError{msg: "--type is required"}
	}
	if !json.Valid([]byte(*payload)) {
		return &usageError{msg: "--payload must be valid JSON"}
	}

	key := *idemKey
	if key == "" {
		key = "cli-" + randomHex(8)
	}

	c, err := e.newClient(&g, true)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var job jobJSON
	err = c.do(ctx, "POST", "/jobs", map[string]any{
		"type":         *jtype,
		"payload":      json.RawMessage(*payload),
		"priority":     *priority,
		"max_attempts": *maxAttempts,
	}, map[string]string{"Idempotency-Key": key}, &job)
	if err != nil {
		return err
	}

	if g.json {
		return printJSON(e.stdout, job)
	}
	fmt.Fprintf(e.stdout, "job %s submitted\n", job.ID)
	printJobDetail(e.stdout, &job)
	fmt.Fprintf(e.stdout, "idempotency-key: %s\n", key)
	return nil
}

// cmdJobsList handles `raven jobs list [--status S] [--type T] [--page N]`.
func cmdJobsList(e *env, args []string) error {
	fs := newFlagSet("jobs list")
	var g globals
	g.register(fs)
	status := fs.String("status", "", "filter by status (queued, processing, success, failed, retrying, cancelled, dead)")
	jtype := fs.String("type", "", "filter by job type")
	page := fs.Int("page", 1, "page number (1-based)")
	pageSize := fs.Int("page-size", 20, "items per page (max 100)")
	pos, err := parseAll(fs, args)
	if err != nil {
		return &usageError{msg: err.Error()}
	}
	if len(pos) != 0 {
		return &usageError{msg: "unexpected argument " + pos[0]}
	}

	c, err := e.newClient(&g, true)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	query := "?page=" + strconv.Itoa(*page) + "&page_size=" + strconv.Itoa(*pageSize)
	if *status != "" {
		query += "&status=" + url.QueryEscape(*status)
	}
	if *jtype != "" {
		query += "&type=" + url.QueryEscape(*jtype)
	}
	var list jobListJSON
	if err := c.do(ctx, "GET", "/jobs"+query, nil, nil, &list); err != nil {
		return err
	}

	if g.json {
		return printJSON(e.stdout, list)
	}
	rows := make([][]string, 0, len(list.Jobs))
	for _, j := range list.Jobs {
		rows = append(rows, []string{
			j.ID, j.Type, j.Status,
			strconv.Itoa(int(j.Priority)),
			strconv.Itoa(int(j.Attempts)) + "/" + strconv.Itoa(int(j.MaxAttempts)),
			fmtUnix(j.CreatedAt),
		})
	}
	table(e.stdout, []string{"ID", "TYPE", "STATUS", "PRI", "TRIES", "CREATED"}, rows)
	fmt.Fprintf(e.stdout, "page %d · %d/page · %d total\n", list.Page.Page, list.Page.PageSize, list.Page.Total)
	return nil
}

// cmdJobsGet handles `raven jobs get <id>`.
func cmdJobsGet(e *env, args []string) error {
	job, g, err := e.fetchJobByArgs("jobs get", args)
	if err != nil {
		return err
	}
	if g.json {
		return printJSON(e.stdout, job)
	}
	printJobDetail(e.stdout, job)
	return nil
}

// cmdJobsCancel handles `raven jobs cancel <id>`.
func cmdJobsCancel(e *env, args []string) error {
	return e.jobAction("jobs cancel", args, "cancelled")
}

// cmdJobsRequeue handles `raven jobs requeue <id>` (DEAD jobs only — the
// jobs service enforces that rule).
func cmdJobsRequeue(e *env, args []string) error {
	return e.jobAction("jobs requeue", args, "requeued")
}

// jobAction is the shared body of cancel and requeue: POST to an action
// endpoint and print the resulting job.
func (e *env) jobAction(name string, args []string, verb string) error {
	fs := newFlagSet(name)
	var g globals
	g.register(fs)
	pos, err := parseAll(fs, args)
	if err != nil {
		return &usageError{msg: err.Error()}
	}
	if len(pos) != 1 {
		return &usageError{msg: "expected exactly one job id"}
	}

	c, err := e.newClient(&g, true)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	action := name[len("jobs "):] // "cancel" | "requeue"
	var job jobJSON
	if err := c.do(ctx, "POST", "/jobs/"+pos[0]+"/"+action, nil, nil, &job); err != nil {
		return err
	}
	if g.json {
		return printJSON(e.stdout, job)
	}
	fmt.Fprintf(e.stdout, "job %s %s (status %s)\n", job.ID, verb, job.Status)
	return nil
}

// fetchJobByArgs parses "<id>" plus global flags and GETs the job.
func (e *env) fetchJobByArgs(name string, args []string) (*jobJSON, *globals, error) {
	fs := newFlagSet(name)
	var g globals
	g.register(fs)
	pos, err := parseAll(fs, args)
	if err != nil {
		return nil, nil, &usageError{msg: err.Error()}
	}
	if len(pos) != 1 {
		return nil, nil, &usageError{msg: "expected exactly one job id"}
	}
	c, err := e.newClient(&g, true)
	if err != nil {
		return nil, nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var job jobJSON
	if err := c.do(ctx, "GET", "/jobs/"+pos[0], nil, nil, &job); err != nil {
		return nil, nil, err
	}
	return &job, &g, nil
}

// cmdJobsWatch handles `raven jobs watch <id> [--interval 2s] [--timeout 10m]`.
// It polls until the job reaches a terminal status, printing every status
// change. Exit code: 0 on SUCCESS, 1 on any other terminal state (so shell
// scripts can branch on it).
func cmdJobsWatch(e *env, args []string) error {
	fs := newFlagSet("jobs watch")
	var g globals
	g.register(fs)
	interval := fs.Duration("interval", 2*time.Second, "polling interval")
	timeout := fs.Duration("timeout", 10*time.Minute, "give up after this long (0 = never)")
	pos, err := parseAll(fs, args)
	if err != nil {
		return &usageError{msg: err.Error()}
	}
	if len(pos) != 1 {
		return &usageError{msg: "expected exactly one job id"}
	}
	if *interval < 50*time.Millisecond {
		return &usageError{msg: "--interval must be at least 50ms"}
	}

	c, err := e.newClient(&g, true)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *timeout)
		defer cancel()
	}

	last := ""
	var final *jobJSON
	for {
		var job jobJSON
		err := c.do(ctx, "GET", "/jobs/"+pos[0], nil, nil, &job)
		if err != nil {
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("watch timed out after %s (last status %s)", *timeout, dash(last))
			}
			return err
		}
		if job.Status != last {
			line := fmt.Sprintf("%s  %s", clockNow(), job.Status)
			if job.Status == "PROCESSING" && job.WorkerID != "" {
				line += "  (worker " + job.WorkerID + ")"
			}
			if job.Error != "" && terminalJobStatuses[job.Status] {
				line += "  — " + job.Error
			}
			if g.json {
				raw, _ := json.Marshal(job)
				line = string(raw)
			}
			fmt.Fprintln(e.stdout, line)
			last = job.Status
		}
		if terminalJobStatuses[job.Status] {
			final = &job
			break
		}
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("watch timed out after %s (last status %s)", *timeout, dash(last))
			}
			return fmt.Errorf("watch interrupted (last status %s)", dash(last))
		case <-time.After(*interval):
		}
	}

	if final.Status != "SUCCESS" {
		return &errJobUnsuccessful{status: final.Status}
	}
	return nil
}

// printJobDetail renders one job as an aligned key/value view.
func printJobDetail(w io.Writer, j *jobJSON) {
	pairs := [][2]string{
		{"id", j.ID},
		{"type", j.Type},
		{"status", j.Status},
		{"priority", strconv.Itoa(int(j.Priority))},
		{"attempts", strconv.Itoa(int(j.Attempts)) + "/" + strconv.Itoa(int(j.MaxAttempts))},
		{"worker", dash(j.WorkerID)},
		{"created", fmtUnix(j.CreatedAt)},
		{"started", fmtUnix(j.StartedAt)},
		{"finished", fmtUnix(j.FinishedAt)},
		{"error", dash(j.Error)},
	}
	kv(w, pairs)
	payload := j.Payload
	var pretty bytes.Buffer
	if json.Indent(&pretty, j.Payload, "", "  ") == nil {
		payload = pretty.Bytes()
	}
	fmt.Fprintf(w, "payload: %s\n", string(payload))
}

// randomHex returns n random bytes hex-encoded (2n chars). Used for the
// default idempotency keys; crypto/rand failure is practically impossible
// and degrades to a timestamp-based key.
func randomHex(n int) string {
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return hex.EncodeToString([]byte(strconv.FormatInt(time.Now().UnixNano(), 16)))
	}
	return hex.EncodeToString(buf)
}
