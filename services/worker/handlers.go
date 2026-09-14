// Package worker implements the RAVEN worker service: it consumes jobs from
// the broker, executes them with bounded concurrency, retries failures with
// exponential backoff, sends exhausted jobs to the DLQ and heartbeats itself
// into the Redis worker registry. Ops HTTP (:8085) serves /health, /ready,
// /metrics and /debug/stats.
package worker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/refleeexzz/RAVEN/services/jobs"
)

// Handler executes one job. A nil error marks the attempt successful. A
// plain error means "retryable failure"; a *PermanentError sends the job
// straight to the DLQ without burning the remaining attempts.
type Handler func(ctx context.Context, job *jobs.Job) error

// PermanentError marks a failure that retrying can never fix (bad payload,
// unknown type).
type PermanentError struct{ msg string }

func (e *PermanentError) Error() string { return e.msg }

// permanentf builds a PermanentError.
func permanentf(format string, args ...any) *PermanentError {
	return &PermanentError{msg: fmt.Sprintf(format, args...)}
}

// isPermanent reports whether err must not be retried.
func isPermanent(err error) bool {
	var pe *PermanentError
	return errors.As(err, &pe)
}

// ---------------------------------------------------------------------------
// Handler registry
// ---------------------------------------------------------------------------

// newHandlers builds the type -> handler map. Lookup of an unknown type
// returns a PermanentError: no amount of retrying teaches the worker a type
// it does not know.
func newHandlers(emailLog *slog.Logger, httpClient *http.Client) map[string]Handler {
	return map[string]Handler{
		"send_email":   sendEmailHandler(emailLog),
		"resize_image": resizeImageHandler(),
		"webhook":      webhookHandler(httpClient),
	}
}

// lookupHandler resolves the type, returning a PermanentError for unknown
// types.
func lookupHandler(reg map[string]Handler, jobType string) (Handler, error) {
	h, ok := reg[jobType]
	if !ok {
		return nil, permanentf("unknown job type %q", jobType)
	}
	return h, nil
}

// ---------------------------------------------------------------------------
// send_email: simulated SMTP. 200-800ms of latency, then log the "send".
// ---------------------------------------------------------------------------

type emailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func sendEmailHandler(log *slog.Logger) Handler {
	return func(ctx context.Context, job *jobs.Job) error {
		var p emailPayload
		if err := json.Unmarshal([]byte(job.Payload), &p); err != nil {
			return permanentf("send_email payload is not an object: %v", err)
		}
		if p.To == "" {
			return permanentf("send_email payload requires a 'to' address")
		}

		latency := 200*time.Millisecond + time.Duration(rand.IntN(600))*time.Millisecond
		if err := sleepCtx(ctx, latency); err != nil {
			return err // job timeout / shutdown: retryable
		}
		log.Info("email sent",
			slog.String("job_id", job.ID),
			slog.String("to", p.To),
			slog.String("subject", p.Subject),
			slog.Duration("latency", latency),
		)
		return nil
	}
}

// ---------------------------------------------------------------------------
// resize_image: CPU-shaped work. Repeatedly hash the payload for ~300ms so
// benchmarks and dashboards see real utilization.
// ---------------------------------------------------------------------------

const resizeWorkDuration = 300 * time.Millisecond

func resizeImageHandler() Handler {
	return func(ctx context.Context, job *jobs.Job) error {
		var probe map[string]any
		if err := json.Unmarshal([]byte(job.Payload), &probe); err != nil {
			return permanentf("resize_image payload is not an object: %v", err)
		}

		digest := sha256.Sum256([]byte(job.Payload))
		deadline := time.Now().Add(resizeWorkDuration)
		i := 0
		for time.Now().Before(deadline) {
			for k := 0; k < 1024; k++ {
				digest = sha256.Sum256(digest[:])
			}
			i++
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		_ = digest // the point is the work, not the result
		return nil
	}
}

// ---------------------------------------------------------------------------
// webhook: real HTTP POST to payload.url with the job payload as JSON body.
// Non-2xx and transport errors are retryable; a missing/bad URL is not.
// ---------------------------------------------------------------------------

type webhookPayload struct {
	URL string `json:"url"`
}

func webhookHandler(client *http.Client) Handler {
	return func(ctx context.Context, job *jobs.Job) error {
		var p webhookPayload
		if err := json.Unmarshal([]byte(job.Payload), &p); err != nil {
			return permanentf("webhook payload is not an object: %v", err)
		}
		if p.URL == "" {
			return permanentf("webhook payload requires a 'url'")
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.URL,
			bytes.NewReader([]byte(job.Payload)))
		if err != nil {
			return permanentf("webhook url is invalid: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("webhook POST %s: %w", p.URL, err) // retryable
		}
		defer func() { _ = resp.Body.Close() }()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return fmt.Errorf("webhook POST %s: got status %d", p.URL, resp.StatusCode)
		}
		return nil
	}
}

// sleepCtx sleeps d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
