//go:build chaos

package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// apiClient talks to the gateway. It logs in once and re-logs in when the
// access token expires or a call comes back 401 (tokens live ~15 min and
// the suite can run longer than that).
type apiClient struct {
	base  string
	email string
	pass  string
	hc    *http.Client

	mu       sync.Mutex
	token    string
	tokenExp time.Time
}

func newAPIClient(base string) *apiClient {
	return &apiClient{
		base:  strings.TrimRight(base, "/"),
		email: envOr("RAVEN_CHAOS_EMAIL", "e2e@raven.dev"),
		pass:  envOr("RAVEN_CHAOS_PASSWORD", "supersecret123"),
		hc:    &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *apiClient) login(ctx context.Context) error {
	status, resp, err := c.raw(ctx, http.MethodPost, "/api/auth/login",
		map[string]string{"email": c.email, "password": c.pass}, nil)
	if err != nil {
		return fmt.Errorf("login request: %w", err)
	}
	if status != http.StatusOK {
		return fmt.Errorf("login: status %d: %s", status, truncate(string(resp)))
	}
	var lr struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(resp, &lr); err != nil {
		return fmt.Errorf("login: parse response: %w", err)
	}
	if lr.AccessToken == "" {
		return errors.New("login: empty access_token")
	}
	c.mu.Lock()
	c.token = lr.AccessToken
	c.tokenExp = time.Now().Add(10 * time.Minute)
	c.mu.Unlock()
	return nil
}

func (c *apiClient) raw(ctx context.Context, method, path string, body any, headers map[string]string) (int, []byte, error) {
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return 0, nil, err
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, b, err
}

// do performs an authenticated request, re-logging in once on 401 or on an
// expired token.
func (c *apiClient) do(ctx context.Context, method, path string, body any, headers map[string]string) (int, []byte, error) {
	for attempt := 0; attempt < 2; attempt++ {
		c.mu.Lock()
		tok, exp := c.token, c.tokenExp
		c.mu.Unlock()
		if tok == "" || time.Now().After(exp) {
			if err := c.login(ctx); err != nil {
				return 0, nil, fmt.Errorf("re-login: %w", err)
			}
			continue
		}
		h := map[string]string{"Authorization": "Bearer " + tok}
		for k, v := range headers {
			h[k] = v
		}
		status, b, err := c.raw(ctx, method, path, body, h)
		if err != nil {
			return status, b, err
		}
		if status == http.StatusUnauthorized {
			c.mu.Lock()
			c.token = ""
			c.mu.Unlock()
			continue
		}
		return status, b, nil
	}
	return 0, nil, errors.New("request failed after re-login")
}

// probe is a lightweight authenticated GET with no re-login: meant for
// sampling the API during an outage, when a login attempt would just hang.
func (c *apiClient) probe(ctx context.Context, path string) (int, error) {
	c.mu.Lock()
	tok := c.token
	c.mu.Unlock()
	status, _, err := c.raw(ctx, http.MethodGet, path, nil,
		map[string]string{"Authorization": "Bearer " + tok})
	return status, err
}

// ready hits the gateway readiness endpoint (no auth).
func (c *apiClient) ready(ctx context.Context) (int, error) {
	status, _, err := c.raw(ctx, http.MethodGet, "/ready", nil, nil)
	return status, err
}

// --- jobs API ------------------------------------------------------------

type Job struct {
	ID          string `json:"id"`
	Type        string `json:"type"`
	Status      string `json:"status"`
	Attempts    int    `json:"attempts"`
	MaxAttempts int    `json:"max_attempts"`
	Error       string `json:"error"`
	WorkerID    string `json:"worker_id"`
	StartedAt   int64  `json:"started_at"`
	FinishedAt  int64  `json:"finished_at"`
}

// notFound is a synthetic status: the API returned 404 for a job we created.
const notFound = "NOT_FOUND"

func terminal(status string) bool {
	switch status {
	case "SUCCESS", "FAILED", "CANCELLED", notFound:
		return true
	}
	return false
}

func (c *apiClient) createJob(ctx context.Context, typ string, payload map[string]any, priority int, idemKey string) (Job, int, error) {
	var j Job
	status, b, err := c.do(ctx, http.MethodPost, "/api/jobs", map[string]any{
		"type": typ, "payload": payload, "priority": priority,
	}, map[string]string{"Idempotency-Key": idemKey})
	if err != nil {
		return j, status, err
	}
	if status/100 == 2 {
		if err := json.Unmarshal(b, &j); err != nil {
			return j, status, fmt.Errorf("parse created job: %w", err)
		}
	}
	return j, status, nil
}

func (c *apiClient) getJob(ctx context.Context, id string) (Job, int, error) {
	var j Job
	status, b, err := c.do(ctx, http.MethodGet, "/api/jobs/"+id, nil, nil)
	if err != nil {
		return j, status, err
	}
	if status == http.StatusOK {
		if err := json.Unmarshal(b, &j); err != nil {
			return j, status, fmt.Errorf("parse job %s: %w", id, err)
		}
	}
	return j, status, nil
}

func (c *apiClient) requeueJob(ctx context.Context, id string) (int, error) {
	status, _, err := c.do(ctx, http.MethodPost, "/api/jobs/"+id+"/requeue", nil, nil)
	return status, err
}

// --- burst + polling helpers ----------------------------------------------

// burst submits jobs in the background so a scenario can kill things while
// work is still flowing in.
type burst struct {
	mu       sync.Mutex
	ids      []string
	failures []string
	done     chan struct{}
}

func (b *burst) created() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.ids)
}

func (b *burst) snapshot() (ids []string, failures []string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]string(nil), b.ids...), append([]string(nil), b.failures...)
}

// startBurst submits count jobs of the given type, pacing apart, with unique
// per-run idempotency keys (safe to re-run the suite).
func (k *chaosKit) startBurst(count int, typ string, payloadFor func(i int) map[string]any, pacing time.Duration) *burst {
	b := &burst{done: make(chan struct{})}
	go func() {
		defer close(b.done)
		for i := 0; i < count; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			job, code, err := k.api.createJob(ctx, typ, payloadFor(i), 5,
				fmt.Sprintf("chaos-%s-%s-%d", k.runID, typ, i))
			cancel()
			b.mu.Lock()
			if err != nil || code/100 != 2 {
				b.failures = append(b.failures, fmt.Sprintf("job %d: http=%d err=%v", i, code, err))
			} else {
				b.ids = append(b.ids, job.ID)
			}
			b.mu.Unlock()
			if pacing > 0 {
				time.Sleep(pacing)
			}
		}
	}()
	return b
}

// waitCreated blocks until at least n jobs were created (or the burst ended).
func (b *burst) waitCreated(n int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if b.created() >= n {
			return true
		}
		select {
		case <-b.done:
			return b.created() >= n
		case <-time.After(50 * time.Millisecond):
		}
	}
	return false
}

// waitTerminal polls the given job ids until all reach a terminal status, no
// non-terminal job has changed state for stallAfter (stuck), or the deadline
// passes — whichever comes first. Returns the last known state of every job.
// A job that returns 404 is recorded as NOT_FOUND — that is real data loss.
func (k *chaosKit) waitTerminal(ids []string, deadline time.Duration) map[string]Job {
	const stallAfter = 75 * time.Second
	final := make(map[string]Job, len(ids))
	lastSeen := make(map[string]Job, len(ids))
	lastChange := time.Now()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		remaining := 0
		changed := false
		for _, id := range ids {
			if terminal(final[id].Status) {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			job, code, err := k.api.getJob(ctx, id)
			cancel()
			switch {
			case err == nil && code == http.StatusOK:
				final[id] = job
				if prev, ok := lastSeen[id]; !ok || prev.Status != job.Status || prev.Attempts != job.Attempts {
					changed = true
					lastSeen[id] = job
				}
				if !terminal(job.Status) {
					remaining++
				}
			case err == nil && code == http.StatusNotFound:
				final[id] = Job{ID: id, Status: notFound}
			default:
				remaining++
			}
		}
		if remaining == 0 {
			return final
		}
		if changed {
			lastChange = time.Now()
		} else if time.Since(lastChange) > stallAfter {
			k.t.Logf("waitTerminal: no job state change for %s — declaring the rest stuck", stallAfter)
			return final
		}
		time.Sleep(750 * time.Millisecond)
	}
	return final
}

// jobStats is the analysis of a finished waitTerminal.
type jobStats struct {
	total         int
	success       int
	failed        int
	cancelled     int
	lost          int      // NOT_FOUND — row vanished, real data loss
	stranded      []string // stuck PROCESSING/RETRYING at the deadline
	pending       int      // still QUEUED or never fetched
	overAttempted []string // attempts > max_attempts
	duplicates    []string // attempts > 1
}

func analyze(ids []string, final map[string]Job) jobStats {
	s := jobStats{total: len(ids)}
	for _, id := range ids {
		j, ok := final[id]
		if !ok || j.Status == "" {
			s.pending++
			continue
		}
		switch j.Status {
		case "SUCCESS":
			s.success++
		case "FAILED":
			s.failed++
		case "CANCELLED":
			s.cancelled++
		case notFound:
			s.lost++
		case "PROCESSING", "RETRYING":
			s.stranded = append(s.stranded, fmt.Sprintf("%s(%s,attempts=%d)", id, j.Status, j.Attempts))
		default:
			s.pending++
		}
		if j.MaxAttempts > 0 && j.Attempts > j.MaxAttempts {
			s.overAttempted = append(s.overAttempted, fmt.Sprintf("%s(%d/%d)", id, j.Attempts, j.MaxAttempts))
		}
		if j.Attempts > 1 {
			s.duplicates = append(s.duplicates, fmt.Sprintf("%s(x%d)", id, j.Attempts))
		}
	}
	return s
}

func dupSummary(s jobStats) string {
	if len(s.duplicates) == 0 {
		return "0 jobs ran more than once"
	}
	return fmt.Sprintf("%d job(s) ran more than once: %s", len(s.duplicates), strings.Join(s.duplicates, ", "))
}

func lossSummary(s jobStats) string {
	if s.lost == 0 {
		return "no (0 lost)"
	}
	return fmt.Sprintf("YES — %d job(s) returned 404 after being created", s.lost)
}
