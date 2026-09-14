// ops.go implements the read-only operational commands: `raven workers`,
// `raven health` and `raven version`.
package cli

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"time"
)

// workersPayload is the GET /api/workers response: a list of Redis hash
// field maps (the worker registry, see services/worker/registry.go).
type workersPayload struct {
	Workers []map[string]string `json:"workers"`
}

// cmdWorkers handles `raven workers`.
func cmdWorkers(e *env, args []string) error {
	fs := newFlagSet("workers")
	var g globals
	g.register(fs)
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

	var payload workersPayload
	if err := c.do(ctx, "GET", "/workers", nil, nil, &payload); err != nil {
		return err
	}
	if g.json {
		return printJSON(e.stdout, payload)
	}
	if len(payload.Workers) == 0 {
		fmt.Fprintln(e.stdout, "no workers registered right now")
		return nil
	}
	sort.Slice(payload.Workers, func(i, j int) bool {
		return payload.Workers[i]["id"] < payload.Workers[j]["id"]
	})
	rows := make([][]string, 0, len(payload.Workers))
	for _, w := range payload.Workers {
		rows = append(rows, []string{
			dash(w["id"]),
			dash(w["started_at"]),
			dash(w["last_heartbeat"]),
			dash(w["jobs_processed"]),
			dash(w["in_flight"]),
		})
	}
	table(e.stdout, []string{"ID", "STARTED", "LAST HEARTBEAT", "PROCESSED", "IN FLIGHT"}, rows)
	return nil
}

// healthPayload mirrors GET /api/health/services.
type healthPayload struct {
	CheckedAt time.Time `json:"checked_at"`
	Services  []struct {
		Name      string `json:"name"`
		Status    string `json:"status"` // ok | degraded | down
		LatencyMs int64  `json:"latency_ms"`
		Detail    string `json:"detail,omitempty"`
	} `json:"services"`
}

// cmdHealth handles `raven health`. It exits 0 when every service is ok
// and 1 when anything is degraded or down, so it works as a script probe.
// The endpoint is public — no token needed.
func cmdHealth(e *env, args []string) error {
	fs := newFlagSet("health")
	var g globals
	g.register(fs)
	pos, err := parseAll(fs, args)
	if err != nil {
		return &usageError{msg: err.Error()}
	}
	if len(pos) != 0 {
		return &usageError{msg: "unexpected argument " + pos[0]}
	}

	c, err := e.newClient(&g, false)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var payload healthPayload
	if err := c.do(ctx, "GET", "/health/services", nil, nil, &payload); err != nil {
		return err
	}
	if g.json {
		return printJSON(e.stdout, payload)
	}

	overall := "ok"
	rows := make([][]string, 0, len(payload.Services))
	for _, s := range payload.Services {
		if s.Status == "down" {
			overall = "down"
		} else if s.Status == "degraded" && overall == "ok" {
			overall = "degraded"
		}
		rows = append(rows, []string{
			s.Name, s.Status, fmt.Sprintf("%dms", s.LatencyMs), dash(s.Detail),
		})
	}
	table(e.stdout, []string{"SERVICE", "STATUS", "LATENCY", "DETAIL"}, rows)
	fmt.Fprintf(e.stdout, "overall: %s (checked %s)\n", overall,
		payload.CheckedAt.Local().Format("2006-01-02 15:04:05"))

	if overall != "ok" {
		return fmt.Errorf("some services are not healthy")
	}
	return nil
}

// cmdVersion handles `raven version`.
func cmdVersion(e *env, args []string) error {
	fs := newFlagSet("version")
	var g globals
	g.register(fs)
	if _, err := parseAll(fs, args); err != nil {
		return &usageError{msg: err.Error()}
	}
	if g.json {
		return printJSON(e.stdout, map[string]string{
			"version": Version,
			"go":      runtime.Version(),
			"os_arch": runtime.GOOS + "/" + runtime.GOARCH,
		})
	}
	fmt.Fprintf(e.stdout, "raven %s (%s, %s/%s)\n", Version, runtime.Version(), runtime.GOOS, runtime.GOARCH)
	return nil
}
