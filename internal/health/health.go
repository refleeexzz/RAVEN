// Package health implements the /health (liveness) and /ready (readiness)
// endpoints every RAVEN service exposes. Liveness answers "is the process
// up?"; readiness runs registered dependency checks (Postgres, Redis,
// broker) and answers "can this pod take traffic?".
package health

import (
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Checker probes one dependency. It must be fast: readiness probes run
// on a tight kubelet cadence. Checkers should apply their own short
// timeout on top of the probe context.
type Checker func(ctx context.Context) error

// Registry collects dependency checkers and serves the probe endpoints.
type Registry struct {
	mu       sync.RWMutex
	checkers map[string]Checker
	timeout  time.Duration
}

// NewRegistry builds a registry. timeout bounds the whole readiness run.
func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{
		checkers: make(map[string]Checker),
		timeout:  timeout,
	}
}

// Register adds a named dependency check (e.g. "postgres", "redis").
func (r *Registry) Register(name string, c Checker) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.checkers[name] = c
}

// Liveness always returns 200 while the process can serve HTTP.
func (r *Registry) Liveness() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
}

// Readiness runs all checkers; 200 only when every dependency is healthy.
func (r *Registry) Readiness() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		ctx, cancel := context.WithTimeout(req.Context(), r.timeout)
		defer cancel()

		r.mu.RLock()
		checks := make(map[string]Checker, len(r.checkers))
		for name, c := range r.checkers {
			checks[name] = c
		}
		r.mu.RUnlock()

		results := make(map[string]string, len(checks))
		ready := true
		for name, c := range checks {
			if err := c(ctx); err != nil {
				results[name] = "fail: " + err.Error()
				ready = false
			} else {
				results[name] = "ok"
			}
		}

		status := http.StatusOK
		if !ready {
			status = http.StatusServiceUnavailable
		}
		writeJSON(w, status, map[string]any{
			"status": map[bool]string{true: "ready", false: "not_ready"}[ready],
			"checks": results,
		})
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
