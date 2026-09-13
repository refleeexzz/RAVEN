package gateway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHealthAggregate(t *testing.T) {
	okProber := func(detail string) prober {
		return func(context.Context) (string, string) { return "ok", detail }
	}
	downProber := func(context.Context) (string, string) { return "down", "unreachable" }

	tests := []struct {
		name       string
		probers    map[string]prober
		wantStatus map[string]string
	}{
		{
			name: "all healthy",
			probers: map[string]prober{
				"gateway": okProber("self"), "auth": okProber("reachable"),
				"users": okProber("reachable"), "jobs": okProber("reachable"),
				"broker": okProber("tcp listener up"), "websocket": okProber("2 connections"),
				"worker_pool": okProber("5 workers"),
			},
			wantStatus: map[string]string{"gateway": "ok", "auth": "ok", "worker_pool": "ok"},
		},
		{
			name: "one down keeps the others ok",
			probers: map[string]prober{
				"gateway": okProber("self"), "auth": downProber,
				"users": okProber("reachable"), "jobs": okProber("reachable"),
				"broker": okProber("tcp listener up"), "websocket": okProber("0 connections"),
				"worker_pool": func(context.Context) (string, string) { return "degraded", "no workers registered" },
			},
			wantStatus: map[string]string{"auth": "down", "jobs": "ok", "worker_pool": "degraded"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := &healthAgg{probers: tt.probers}
			req := httptest.NewRequest(http.MethodGet, "/api/health/services", nil)
			rec := httptest.NewRecorder()
			agg.handler(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			var payload healthPayload
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
				t.Fatalf("invalid json: %v", err)
			}
			if len(payload.Services) != len(healthOrder) {
				t.Fatalf("got %d services, want %d", len(payload.Services), len(healthOrder))
			}
			// Display order must follow healthOrder.
			for i, s := range payload.Services {
				if s.Name != healthOrder[i] {
					t.Errorf("position %d = %s, want %s", i, s.Name, healthOrder[i])
				}
			}
			byName := map[string]string{}
			for _, s := range payload.Services {
				byName[s.Name] = s.Status
			}
			for name, want := range tt.wantStatus {
				if byName[name] != want {
					t.Errorf("service %s status = %s, want %s", name, byName[name], want)
				}
			}
			if payload.CheckedAt.IsZero() {
				t.Error("checked_at missing")
			}
		})
	}
}

func TestHealthAggregateSlowProbeDegrades(t *testing.T) {
	slow := func(ctx context.Context) (string, string) {
		select {
		case <-time.After(2 * time.Second):
			return "ok", "slow"
		case <-ctx.Done():
			return "down", "timeout" // probe ctx (1.5s) fires first
		}
	}
	agg := &healthAgg{probers: map[string]prober{
		"gateway": slow,
		"auth":    func(context.Context) (string, string) { return "ok", "reachable" },
	}}
	req := httptest.NewRequest(http.MethodGet, "/api/health/services", nil)
	rec := httptest.NewRecorder()
	start := time.Now()
	agg.handler(rec, req)

	// The slow probe must be bounded by probeTimeout, not its own 2s.
	if elapsed := time.Since(start); elapsed > probeTimeout+500*time.Millisecond {
		t.Fatalf("handler took %v, want <= ~2s", elapsed)
	}
	var payload healthPayload
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if payload.Services[0].Name == "gateway" && payload.Services[0].Status != "down" {
		t.Errorf("slow probe status = %s, want down (timed out)", payload.Services[0].Status)
	}
}
