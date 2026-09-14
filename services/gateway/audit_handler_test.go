package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/refleeexzz/RAVEN/internal/audit"
)

// fakeLister replays a fixed result and records the filter it received.
type fakeLister struct {
	events []audit.Event
	err    error
	got    audit.Filter
}

func (f *fakeLister) List(_ context.Context, filter audit.Filter) ([]audit.Event, error) {
	f.got = filter
	return f.events, f.err
}

func doAuditRequest(t *testing.T, h *auditHandlers, url string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, url, nil)
	rec := httptest.NewRecorder()
	h.list(rec, req)
	return rec
}

func TestAuditHandlerListFiltersAndPagination(t *testing.T) {
	t.Parallel()

	lister := &fakeLister{events: []audit.Event{
		{ID: 42, TS: time.Now(), ActorID: "u-1", Action: "jobs.cancel", Outcome: audit.OutcomeSuccess},
		{ID: 41, TS: time.Now(), ActorID: "u-2", Action: "auth.login.failure", Outcome: audit.OutcomeFailure},
	}}
	h := newAuditHandlers(lister)

	rec := doAuditRequest(t, h, "/api/audit?limit=2&action=jobs.cancel&actor=u-1&before_id=50")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", rec.Code, rec.Body)
	}
	// The filter must reach the store untouched.
	if lister.got.Action != "jobs.cancel" || lister.got.Actor != "u-1" ||
		lister.got.BeforeID != 50 || lister.got.Limit != 2 {
		t.Errorf("filter = %+v, want action/actor/before_id/limit propagated", lister.got)
	}

	var resp auditListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Events) != 2 {
		t.Fatalf("events = %d, want 2", len(resp.Events))
	}
	// A full page hands back the oldest id as the next cursor.
	if resp.NextBeforeID != 41 {
		t.Errorf("next_before_id = %d, want 41", resp.NextBeforeID)
	}
}

func TestAuditHandlerPartialPageHasNoCursor(t *testing.T) {
	t.Parallel()

	lister := &fakeLister{events: []audit.Event{
		{ID: 42, TS: time.Now(), ActorID: "u-1", Action: "jobs.cancel", Outcome: audit.OutcomeSuccess},
	}}
	h := newAuditHandlers(lister)

	rec := doAuditRequest(t, h, "/api/audit?limit=10")
	var resp auditListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.NextBeforeID != 0 {
		t.Errorf("next_before_id = %d, want 0 (partial page = end of trail)", resp.NextBeforeID)
	}
}

func TestAuditHandlerEmptyRendersArray(t *testing.T) {
	t.Parallel()

	h := newAuditHandlers(&fakeLister{})
	rec := doAuditRequest(t, h, "/api/audit")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var resp auditListResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Events == nil || len(resp.Events) != 0 {
		t.Errorf("events = %v, want empty array (not null)", resp.Events)
	}
}

func TestAuditHandlerRejectsBadParams(t *testing.T) {
	t.Parallel()

	h := newAuditHandlers(&fakeLister{})
	for _, url := range []string{
		"/api/audit?limit=abc",
		"/api/audit?limit=-3",
		"/api/audit?before_id=xyz",
		"/api/audit?before_id=-1",
	} {
		if rec := doAuditRequest(t, h, url); rec.Code != http.StatusBadRequest {
			t.Errorf("GET %s: status = %d, want 400", url, rec.Code)
		}
	}
}

func TestAuditHandlerStoreErrorIs503(t *testing.T) {
	t.Parallel()

	h := newAuditHandlers(&fakeLister{err: errors.New("db down")})
	if rec := doAuditRequest(t, h, "/api/audit"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestAuditHandlerDisabledIs503(t *testing.T) {
	t.Parallel()

	h := newAuditHandlers(nil)
	if rec := doAuditRequest(t, h, "/api/audit"); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 when audit is disabled", rec.Code)
	}
}
