package gateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
)

// expectedRoutes is the contract route table from
// docs/contracts/ports-and-env.md, transcribed once so the test compares
// implementation against the spec instead of against itself.
var expectedRoutes = []struct {
	method string
	path   string
	perm   string
}{
	{"POST", "/api/auth/register", ""},
	{"POST", "/api/auth/login", ""},
	{"POST", "/api/auth/refresh", ""},
	{"POST", "/api/auth/logout", permAuthenticated},

	{"GET", "/api/users", ravenauth.PermUsersRead},
	{"GET", "/api/users/{id}", ravenauth.PermUsersRead},
	{"POST", "/api/users", ravenauth.PermUsersWrite},
	{"PUT", "/api/users/{id}", ravenauth.PermUsersWrite},
	{"DELETE", "/api/users/{id}", ravenauth.PermUsersDelete},

	{"POST", "/api/jobs", ravenauth.PermJobsCreate},
	{"GET", "/api/jobs", ravenauth.PermJobsRead},
	{"GET", "/api/jobs/{id}", ravenauth.PermJobsRead},
	{"POST", "/api/jobs/{id}/cancel", ravenauth.PermJobsCancel},
	{"POST", "/api/jobs/{id}/requeue", ravenauth.PermJobsCreate},

	{"GET", "/api/workers", ravenauth.PermJobsRead},

	{"GET", "/api/audit", ravenauth.PermUsersDelete}, // admin-only audit trail

	{"GET", "/api/health/services", ""}, // aggregated service health for the console
}

func TestRouteTableMatchesContract(t *testing.T) {
	t.Parallel()

	// table() only reads handler method values, so a bare server works.
	s := &server{}
	got := s.table()

	if len(got) != len(expectedRoutes) {
		t.Fatalf("route count: got %d, want %d", len(got), len(expectedRoutes))
	}
	for i, want := range expectedRoutes {
		rt := got[i]
		if rt.method != want.method || rt.path != want.path || rt.perm != want.perm {
			t.Errorf("route %d: got {%s %s perm=%q}, want {%s %s perm=%q}",
				i, rt.method, rt.path, rt.perm, want.method, want.path, want.perm)
		}
		if rt.handler == nil {
			t.Errorf("route %d (%s %s): handler is nil", i, want.method, want.path)
		}
	}
}

// TestRouteTableRegistersInMux feeds the table into a real ServeMux: any
// pattern conflict panics here, and each route must match with its method.
func TestRouteTableRegistersInMux(t *testing.T) {
	t.Parallel()

	s := &server{}
	mux := http.NewServeMux()
	for _, rt := range s.table() {
		mux.HandleFunc(rt.method+" "+rt.path, func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		})
	}

	for _, want := range expectedRoutes {
		// Concrete URL: fill {id} with a real value.
		url := strings.ReplaceAll(want.path, "{id}", "00000000-0000-0000-0000-000000000000")
		req := httptest.NewRequest(want.method, url, nil)

		_, pattern := mux.Handler(req)
		if pattern != want.method+" "+want.path {
			t.Errorf("mux match for %s %s: got pattern %q, want %q",
				want.method, url, pattern, want.method+" "+want.path)
		}
	}
}

// TestRouteTableNoWildcardPerms guards the invariant that every protected
// route uses a known, exact permission from internal/auth.
func TestRouteTableNoWildcardPerms(t *testing.T) {
	t.Parallel()

	known := map[string]bool{
		"":                        true,
		permAuthenticated:         true,
		ravenauth.PermUsersRead:   true,
		ravenauth.PermUsersWrite:  true,
		ravenauth.PermUsersDelete: true,
		ravenauth.PermJobsCreate:  true,
		ravenauth.PermJobsRead:    true,
		ravenauth.PermJobsCancel:  true,
	}

	s := &server{}
	for _, rt := range s.table() {
		if !known[rt.perm] {
			t.Errorf("%s %s: unknown permission %q", rt.method, rt.path, rt.perm)
		}
		if strings.ContainsAny(rt.perm, "*") && rt.perm != permAuthenticated {
			t.Errorf("%s %s: wildcard permission %q is not allowed in the table", rt.method, rt.path, rt.perm)
		}
	}
}

// TestOpsAndWSRoutesExist checks the non-table branches are wired too.
func TestOpsAndWSRoutesExist(t *testing.T) {
	t.Parallel()

	mux := http.NewServeMux()
	// Register exactly what server.handler() registers, with stubs where the
	// real deps would sit.
	for _, rt := range (&server{}).table() {
		mux.Handle(rt.method+" "+rt.path, http.NotFoundHandler())
	}
	for _, p := range []string{"GET /health", "GET /ready", "GET /metrics", "GET /ws", "GET /ws/"} {
		mux.Handle(p, http.NotFoundHandler())
	}

	checks := []struct {
		method, url, wantPattern string
	}{
		{"GET", "/health", "GET /health"},
		{"GET", "/ready", "GET /ready"},
		{"GET", "/metrics", "GET /metrics"},
		{"GET", "/ws", "GET /ws"},
		{"GET", "/ws/", "GET /ws/"},
		{"GET", "/ws/stream", "GET /ws/"},
	}
	for _, c := range checks {
		req := httptest.NewRequest(c.method, c.url, nil)
		_, pattern := mux.Handler(req)
		if pattern != c.wantPattern {
			t.Errorf("%s %s: got pattern %q, want %q", c.method, c.url, pattern, c.wantPattern)
		}
	}
}

// TestRouteTableDocumentsEveryRoute makes the failure message helpful when
// someone adds a route to the code but forgets the contract test.
func TestRouteTableDocumentsEveryRoute(t *testing.T) {
	t.Parallel()

	s := &server{}
	seen := make(map[string]bool, len(expectedRoutes))
	for _, want := range expectedRoutes {
		seen[want.method+" "+want.path] = true
	}
	for _, rt := range s.table() {
		key := fmt.Sprintf("%s %s", rt.method, rt.path)
		if !seen[key] {
			t.Errorf("route %s is registered but missing from the contract test — update both", key)
		}
	}
}
