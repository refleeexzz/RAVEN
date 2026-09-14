// routes.go is the public route table — the single place where method, path
// and required permission meet. Keep it in sync with
// docs/contracts/ports-and-env.md.
package gateway

import (
	"net/http"

	ravenauth "github.com/refleeexzz/RAVEN/internal/auth"
)

// route is one row of the public route table.
type route struct {
	method  string
	path    string // ServeMux pattern without the method, e.g. "/api/users/{id}"
	perm    string // "" → public; permAuthenticated → any valid token; else the required permission
	handler http.HandlerFunc
}

// table returns every public API route. The ops endpoints (/health, /ready,
// /metrics) and the /ws proxy are wired separately in server.go.
func (s *server) table() []route {
	return []route{
		// Public auth endpoints (rate-limited by IP).
		{"POST", "/api/auth/register", "", s.authH.register},
		{"POST", "/api/auth/login", "", s.authH.login},
		{"POST", "/api/auth/refresh", "", s.authH.refresh},
		{"POST", "/api/auth/logout", permAuthenticated, s.authH.logout},

		// Users CRUD.
		{"GET", "/api/users", ravenauth.PermUsersRead, s.usersH.list},
		{"GET", "/api/users/{id}", ravenauth.PermUsersRead, s.usersH.get},
		{"POST", "/api/users", ravenauth.PermUsersWrite, s.usersH.create},
		{"PUT", "/api/users/{id}", ravenauth.PermUsersWrite, s.usersH.update},
		{"DELETE", "/api/users/{id}", ravenauth.PermUsersDelete, s.usersH.remove},

		// Jobs.
		{"POST", "/api/jobs", ravenauth.PermJobsCreate, s.jobsH.create}, // forwards Idempotency-Key
		{"GET", "/api/jobs", ravenauth.PermJobsRead, s.jobsH.list},
		{"GET", "/api/jobs/{id}", ravenauth.PermJobsRead, s.jobsH.get},
		{"POST", "/api/jobs/{id}/cancel", ravenauth.PermJobsCancel, s.jobsH.cancel},
		{"POST", "/api/jobs/{id}/requeue", ravenauth.PermJobsCreate, s.jobsH.requeue},
		{"POST", "/api/jobs/{id}/replay", ravenauth.PermJobsCreate, s.jobsH.replay},

		// Cron schedules (jobs service, migration 000004).
		{"POST", "/api/crons", ravenauth.PermJobsCreate, s.jobsH.createCron},
		{"GET", "/api/crons", ravenauth.PermJobsRead, s.jobsH.listCrons},
		{"DELETE", "/api/crons/{id}", ravenauth.PermJobsCancel, s.jobsH.deleteCron},

		// Live worker registry (from Redis).
		{"GET", "/api/workers", ravenauth.PermJobsRead, s.jobsH.workers},

		// Platform audit trail, admin-only: users:delete is held by ADMIN
		// (via admin:*) and SERVICE in the seed RBAC and by nobody else.
		{"GET", "/api/audit", ravenauth.PermUsersDelete, s.auditH.list},

		// Aggregated service health for the console (public; light probes).
		{"GET", "/api/health/services", "", s.healthAgg.handler},
	}
}
