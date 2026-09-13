package auth

// Role and permission names. They mirror the seed data in migration 000001;
// the strings (not these constants) are the wire/database contract.
const (
	RoleAdmin   = "ADMIN"
	RoleUser    = "USER"
	RoleWorker  = "WORKER"
	RoleService = "SERVICE"
)

const (
	PermUsersRead   = "users:read"
	PermUsersWrite  = "users:write"
	PermUsersDelete = "users:delete"
	PermJobsCreate  = "jobs:create"
	PermJobsRead    = "jobs:read"
	PermJobsCancel  = "jobs:cancel"
	// PermAdminAll is the wildcard granted to ADMIN (and, via the full
	// grant, to SERVICE). Holders are allowed to do anything.
	PermAdminAll = "admin:*"
)

// HasPermission reports whether perms grants need. A permission is granted
// by an exact match or by the admin wildcard "admin:*". Deliberately kept
// simple: no per-namespace wildcards like "users:*" — the platform only
// defines the single admin wildcard, and narrow wildcards are exactly the
// kind of cleverness that hides privilege-escalation bugs.
func HasPermission(perms []string, need string) bool {
	if need == "" {
		return false
	}
	for _, p := range perms {
		if p == need || p == PermAdminAll {
			return true
		}
	}
	return false
}
