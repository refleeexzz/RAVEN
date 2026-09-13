package auth

import "testing"

func TestHasPermission(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		perms []string
		need  string
		want  bool
	}{
		{name: "exact match", perms: []string{PermUsersRead, PermJobsCreate}, need: PermUsersRead, want: true},
		{name: "exact match, second entry", perms: []string{PermUsersRead, PermJobsCreate}, need: PermJobsCreate, want: true},
		{name: "missing permission", perms: []string{PermUsersRead}, need: PermUsersDelete, want: false},
		{name: "admin wildcard grants anything", perms: []string{PermAdminAll}, need: PermUsersDelete, want: true},
		{name: "admin wildcard grants jobs too", perms: []string{PermAdminAll}, need: PermJobsCancel, want: true},
		{name: "wildcard itself can be required", perms: []string{PermAdminAll}, need: PermAdminAll, want: true},
		{name: "no namespace wildcards", perms: []string{"users:*"}, need: PermUsersRead, want: false},
		{name: "empty perms", perms: nil, need: PermUsersRead, want: false},
		{name: "empty need", perms: []string{PermUsersRead}, need: "", want: false},
		{name: "prefix is not enough", perms: []string{PermUsersRead}, need: "users", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := HasPermission(tt.perms, tt.need); got != tt.want {
				t.Errorf("HasPermission(%v, %q) = %v, want %v", tt.perms, tt.need, got, tt.want)
			}
		})
	}
}
