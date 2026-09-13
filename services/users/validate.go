package users

import (
	"strings"

	gencommon "github.com/raven/platform/internal/gen/common"
)

const (
	defaultPage     = 1
	defaultPageSize = 20
	maxPageSize     = 100
)

// normalizePage clamps a PageRequest into sane bounds: page is 1-based,
// page_size defaults to 20 and is capped at 100 (platform contract).
func normalizePage(p *gencommon.PageRequest) (page, pageSize int) {
	page, pageSize = defaultPage, defaultPageSize
	if p == nil {
		return page, pageSize
	}
	if p.GetPage() > 0 {
		page = int(p.GetPage())
	}
	if p.GetPageSize() > 0 {
		pageSize = int(p.GetPageSize())
	}
	if pageSize > maxPageSize {
		pageSize = maxPageSize
	}
	return page, pageSize
}

// escapeLike escapes the LIKE/ILIKE metacharacters in user input so an
// email filter can never widen into a pattern the caller did not intend.
// The SQL using this must keep `ESCAPE '\'` in lockstep.
func escapeLike(s string) string {
	return likeEscaper.Replace(s)
}

var likeEscaper = strings.NewReplacer(
	`\`, `\\`,
	`%`, `\%`,
	`_`, `\_`,
)

// emailPattern turns a raw substring filter into a safe ILIKE pattern.
func emailPattern(filter string) string {
	return "%" + escapeLike(filter) + "%"
}
