package jobs

import (
	"testing"
	"time"
)

// The cron parser is hand-rolled and security-relevant (it drives when
// tenant work fires), so it gets an extensive table: every syntax feature,
// every boundary, the Vixie dom/dow OR rule and a long list of invalid
// inputs that must be rejected with cron_expr_invalid.

func TestParseCronValid(t *testing.T) {
	cases := []struct {
		name string
		expr string
		// Spot-checks: field -> values that must be present.
		minutes []int
		hours   []int
		dom     []int
		months  []int
		dow     []int
	}{
		{name: "all wildcards", expr: "* * * * *",
			minutes: []int{0, 30, 59}, hours: []int{0, 12, 23}, dom: []int{1, 15, 31},
			months: []int{1, 6, 12}, dow: []int{0, 3, 6}},
		{name: "single values", expr: "5 4 3 2 1",
			minutes: []int{5}, hours: []int{4}, dom: []int{3}, months: []int{2}, dow: []int{1}},
		{name: "ranges", expr: "0-30 9-17 1-15 3-6 1-5",
			minutes: []int{0, 15, 30}, hours: []int{9, 13, 17}, dom: []int{1, 8, 15},
			months: []int{3, 4, 6}, dow: []int{1, 3, 5}},
		{name: "step over wildcard", expr: "*/15 */6 * * *",
			minutes: []int{0, 15, 30, 45}, hours: []int{0, 6, 12, 18}},
		{name: "step over range", expr: "0-10/2 8-20/4 * * *",
			minutes: []int{0, 2, 4, 6, 8, 10}, hours: []int{8, 12, 16, 20}},
		{name: "step from value to max", expr: "5/20 * * * *",
			minutes: []int{5, 25, 45}},
		{name: "lists", expr: "1,15,45 0,12 * * 1,3,5",
			minutes: []int{1, 15, 45}, hours: []int{0, 12}, dow: []int{1, 3, 5}},
		{name: "mixed list", expr: "0,*/20,50-55 * * * *",
			minutes: []int{0, 20, 40, 50, 51, 52, 53, 54, 55}},
		{name: "sunday as 7 folds to 0", expr: "0 0 * * 7",
			dow: []int{0}},
		{name: "sunday as 0", expr: "0 0 * * 0",
			dow: []int{0}},
		{name: "dow range including 7", expr: "0 0 * * 5-7",
			dow: []int{0, 5, 6}},
		{name: "extra whitespace tolerated", expr: "  0   12  * * 1 ",
			minutes: []int{0}, hours: []int{12}, dow: []int{1}},
		{name: "midnight", expr: "0 0 * * *", minutes: []int{0}, hours: []int{0}},
		{name: "end of day", expr: "59 23 * * *", minutes: []int{59}, hours: []int{23}},
		{name: "new year", expr: "0 0 1 1 *", dom: []int{1}, months: []int{1}},
		{name: "leap day", expr: "0 0 29 2 *", dom: []int{29}, months: []int{2}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := ParseCron(c.expr)
			if err != nil {
				t.Fatalf("ParseCron(%q): %v", c.expr, err)
			}
			check := func(field string, set map[int]bool, want []int) {
				t.Helper()
				for _, v := range want {
					if !set[v] {
						t.Errorf("%s: value %d missing for %q", field, v, c.expr)
					}
				}
			}
			check("minutes", s.minutes, c.minutes)
			check("hours", s.hours, c.hours)
			check("dom", s.dom, c.dom)
			check("months", s.months, c.months)
			check("dow", s.dow, c.dow)
		})
	}
}

func TestParseCronExactSets(t *testing.T) {
	// Set cardinality pins steps and folds down exactly.
	cases := []struct {
		expr                       string
		nMin, nH, nDom, nMon, nDow int
	}{
		{"* * * * *", 60, 24, 31, 12, 7},
		{"*/2 * * * *", 30, 24, 31, 12, 7},
		{"*/7 * * * *", 9, 24, 31, 12, 7}, // 0,7,...,56
		{"0-59/1 * * * *", 60, 24, 31, 12, 7},
		{"0 0 * * 0-7", 1, 1, 31, 12, 7},      // 7 folds: still all days
		{"1,1,1,2 * * * *", 2, 24, 31, 12, 7}, // duplicates collapse
	}
	for _, c := range cases {
		t.Run(c.expr, func(t *testing.T) {
			s, err := ParseCron(c.expr)
			if err != nil {
				t.Fatalf("ParseCron(%q): %v", c.expr, err)
			}
			if len(s.minutes) != c.nMin || len(s.hours) != c.nH ||
				len(s.dom) != c.nDom || len(s.months) != c.nMon || len(s.dow) != c.nDow {
				t.Errorf("%q: got (%d,%d,%d,%d,%d), want (%d,%d,%d,%d,%d)",
					c.expr, len(s.minutes), len(s.hours), len(s.dom), len(s.months), len(s.dow),
					c.nMin, c.nH, c.nDom, c.nMon, c.nDow)
			}
		})
	}
}

func TestParseCronInvalid(t *testing.T) {
	exprs := []string{
		"",              // empty
		"* * * *",       // too few fields
		"* * * * * *",   // too many fields
		"abc * * * *",   // not a number
		"0 0 0 * *",     // dom below range
		"0 0 32 * *",    // dom above range
		"60 * * * *",    // minute above range
		"-1 * * * *",    // negative
		"* 24 * * *",    // hour above range
		"* * * 0 *",     // month below range
		"* * * 13 *",    // month above range
		"* * * * 8",     // dow above range
		"*/0 * * * *",   // zero step
		"*/-2 * * * *",  // negative step
		"*/x * * * *",   // non-numeric step
		"5/2/3 * * * *", // double slash
		"10-5 * * * *",  // reversed range
		"1- * * * *",    // open range
		"-5 * * * *",    // open range (other side)
		"1,,2 * * * *",  // empty list item
		", * * * *",     // leading comma
		"* * * * MON",   // names unsupported
		"* * * JAN *",   // names unsupported
		"0 0 * * 7-5",   // reversed dow range
		"@daily",        // shorthand unsupported
		"0 0 * * * ",    // trailing tab-only extra field? (single trailing space ok — but this is fine)
	}
	for _, expr := range exprs[:len(exprs)-1] { // last one is actually valid
		t.Run("reject "+expr, func(t *testing.T) {
			s, err := ParseCron(expr)
			if err == nil {
				t.Fatalf("ParseCron(%q) succeeded with %+v, want cron_expr_invalid", expr, s)
			}
		})
	}
	// A trailing space alone is fine (Fields ignores it).
	if _, err := ParseCron("0 0 * * * "); err != nil {
		t.Errorf("trailing space must be tolerated: %v", err)
	}
}

func TestCronNext(t *testing.T) {
	at := func(y int, mo time.Month, d, h, mi int) time.Time {
		return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
	}
	cases := []struct {
		name string
		expr string
		from time.Time
		want time.Time
	}{
		{name: "next minute", expr: "* * * * *",
			from: at(2026, 3, 10, 12, 30), want: at(2026, 3, 10, 12, 31)},
		{name: "strictly after, same minute excluded", expr: "* * * * *",
			from: at(2026, 3, 10, 12, 30).Add(45 * time.Second), want: at(2026, 3, 10, 12, 31)},
		{name: "sub-minute truncates", expr: "* * * * *",
			from: at(2026, 3, 10, 12, 30).Add(1 * time.Second), want: at(2026, 3, 10, 12, 31)},
		{name: "quarter hour steps", expr: "*/15 * * * *",
			from: at(2026, 3, 10, 12, 16), want: at(2026, 3, 10, 12, 30)},
		{name: "hourly at :00", expr: "0 * * * *",
			from: at(2026, 3, 10, 12, 0), want: at(2026, 3, 10, 13, 0)},
		{name: "daily at noon", expr: "0 12 * * *",
			from: at(2026, 3, 10, 12, 0), want: at(2026, 3, 11, 12, 0)},
		{name: "daily later same day", expr: "0 12 * * *",
			from: at(2026, 3, 10, 9, 15), want: at(2026, 3, 10, 12, 0)},
		{name: "weekdays only skips weekend", expr: "0 9 * * 1-5",
			from: at(2026, 3, 13, 18, 0), // Friday evening
			want: at(2026, 3, 16, 9, 0)}, // Monday 9:00
		{name: "first of month", expr: "0 0 1 * *",
			from: at(2026, 3, 10, 0, 0), want: at(2026, 4, 1, 0, 0)},
		{name: "new year", expr: "0 0 1 1 *",
			from: at(2026, 3, 10, 0, 0), want: at(2027, 1, 1, 0, 0)},
		{name: "leap day from non-leap year", expr: "0 0 29 2 *",
			from: at(2026, 3, 1, 0, 0), want: at(2028, 2, 29, 0, 0)},
		{name: "leap day next occurrence same leap year", expr: "0 0 29 2 *",
			from: at(2028, 2, 1, 0, 0), want: at(2028, 2, 29, 0, 0)},
		{name: "dom 31 skips short months", expr: "0 0 31 * *",
			from: at(2026, 4, 1, 0, 0), want: at(2026, 5, 31, 0, 0)},
		// Vixie OR rule: both dom and dow restricted -> either matches.
		{name: "dom OR dow (dom hits first)", expr: "0 0 15 * 1",
			from: at(2026, 3, 10, 0, 0),  // Tuesday Mar 10
			want: at(2026, 3, 15, 0, 0)}, // Sunday Mar 15 (dom hit, before next Monday)
		{name: "dom OR dow (dow hits first)", expr: "0 0 20 * 2",
			from: at(2026, 3, 9, 0, 0),   // Monday Mar 9
			want: at(2026, 3, 10, 0, 0)}, // Tuesday Mar 10 (dow hit)
		// Only dom restricted: dow is '*', plain day matching.
		{name: "dom only", expr: "0 0 15 * *",
			from: at(2026, 3, 16, 0, 0), want: at(2026, 4, 15, 0, 0)},
		// Only dow restricted.
		{name: "dow only", expr: "0 0 * * 5",
			from: at(2026, 3, 10, 0, 0), want: at(2026, 3, 13, 0, 0)}, // Friday
		{name: "list of months", expr: "0 0 1 3,6,9,12 *",
			from: at(2026, 4, 1, 0, 0), want: at(2026, 6, 1, 0, 0)},
		{name: "minute range with step", expr: "10-20/5 * * * *",
			from: at(2026, 3, 10, 12, 12), want: at(2026, 3, 10, 12, 15)},
		{name: "hour range business hours", expr: "0 9-17 * * *",
			from: at(2026, 3, 10, 17, 30), want: at(2026, 3, 11, 9, 0)},
		{name: "year boundary", expr: "0 0 31 12 *",
			from: at(2026, 3, 10, 0, 0), want: at(2026, 12, 31, 0, 0)},
		{name: "input zone normalized to UTC", expr: "0 12 * * *",
			from: time.Date(2026, 3, 10, 9, 0, 0, 0, time.FixedZone("UTC-3", -3*3600)), // = 12:00 UTC
			want: at(2026, 3, 11, 12, 0)},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, err := ParseCron(c.expr)
			if err != nil {
				t.Fatalf("ParseCron(%q): %v", c.expr, err)
			}
			got, ok := s.Next(c.from)
			if !ok {
				t.Fatalf("Next(%v) found nothing for %q", c.from, c.expr)
			}
			if !got.Equal(c.want) {
				t.Errorf("Next(%q, %v) = %v, want %v", c.expr, c.from, got, c.want)
			}
		})
	}
}

func TestCronNextImpossible(t *testing.T) {
	// Feb 30 never comes: Next must give up instead of hanging.
	s, err := ParseCron("0 0 30 2 *")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if next, ok := s.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)); ok {
		t.Errorf("Next for Feb 30 = %v, want not-ok", next)
	}
}

// Next must be a pure function of (expr, from): repeated calls agree, and
// the result always matches the expression itself.
func TestCronNextIdempotent(t *testing.T) {
	s, err := ParseCron("23 4 12 7 3")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	from := time.Date(2026, 3, 10, 0, 0, 0, 0, time.UTC)
	a, okA := s.Next(from)
	b, okB := s.Next(from)
	if !okA || !okB || !a.Equal(b) {
		t.Fatalf("Next not idempotent: %v/%v", a, b)
	}
	if a.Minute() != 23 || a.Hour() != 4 {
		t.Fatalf("Next result does not match the expression: %v", a)
	}
	// Chaining: next(next(t)) is strictly later.
	c, _ := s.Next(a)
	if !c.After(a) {
		t.Fatalf("chained Next not advancing: %v then %v", a, c)
	}
}
