package jobs

import (
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/refleeexzz/RAVEN/pkg/errors"
)

// cron.go is a hand-rolled parser and scheduler for the classic 5-field
// cron expression "minute hour day-of-month month day-of-week". Stdlib
// only — the platform pins no external cron dependency.
//
// Supported syntax per field:
//
//	*            every value
//	*/n          every n-th value
//	a            single value
//	a-b          inclusive range
//	a-b/n        every n-th value inside the range
//	a/n          same as a-max/n (Vixie behavior)
//	x,y,z        a comma list of any of the above
//
// Ranges: minute 0-59, hour 0-23, day-of-month 1-31, month 1-12,
// day-of-week 0-6 with 7 also accepted as Sunday. Names (MON, JAN, ...)
// are NOT supported — numbers only.
//
// Day matching follows Vixie cron: when BOTH day-of-month and day-of-week
// are restricted (not '*'), a day matches when EITHER does; otherwise both
// fields must match.

// cronMaxSearchDays bounds Next()'s search: five years catches the rarest
// legitimate schedule (Feb 29 on a leap year) while still rejecting
// impossible ones ("0 0 31 2 *") quickly.
const cronMaxSearchDays = 5 * 366

type cronSchedule struct {
	minutes map[int]bool
	hours   map[int]bool
	dom     map[int]bool
	months  map[int]bool
	dow     map[int]bool

	// Restricted fields drive the Vixie dom/dow OR rule. A field holding
	// every allowed value ('*') is not restricted.
	domRestricted bool
	dowRestricted bool

	// Sorted views of minutes/hours keep Next() cheap.
	sortedMinutes []int
	sortedHours   []int
}

// ParseCron compiles a 5-field cron expression. The error carries the
// machine code "cron_expr_invalid" with a human reason.
func ParseCron(expr string) (*cronSchedule, error) {
	fields := strings.Fields(expr)
	if len(fields) != 5 {
		return nil, cronExprError("want 5 fields (min hour dom month dow), got " +
			strconv.Itoa(len(fields)))
	}

	minutes, err := parseCronField(fields[0], 0, 59, nil)
	if err != nil {
		return nil, err
	}
	hours, err := parseCronField(fields[1], 0, 23, nil)
	if err != nil {
		return nil, err
	}
	dom, err := parseCronField(fields[2], 1, 31, nil)
	if err != nil {
		return nil, err
	}
	months, err := parseCronField(fields[3], 1, 12, nil)
	if err != nil {
		return nil, err
	}
	// Day-of-week: accept 0-7, fold 7 onto 0 (both are Sunday).
	dow, err := parseCronField(fields[4], 0, 7, func(v int) int {
		if v == 7 {
			return 0
		}
		return v
	})
	if err != nil {
		return nil, err
	}

	c := &cronSchedule{
		minutes:       minutes,
		hours:         hours,
		dom:           dom,
		months:        months,
		dow:           dow,
		domRestricted: len(dom) < 31,
		dowRestricted: len(dow) < 7,
		sortedMinutes: sortedKeys(minutes),
		sortedHours:   sortedKeys(hours),
	}
	return c, nil
}

func cronExprError(why string) error {
	return errors.E(errors.KindInvalid, "cron_expr_invalid",
		"invalid cron expression: "+why, nil)
}

// parseCronField compiles one field into a set of allowed values. mapVal,
// when non-nil, post-processes each parsed value (used to fold dow 7 -> 0).
func parseCronField(spec string, min, max int, mapVal func(int) int) (map[int]bool, error) {
	out := map[int]bool{}
	add := func(v int) {
		if mapVal != nil {
			v = mapVal(v)
		}
		out[v] = true
	}

	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, cronExprError("empty list item in field " + strconv.Quote(spec))
		}

		// Optional step: everything before "/" is the base, after it the
		// step. Only one "/" allowed.
		base, stepStr, hasStep := strings.Cut(part, "/")
		step := 1
		if hasStep {
			n, err := strconv.Atoi(stepStr)
			if err != nil || n < 1 {
				return nil, cronExprError("bad step in " + strconv.Quote(part))
			}
			step = n
		}

		var lo, hi int
		switch {
		case base == "*":
			lo, hi = min, max
		case strings.Contains(base, "-"):
			bounds := strings.SplitN(base, "-", 2)
			var err error
			lo, err = parseCronValue(bounds[0], min, max)
			if err != nil {
				return nil, err
			}
			hi, err = parseCronValue(bounds[1], min, max)
			if err != nil {
				return nil, err
			}
			if lo > hi {
				return nil, cronExprError("reversed range " + strconv.Quote(part))
			}
		default:
			v, err := parseCronValue(base, min, max)
			if err != nil {
				return nil, err
			}
			lo = v
			if hasStep {
				hi = max // "a/n" means "a-max/n" (Vixie)
			} else {
				hi = v
			}
		}

		for v := lo; v <= hi; v += step {
			add(v)
		}
	}
	if len(out) == 0 {
		return nil, cronExprError("field " + strconv.Quote(spec) + " selects nothing")
	}
	return out, nil
}

// parseCronValue reads one numeric bound and checks the field range.
func parseCronValue(s string, min, max int) (int, error) {
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, cronExprError(strconv.Quote(s) + " is not a number (names are not supported)")
	}
	if v < min || v > max {
		return 0, cronExprError("value " + strconv.Itoa(v) + " out of range " +
			strconv.Itoa(min) + "-" + strconv.Itoa(max))
	}
	return v, nil
}

func sortedKeys(set map[int]bool) []int {
	out := make([]int, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Ints(out)
	return out
}

// matchesDay applies the Vixie dom/dow rule to one calendar day (month must
// match too).
func (c *cronSchedule) matchesDay(t time.Time) bool {
	if !c.months[int(t.Month())] {
		return false
	}
	domHit := c.dom[t.Day()]
	// Go Weekday: Sunday = 0, matching cron's numbering.
	dowHit := c.dow[int(t.Weekday())]
	if c.domRestricted && c.dowRestricted {
		return domHit || dowHit
	}
	return domHit && dowHit
}

// Next returns the first time strictly after from that matches the
// schedule, in UTC with minute precision. ok is false when nothing matches
// within cronMaxSearchDays (an impossible schedule like Feb 31).
func (c *cronSchedule) Next(from time.Time) (next time.Time, ok bool) {
	// Earliest candidate: the minute after `from`.
	earliest := from.UTC().Truncate(time.Minute).Add(time.Minute)
	day := time.Date(earliest.Year(), earliest.Month(), earliest.Day(), 0, 0, 0, 0, time.UTC)

	for i := 0; i < cronMaxSearchDays; i++ {
		if c.matchesDay(day) {
			for _, h := range c.sortedHours {
				for _, m := range c.sortedMinutes {
					cand := time.Date(day.Year(), day.Month(), day.Day(), h, m, 0, 0, time.UTC)
					if !cand.Before(earliest) {
						return cand, true
					}
				}
			}
		}
		day = day.AddDate(0, 0, 1)
	}
	return time.Time{}, false
}
