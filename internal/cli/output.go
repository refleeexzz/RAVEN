// output.go holds the presentation helpers: aligned tables (text/tabwriter),
// pretty JSON and timestamp formatting. The CLI prints no ANSI colors at
// all — output stays clean when piped, and readable on every terminal.
package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"
)

// printJSON writes v as indented JSON followed by a newline.
func printJSON(w io.Writer, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(w, string(raw))
	return err
}

// table renders headers plus rows aligned with tabs. Empty cells should be
// passed as "-" by callers so the grid reads well.
func table(w io.Writer, headers []string, rows [][]string) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, strings.Join(headers, "\t"))
	for _, row := range rows {
		fmt.Fprintln(tw, strings.Join(row, "\t"))
	}
	_ = tw.Flush()
}

// kv renders a "key: value" detail view, aligned on the colon.
func kv(w io.Writer, pairs [][2]string) {
	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for _, p := range pairs {
		fmt.Fprintf(tw, "%s\t%s\n", p[0]+":", p[1])
	}
	_ = tw.Flush()
}

// dash maps empty strings to "-" so table cells never collapse.
func dash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// fmtUnix renders a unix-seconds timestamp in local time; 0 means "not
// set" on the API and becomes "-".
func fmtUnix(sec int64) string {
	if sec <= 0 {
		return "-"
	}
	return time.Unix(sec, 0).Local().Format("2006-01-02 15:04:05")
}

// clockNow is the HH:MM:SS prefix used by `jobs watch` lines.
func clockNow() string {
	return time.Now().Local().Format("15:04:05")
}
