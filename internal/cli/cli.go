// Package cli implements the official RAVEN command-line interface
// (cmd/raven). It talks to the public REST API exposed by the gateway
// (default http://localhost:8080/api) and uses only the Go standard
// library: no cobra, no third-party terminal packages.
//
// Exit codes are part of the contract and safe to rely on in scripts:
//
//	0 — success
//	1 — generic error (API error, network failure, job failed, ...)
//	2 — invalid usage (unknown command, bad flags, missing arguments)
//	3 — authentication problem (not logged in, 401/403 from the API)
package cli

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"
)

// Version is the CLI version stamp. Release builds override it with:
//
//	-ldflags "-X github.com/refleeexzz/RAVEN/internal/cli.Version=vX.Y.Z"
var Version = "dev"

// Exit codes (see the package doc).
const (
	ExitOK    = 0
	ExitError = 1
	ExitUsage = 2
	ExitAuth  = 3
)

// usageError marks mistakes made by the person typing the command (bad
// flags, missing args). It maps to exit code 2 and prints command usage.
type usageError struct{ msg string }

func (e *usageError) Error() string { return e.msg }

// authError marks missing local credentials. It maps to exit code 3.
var errNotLoggedIn = errors.New("not logged in — run `raven login` first (or check RAVEN_API_URL)")

// env bundles everything a command needs that tests want to fake: streams,
// environment and clock-independent config resolution.
type env struct {
	stdin  io.Reader
	stdout io.Writer
	stderr io.Writer
	getenv func(string) string
}

func (e *env) envOr(key, def string) string {
	if v := e.getenv(key); v != "" {
		return v
	}
	return def
}

// globals are the flags accepted by every command.
type globals struct {
	json   bool
	apiURL string
}

// register adds the global flags to a command's FlagSet.
func (g *globals) register(fs *flag.FlagSet) {
	fs.BoolVar(&g.json, "json", false, "print machine-readable JSON")
	fs.StringVar(&g.apiURL, "api-url", "", "gateway API base URL (overrides RAVEN_API_URL and the saved config)")
}

// newFlagSet builds a FlagSet that stays quiet on parse errors — the
// dispatcher renders the error and usage itself in a consistent shape.
func newFlagSet(name string) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	return fs
}

// parseAll parses flags no matter where they appear: the stdlib flag
// package stops at the first positional argument, but users naturally type
// `raven jobs watch <id> --interval 5s`. It returns the positional args.
func parseAll(fs *flag.FlagSet, args []string) ([]string, error) {
	var positional []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return nil, err
		}
		args = fs.Args()
		if len(args) == 0 {
			break
		}
		positional = append(positional, args[0])
		args = args[1:]
	}
	return positional, nil
}

// command is one leaf of the command tree.
type command struct {
	name  string // as typed, e.g. "jobs submit"
	short string // one-line summary for the root help
	run   func(e *env, args []string) error
}

// commands returns the full command table.
func commands() []command {
	return []command{
		{"login", "log in and save the API token", cmdLogin},
		{"logout", "log out and drop the saved token", cmdLogout},
		{"jobs submit", "submit a job", cmdJobsSubmit},
		{"jobs list", "list your jobs", cmdJobsList},
		{"jobs get", "show one job", cmdJobsGet},
		{"jobs cancel", "cancel a queued/processing job", cmdJobsCancel},
		{"jobs requeue", "requeue a dead job from the DLQ", cmdJobsRequeue},
		{"jobs watch", "poll a job until it reaches a final state", cmdJobsWatch},
		{"workers", "list live workers", cmdWorkers},
		{"health", "show aggregated service health", cmdHealth},
		{"version", "print the CLI version", cmdVersion},
	}
}

// Run is the entry point of the CLI. It returns the process exit code;
// cmd/raven/main.go just forwards os.Exit(Run(...)).
func Run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	e := &env{stdin: stdin, stdout: stdout, stderr: stderr, getenv: getenvDefault}

	if len(args) == 0 {
		printRootUsage(stderr)
		return ExitUsage
	}

	// Help is accepted anywhere it makes sense.
	if args[0] == "help" || args[0] == "-h" || args[0] == "--help" {
		printRootUsage(stdout)
		return ExitOK
	}

	// Commands have one or two words ("health", "jobs submit").
	var cmd command
	var rest []string
	found := false
	for _, c := range commands() {
		parts := strings.Fields(c.name)
		if len(args) >= len(parts) && strings.EqualFold(strings.Join(args[:len(parts)], " "), c.name) {
			cmd = c
			rest = args[len(parts):]
			found = true
			break
		}
	}
	if !found {
		fmt.Fprintf(stderr, "raven: unknown command %q\n\n", strings.Join(args, " "))
		printRootUsage(stderr)
		return ExitUsage
	}

	err := cmd.run(e, rest)
	return renderError(e, cmd.name, err)
}

// renderError turns a command error into the right exit code and message.
func renderError(e *env, cmdName string, err error) int {
	if err == nil {
		return ExitOK
	}
	var ue *usageError
	if errors.As(err, &ue) {
		fmt.Fprintf(e.stderr, "raven %s: %s\n", cmdName, ue.msg)
		if u := commandUsage(cmdName); u != "" {
			fmt.Fprintf(e.stderr, "usage: %s\n", u)
		}
		return ExitUsage
	}
	if errors.Is(err, errNotLoggedIn) {
		fmt.Fprintf(e.stderr, "raven: %v\n", err)
		return ExitAuth
	}
	var ae *apiError
	if errors.As(err, &ae) && (ae.Status == 401 || ae.Status == 403) {
		fmt.Fprintf(e.stderr, "raven: %v\n", err)
		return ExitAuth
	}
	fmt.Fprintf(e.stderr, "raven: %v\n", err)
	return ExitError
}

// printRootUsage renders the top-level help.
func printRootUsage(w io.Writer) {
	fmt.Fprintln(w, `raven — the official RAVEN CLI

usage:
  raven <command> [flags]

commands:`)
	wid := 0
	for _, c := range commands() {
		if len(c.name) > wid {
			wid = len(c.name)
		}
	}
	for _, c := range commands() {
		fmt.Fprintf(w, "  %-*s  %s\n", wid, c.name, c.short)
	}
	fmt.Fprintln(w, `
flags accepted by every command:
  --json          machine-readable JSON output
  --api-url URL   gateway API base URL (default http://localhost:8080/api)

environment:
  RAVEN_API_URL   same as --api-url (flag wins)
  RAVEN_PASSWORD  password for `+"`raven login`"+` (skips the prompt)
  RAVEN_CONFIG    config file path (default ~/.raven/config.json)

exit codes: 0 ok · 1 error · 2 usage · 3 auth
more: docs/cli.md`)
}

// commandUsage is the one-line usage hint shown after a usage error.
func commandUsage(name string) string {
	switch name {
	case "login":
		return "raven login --email you@example.com [--password pw] [--api-url URL]"
	case "logout":
		return "raven logout"
	case "jobs submit":
		return "raven jobs submit --type send_email --payload '{" + `"to":"a@b.c"` + "}' [--priority N] [--max-attempts N] [--idempotency-key K]"
	case "jobs list":
		return "raven jobs list [--status queued] [--type T] [--page N] [--page-size N]"
	case "jobs get":
		return "raven jobs get <id>"
	case "jobs cancel":
		return "raven jobs cancel <id>"
	case "jobs requeue":
		return "raven jobs requeue <id>"
	case "jobs watch":
		return "raven jobs watch <id> [--interval 2s] [--timeout 10m]"
	case "workers":
		return "raven workers"
	case "health":
		return "raven health"
	case "version":
		return "raven version"
	default:
		return ""
	}
}
