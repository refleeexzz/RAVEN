// Command raven is the official RAVEN CLI. It is deliberately thin: all
// logic lives in internal/cli so it stays testable; main only wires the
// real process streams and forwards the exit code.
package main

import (
	"os"

	"github.com/refleeexzz/RAVEN/internal/cli"
)

func main() {
	os.Exit(cli.Run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}
