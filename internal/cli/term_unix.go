//go:build !windows

// term_unix.go disables terminal echo for the password prompt via the
// stty(1) binary, which exists on every POSIX system the Go toolchain
// targets. Doing it with raw ioctls would need per-platform termios
// layouts that the stdlib does not export; stty keeps this file small and
// portable. If stty is missing we warn and read with echo on.
package cli

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// readNoEcho reads one password line from f with terminal echo disabled.
func readNoEcho(f *os.File) (password string, readErr error) {
	off := exec.Command("stty", "-echo")
	off.Stdin = f
	if err := off.Run(); err != nil {
		if _, err := fmt.Fprintf(os.Stderr, "raven: warning: cannot disable echo (%v) — password will be visible\n", err); err != nil {
			return "", err
		}
	} else {
		on := exec.Command("stty", "echo")
		on.Stdin = f
		defer func() {
			if err := on.Run(); err != nil {
				readErr = errors.Join(readErr, fmt.Errorf("cannot restore terminal echo: %w", err))
			}
		}()
	}
	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("cannot read password: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}
