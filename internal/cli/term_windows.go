//go:build windows

// term_windows.go disables console echo for the password prompt using the
// Win32 console API through stdlib syscall — no golang.org/x/term needed.
// If the console mode cannot be toggled (redirected stdin, exotic
// terminals) we fall back to a plain echoed read with a warning, which is
// still better than failing the login.
package cli

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"syscall"
	"unsafe"
)

const enableEchoInput = 0x0004

var (
	kernel32           = syscall.NewLazyDLL("kernel32.dll")
	procGetConsoleMode = kernel32.NewProc("GetConsoleMode")
	procSetConsoleMode = kernel32.NewProc("SetConsoleMode")
)

// readNoEcho reads one password line from f with console echo disabled.
func readNoEcho(f *os.File) (string, error) {
	h := syscall.Handle(f.Fd())
	var mode uint32
	r, _, err := procGetConsoleMode.Call(uintptr(h), uintptr(unsafe.Pointer(&mode)))
	if r == 0 {
		fmt.Fprintf(os.Stderr, "raven: warning: cannot disable echo (%v) — password will be visible\n", err)
		return readLine(f)
	}
	if r, _, err := procSetConsoleMode.Call(uintptr(h), uintptr(mode&^enableEchoInput)); r == 0 {
		fmt.Fprintf(os.Stderr, "raven: warning: cannot disable echo (%v) — password will be visible\n", err)
		return readLine(f)
	}
	defer procSetConsoleMode.Call(uintptr(h), uintptr(mode))
	return readLine(f)
}

func readLine(f *os.File) (string, error) {
	line, err := bufio.NewReader(f).ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("cannot read password: %w", err)
	}
	return strings.TrimRight(line, "\r\n"), nil
}
