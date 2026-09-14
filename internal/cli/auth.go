// auth.go implements `raven login` and `raven logout`.
//
// Password sources, in order: --password flag > RAVEN_PASSWORD env >
// interactive prompt with echo disabled (see term_*.go). The password is
// never written to disk — only the token pair lands in the config file.
package cli

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"
)

// tokenPairJSON mirrors the gateway login response.
type tokenPairJSON struct {
	AccessToken      string `json:"access_token"`
	RefreshToken     string `json:"refresh_token"`
	AccessExpiresAt  int64  `json:"access_expires_at"`
	RefreshExpiresAt int64  `json:"refresh_expires_at"`
}

// cmdLogin handles `raven login [--email E] [--password P] [--api-url U]`.
func cmdLogin(e *env, args []string) error {
	fs := newFlagSet("login")
	var g globals
	g.register(fs)
	email := fs.String("email", "", "account email")
	password := fs.String("password", "", "account password (prefer RAVEN_PASSWORD or the prompt)")
	pos, err := parseAll(fs, args)
	if err != nil {
		return &usageError{msg: err.Error()}
	}
	if len(pos) != 0 {
		return &usageError{msg: "unexpected argument " + pos[0]}
	}

	if *email == "" {
		line, err := promptLine(e, "email: ")
		if err != nil {
			return err
		}
		*email = strings.TrimSpace(line)
	}
	if *email == "" {
		return &usageError{msg: "email is required (--email)"}
	}

	pw := *password
	if pw == "" {
		pw = e.getenv("RAVEN_PASSWORD")
	}
	if pw == "" {
		var err error
		pw, err = promptPassword(e, "password: ")
		if err != nil {
			return err
		}
	}
	if pw == "" {
		return &usageError{msg: "password is required (--password, RAVEN_PASSWORD or the prompt)"}
	}

	cfg, err := e.loadConfig()
	if err != nil {
		return err
	}
	c := newClientFromConfig(e.resolveAPIURL(&g, cfg), config{})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var pair tokenPairJSON
	err = c.do(ctx, "POST", "/auth/login", map[string]string{
		"email":    *email,
		"password": pw,
	}, nil, &pair)
	if err != nil {
		return err
	}

	cfg.APIURL = c.base
	cfg.Email = *email
	cfg.AccessToken = pair.AccessToken
	cfg.RefreshToken = pair.RefreshToken
	if err := e.saveConfig(cfg); err != nil {
		return err
	}

	if g.json {
		return printJSON(e.stdout, map[string]any{
			"ok":      true,
			"email":   cfg.Email,
			"api_url": cfg.APIURL,
		})
	}
	fmt.Fprintf(e.stdout, "logged in as %s\n", cfg.Email)
	fmt.Fprintf(e.stdout, "api url: %s\n", cfg.APIURL)
	fmt.Fprintf(e.stdout, "token saved (access expires %s)\n", fmtUnix(pair.AccessExpiresAt))
	return nil
}

// cmdLogout handles `raven logout`. It revokes the refresh token remotely
// when possible, then always drops the local credentials.
func cmdLogout(e *env, args []string) error {
	fs := newFlagSet("logout")
	var g globals
	g.register(fs)
	pos, err := parseAll(fs, args)
	if err != nil {
		return &usageError{msg: err.Error()}
	}
	if len(pos) != 0 {
		return &usageError{msg: "unexpected argument " + pos[0]}
	}

	c, err := e.newClient(&g, true)
	if err != nil {
		return err
	}
	cfg, _ := e.loadConfig()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	remoteErr := c.do(ctx, "POST", "/auth/logout", map[string]string{
		"refresh_token": cfg.RefreshToken,
	}, nil, nil)

	// Local credentials go away no matter what the server said; a half-dead
	// session should not trap the user.
	cfg.AccessToken = ""
	cfg.RefreshToken = ""
	if err := e.saveConfig(cfg); err != nil {
		return err
	}

	if remoteErr != nil {
		fmt.Fprintf(e.stderr, "raven: warning: remote logout failed (%v); local token removed anyway\n", remoteErr)
	}
	if g.json {
		return printJSON(e.stdout, map[string]any{"ok": true})
	}
	fmt.Fprintln(e.stdout, "logged out")
	return nil
}

// promptLine reads one echoed line (used for the email prompt).
func promptLine(e *env, label string) (string, error) {
	fmt.Fprint(e.stderr, label)
	line, err := bufio.NewReader(e.stdin).ReadString('\n')
	if err != nil && line == "" {
		return "", fmt.Errorf("cannot read input: %w", err)
	}
	return line, nil
}

// promptPassword reads one line with terminal echo disabled. It needs the
// real stdin file; when stdin is not a terminal (pipes, tests) it refuses
// and points at the flag/env alternatives.
func promptPassword(e *env, label string) (string, error) {
	f, ok := e.stdin.(*os.File)
	if !ok {
		return "", fmt.Errorf("cannot prompt for a password on this stdin — use --password or RAVEN_PASSWORD")
	}
	fmt.Fprint(e.stderr, label)
	pw, err := readNoEcho(f)
	fmt.Fprintln(e.stderr) // echo was off; move to the next line ourselves
	if err != nil {
		return "", err
	}
	return pw, nil
}
