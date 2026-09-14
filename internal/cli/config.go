// config.go manages the on-disk CLI state: ~/.raven/config.json with the
// API base URL and the token pair from the last login. The file is written
// with 0600 (owner-only) permissions; on Windows that is best-effort — the
// Go runtime maps chmod onto the read-only attribute and real protection
// comes from the user profile ACL, which already restricts the directory
// to the owning account. See docs/cli.md.
package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// getenvDefault reads real environment variables.
var getenvDefault = os.Getenv

// defaultAPIURL is used when nothing else configures a base URL.
const defaultAPIURL = "http://localhost:8080/api"

// config is the JSON document stored on disk. Tokens stay out of flags and
// environment so they do not leak into shell history or process listings.
type config struct {
	APIURL       string `json:"api_url,omitempty"`
	Email        string `json:"email,omitempty"`
	AccessToken  string `json:"access_token,omitempty"`
	RefreshToken string `json:"refresh_token,omitempty"`
}

// configPath resolves where the config file lives:
// RAVEN_CONFIG > ~/.raven/config.json.
func (e *env) configPath() (string, error) {
	if p := e.getenv("RAVEN_CONFIG"); p != "" {
		return p, nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot find the home directory: %w", err)
	}
	return filepath.Join(home, ".raven", "config.json"), nil
}

// loadConfig reads the config file. A missing file is not an error — it
// just means "never logged in".
func (e *env) loadConfig() (config, error) {
	var cfg config
	path, err := e.configPath()
	if err != nil {
		return cfg, err
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return cfg, nil
	}
	if err != nil {
		return cfg, fmt.Errorf("cannot read %s: %w", path, err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return cfg, fmt.Errorf("config file %s is corrupt: %w", path, err)
	}
	return cfg, nil
}

// saveConfig writes the config file with owner-only permissions. The
// parent directory gets 0700 so tokens do not sit in a world-readable
// folder either.
func (e *env) saveConfig(cfg config) error {
	path, err := e.configPath()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("cannot create %s: %w", filepath.Dir(path), err)
	}
	raw, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return fmt.Errorf("cannot write %s: %w", path, err)
	}
	// WriteFile permissions only apply at creation; enforce 0600 on
	// pre-existing files too. Best-effort on Windows (see package doc).
	_ = os.Chmod(path, 0o600)
	return nil
}

// resolveAPIURL picks the base URL with the usual precedence:
// --api-url flag > RAVEN_API_URL env > saved config > default.
func (e *env) resolveAPIURL(g *globals, cfg config) string {
	if g.apiURL != "" {
		return g.apiURL
	}
	if v := e.getenv("RAVEN_API_URL"); v != "" {
		return v
	}
	if cfg.APIURL != "" {
		return cfg.APIURL
	}
	return defaultAPIURL
}

// newClient builds an API client from the environment and global flags.
// requireToken makes the command fail fast with exit code 3 when the user
// never logged in.
func (e *env) newClient(g *globals, requireToken bool) (*client, error) {
	cfg, err := e.loadConfig()
	if err != nil {
		return nil, err
	}
	c := newClientFromConfig(e.resolveAPIURL(g, cfg), cfg)
	if requireToken && c.token == "" {
		return nil, errNotLoggedIn
	}
	return c, nil
}
