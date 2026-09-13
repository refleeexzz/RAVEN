// Package config loads configuration from environment variables.
// Twelve-factor style: no config files, no flags, just env vars with
// sane defaults for local development. Every service reads the same
// variable names so Docker Compose and Kubernetes can inject them
// uniformly (see docs/contracts/ports-and-env.md).
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Get returns the env value or def when unset/empty.
func Get(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// MustGet returns the env value or panics with a clear message.
// Use it in main() only, for secrets that must never have defaults
// (JWT_SECRET, DATABASE_URL in prod...).
func MustGet(key string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	panic(fmt.Sprintf("config: required env var %s is not set", key))
}

// GetInt parses an int env var, falling back to def on absence or
// malformed values.
func GetInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

// GetBool parses a bool env var ("true"/"1"/"yes"), falling back to def.
func GetBool(key string, def bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

// GetDuration parses a duration env var ("500ms", "2s"), falling back
// to def.
func GetDuration(key string, def time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}
