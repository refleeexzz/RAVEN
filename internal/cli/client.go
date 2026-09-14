// client.go is the thin HTTP layer over the gateway's public REST API.
// It knows three things: how to attach the bearer token, how to decode
// JSON responses, and how to render the standard error envelope
// {"error":{"code","message","request_id"}} as a readable Go error.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// client talks to one gateway base URL.
type client struct {
	base  string // e.g. http://localhost:8080/api (no trailing slash)
	token string // access token; empty means anonymous
	hc    *http.Client
	ua    string
}

// newClientFromConfig builds a client from the resolved URL and config.
func newClientFromConfig(baseURL string, cfg config) *client {
	return &client{
		base:  strings.TrimRight(baseURL, "/"),
		token: cfg.AccessToken,
		hc:    &http.Client{Timeout: 30 * time.Second},
		ua:    "raven-cli/" + Version,
	}
}

// apiError is the gateway error envelope, decoded.
type apiError struct {
	Status    int
	Code      string
	Message   string
	RequestID string
}

// Error renders the full envelope so support can trace failures:
// "invalid_json: request body must be valid JSON (request id: 01J...)".
func (e *apiError) Error() string {
	msg := e.Code
	if e.Message != "" {
		msg += ": " + e.Message
	}
	if e.RequestID != "" {
		msg += " (request id: " + e.RequestID + ")"
	}
	return msg
}

// errorEnvelope mirrors the gateway's wire shape for failures.
type errorEnvelope struct {
	Error struct {
		Code      string `json:"code"`
		Message   string `json:"message"`
		RequestID string `json:"request_id"`
	} `json:"error"`
}

// do performs one request. body is JSON-encoded when non-nil; out is
// JSON-decoded when non-nil. Any non-2xx status becomes an *apiError.
func (c *client) do(ctx context.Context, method, path string, body any, headers map[string]string, out any) error {
	var rdr io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("cannot encode request body: %w", err)
		}
		rdr = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rdr)
	if err != nil {
		return fmt.Errorf("cannot build request: %w", err)
	}
	req.Header.Set("User-Agent", c.ua)
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("cannot reach the gateway at %s: %w", c.base, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return fmt.Errorf("cannot read the response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var env errorEnvelope
		if json.Unmarshal(raw, &env) == nil && (env.Error.Code != "" || env.Error.Message != "") {
			return &apiError{
				Status:    resp.StatusCode,
				Code:      env.Error.Code,
				Message:   env.Error.Message,
				RequestID: env.Error.RequestID,
			}
		}
		// Not our envelope (proxy error, HTML page, ...). Do not leak raw
		// bodies; the status alone is the safe, useful part.
		return &apiError{
			Status:  resp.StatusCode,
			Code:    fmt.Sprintf("http_%d", resp.StatusCode),
			Message: http.StatusText(resp.StatusCode),
		}
	}

	if out == nil || len(raw) == 0 {
		return nil
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("cannot decode the response: %w", err)
	}
	return nil
}
