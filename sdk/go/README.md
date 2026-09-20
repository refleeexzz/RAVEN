# raven — RAVEN Go SDK

The official Go client for the [RAVEN](../../README.md) distributed jobs
platform. It talks to the public REST API exposed by the gateway and has
**zero dependencies** — standard library only.

```
go get github.com/refleeexzz/RAVEN/sdk/go
```

Requires Go 1.23+.

## Quick start

```go
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	raven "github.com/refleeexzz/RAVEN/sdk/go"
)

func main() {
	ctx := context.Background()

	c := raven.New(raven.WithBaseURL("http://localhost:8080"))
	if _, err := c.Login(ctx, "me@example.com", "correct horse battery"); err != nil {
		log.Fatal(err)
	}
	// The token pair now lives on the client and refreshes itself
	// when the access token nears expiry.

	payload, _ := raven.NewJobPayload(map[string]any{
		"url":   "https://me.example/hook",
		"event": "deploy.done",
	})
	job, err := c.CreateJob(ctx, raven.CreateJobRequest{
		Type:    "webhook",
		Payload: payload,
	})
	if err != nil {
		log.Fatal(err)
	}

	final, err := c.WatchJob(ctx, job.ID, 2*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(final.ID, final.Status)
}
```

See [examples/basic](examples/basic/main.go) for a longer tour.

## Authentication — two flavors

| Flavor | How | Notes |
|--------|-----|-------|
| Email + password (JWT) | `c.Login(ctx, email, password)` | Access tokens live 15 min; the client rotates the pair automatically before they expire, and once more on a surprise `401`. |
| API key | `raven.New(raven.WithAPIKey("rav_live_..."))` | Static machine credential; nothing to refresh. Create keys with `c.CreateAPIKey`. |

Persist the session across restarts with `c.Tokens()` and restore it with
`raven.WithTokenPair(pair)` — the stored refresh token rotates on every
refresh, so always persist the latest pair.

## Capabilities

- **Auth**: `Register`, `Login`, `Refresh`, `Logout`
- **Jobs**: `CreateJob`, `ListJobs`, `GetJob`, `CancelJob`, `RequeueJob`,
  `ReplayJob`, `JobDeliveries`, `WatchJob` (blocking) / `JobWatch` (channel)
- **Cron schedules**: `CreateCron`, `ListCrons`, `DeleteCron`
- **API keys**: `CreateAPIKey`, `ListAPIKeys`, `RevokeAPIKey`
- **Ops**: `ListWorkers`, `HealthServices`, `ListAuditEvents` (admin)

Every method takes a `context.Context` first. Timeouts, cancellation and
tracing flow through it.

## Idempotency

Every mutating call automatically sends an `Idempotency-Key` header (random
128-bit hex). When you retry a create after a network timeout, pin the key
so the retry returns the *same* job instead of a duplicate:

```go
job, err := c.CreateJob(ctx, req, raven.WithIdempotencyKey("deploy-2026-01-01-001"))
```

## Errors

API failures come back as `*raven.Error` with the stable machine `Code`,
the client-safe `Message`, the `RequestID` from the gateway logs and the
HTTP `StatusCode`:

```go
job, err := c.GetJob(ctx, "job_nope")
if raven.IsCode(err, "job_not_found") {
	// handle a missing job
}
var re *raven.Error
if errors.As(err, &re) {
	log.Printf("failed: %s (%s), quote request %s", re.Code, re.Message, re.RequestID)
}
```

## Configuration

| Option | Default | Purpose |
|--------|---------|---------|
| `WithBaseURL` | `http://localhost:8080` | Point at another gateway. |
| `WithTimeout` | `10s` | Per-request timeout. Context deadlines still win. |
| `WithHTTPClient` | — | Bring your own `*http.Client` (proxies, tracing, tests). |
| `WithAPIKey` | — | `Authorization: ApiKey rav_live_...` on every call. |
| `WithTokenPair` | — | Restore a persisted session. |
| `WithIdempotencyKeyFunc` | crypto/rand | Custom key generator (tests). |

## Not included (yet)

WebSocket streaming (`/ws`) is not part of this SDK — `WatchJob` polls the
REST API instead. A streaming helper may land in a future version.

## Development

```
go test ./...
```

The tests run against an `httptest` mock gateway; no live stack needed.
