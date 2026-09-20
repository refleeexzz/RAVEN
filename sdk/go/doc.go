// Package raven is the official Go client for the RAVEN distributed jobs
// platform. It speaks to the public REST API exposed by the gateway
// (default http://localhost:8080) and covers auth, jobs, cron schedules,
// API keys, workers and aggregated health.
//
// The client has no dependencies beyond the standard library. Every method
// takes a context.Context as its first argument and honors cancellation.
//
// Quick start:
//
//	c := raven.New(
//		raven.WithBaseURL("http://localhost:8080"),
//	)
//	tokens, err := c.Login(ctx, "me@example.com", "correct horse battery")
//	// c now carries the token pair and refreshes it automatically.
//	job, err := c.CreateJob(ctx, raven.CreateJobRequest{
//		Type:    "webhook",
//		Payload: map[string]any{"url": "https://me.example/hook"},
//	})
//
// For machine credentials use an API key instead:
//
//	c := raven.New(raven.WithAPIKey("rav_live_..."))
//
// Errors returned by the API are surfaced as *raven.Error, which exposes
// the stable machine Code, the client-safe Message and the RequestID that
// matches the gateway logs.
package raven
