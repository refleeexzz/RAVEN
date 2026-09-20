// Command basic is a small tour of the RAVEN Go SDK against a live local
// stack (docker compose up). It registers a throwaway user, submits a
// webhook job, watches it to a terminal state and lists workers.
//
// Run from the sdk/go directory:
//
//	go run ./examples/basic -email demo@example.com -password demo12345
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	raven "github.com/refleeexzz/RAVEN/sdk/go"
)

func main() {
	baseURL := flag.String("base", "http://localhost:8080", "gateway base URL")
	email := flag.String("email", "demo@example.com", "account email")
	password := flag.String("password", "demo12345", "account password (8+ chars)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	c := raven.New(raven.WithBaseURL(*baseURL))

	// Register is idempotent-friendly for demos: if the email is taken we
	// just log in with the same credentials.
	if _, err := c.Register(ctx, raven.RegisterRequest{
		Email: *email, Password: *password, DisplayName: "SDK Demo",
	}); err != nil && !raven.IsCode(err, "email_taken") {
		log.Printf("register: %v (continuing to login)", err)
	}
	if _, err := c.Login(ctx, *email, *password); err != nil {
		log.Fatalf("login: %v", err)
	}
	fmt.Println("logged in")

	payload, err := raven.NewJobPayload(map[string]any{
		"url":   "https://example.com/hook",
		"event": "sdk.demo",
	})
	if err != nil {
		log.Fatalf("payload: %v", err)
	}
	job, err := c.CreateJob(ctx, raven.CreateJobRequest{
		Type: "webhook", Payload: payload,
	})
	if err != nil {
		log.Fatalf("create job: %v", err)
	}
	fmt.Printf("job %s created, status %s\n", job.ID, job.Status)

	final, err := c.WatchJob(ctx, job.ID, 2*time.Second)
	if err != nil {
		log.Fatalf("watch: %v", err)
	}
	fmt.Printf("job %s finished as %s\n", final.ID, final.Status)

	workers, err := c.ListWorkers(ctx)
	if err != nil {
		log.Fatalf("workers: %v", err)
	}
	fmt.Printf("%d live worker(s)\n", len(workers))

	health, err := c.HealthServices(ctx)
	if err != nil {
		log.Fatalf("health: %v", err)
	}
	for _, svc := range health.Services {
		fmt.Printf("  %-12s %-9s %s\n", svc.Name, svc.Status, svc.Detail)
	}

	if err := c.Logout(ctx); err != nil {
		log.Fatalf("logout: %v", err)
	}
	fmt.Println("logged out")
}
