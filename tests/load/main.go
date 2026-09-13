// loadgen is a small load generator for the RAVEN gateway. It logs in once,
// then hammers the API with a mix of job creations and job listings from N
// concurrent clients, printing live stats. Used to demo HPA scale-out on k8s.
//
//	go run ./tests/load -url http://localhost:8080 -c 120 -d 150s
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"sync/atomic"
	"time"
)

func main() {
	var (
		baseURL     = flag.String("url", "http://localhost:8080", "gateway base URL")
		email       = flag.String("email", "e2e@raven.dev", "login email")
		password    = flag.String("password", "supersecret123", "login password")
		jobType     = flag.String("type", "send_email", "job type: send_email (io-bound) or resize_image (cpu-bound)")
		concurrency = flag.Int("c", 50, "concurrent clients")
		duration    = flag.Duration("d", 60*time.Second, "test duration")
	)
	flag.Parse()

	client := &http.Client{
		Timeout: 30 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        *concurrency * 2,
			MaxIdleConnsPerHost: *concurrency * 2,
		},
	}

	token, err := login(client, *baseURL, *email, *password)
	if err != nil {
		fmt.Println("login failed:", err)
		os.Exit(1)
	}
	fmt.Printf("logged in as %s — load: %d clients for %s\n", *email, *concurrency, *duration)

	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	var created, listed, failed atomic.Int64
	lat := newLatency()

	for i := 0; i < *concurrency; i++ {
		go func(n int) {
			seq := 0
			for ctx.Err() == nil {
				seq++
				start := time.Now()
				var err error
				if seq%5 == 0 { // 20% reads, 80% job creation
					err = listJobs(client, *baseURL, token)
					if err == nil {
						listed.Add(1)
					}
				} else {
					err = createJob(client, *baseURL, token, *jobType, n, seq)
					if err == nil {
						created.Add(1)
					}
				}
				if err != nil {
					failed.Add(1)
				} else {
					lat.add(time.Since(start))
				}
			}
		}(i)
	}

	ticker := time.NewTicker(5 * time.Second)
	done := time.After(*duration)
	start := time.Now()
loop:
	for {
		select {
		case <-ticker.C:
			el := time.Since(start).Round(time.Second)
			fmt.Printf("[%6s] created=%d listed=%d failed=%d (%.0f req/s)\n",
				el, created.Load(), listed.Load(), failed.Load(),
				float64(created.Load()+listed.Load())/el.Seconds())
		case <-done:
			ticker.Stop()
			break loop
		}
	}

	total := created.Load() + listed.Load()
	fmt.Printf("\n== done ==\ntotal=%d created=%d listed=%d failed=%d\n",
		total+failed.Load(), created.Load(), listed.Load(), failed.Load())
	fmt.Printf("latency p50=%s p95=%s p99=%s\n", lat.percentile(0.50), lat.percentile(0.95), lat.percentile(0.99))
}

func login(c *http.Client, base, email, pass string) (string, error) {
	body, _ := json.Marshal(map[string]string{"email": email, "password": pass})
	resp, err := c.Post(base+"/api/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login status %d", resp.StatusCode)
	}
	var out struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.AccessToken, nil
}

func createJob(c *http.Client, base, token, jobType string, client, seq int) error {
	payload := fmt.Sprintf(`{"type":%q,"payload":{"to":"load-%d-%d@raven.dev","subject":"load","image":"%d bytes of fake image data for resize jobs"},"priority":5}`, jobType, client, seq, seq%97)
	req, _ := http.NewRequest(http.MethodPost, base+"/api/jobs", bytes.NewReader([]byte(payload)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Idempotency-Key", fmt.Sprintf("load-%d-%d", client, seq))
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

func listJobs(c *http.Client, base, token string) error {
	req, _ := http.NewRequest(http.MethodGet, base+"/api/jobs?page=1&page_size=20", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := c.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode >= 400 {
		return fmt.Errorf("status %d", resp.StatusCode)
	}
	return nil
}

// latency is a simple concurrent-safe latency collector (mutex-free via
// atomic append to chunked slices would be nicer; with ~100k samples a
// mutex is fine and clearer).
type latency struct {
	ch chan time.Duration
	mu []time.Duration
}

func newLatency() *latency {
	l := &latency{ch: make(chan time.Duration, 1<<16)}
	l.mu = make([]time.Duration, 0, 1<<17)
	go func() {
		for d := range l.ch {
			l.mu = append(l.mu, d)
		}
	}()
	return l
}

func (l *latency) add(d time.Duration) { l.ch <- d }

func (l *latency) percentile(p float64) time.Duration {
	s := append([]time.Duration(nil), l.mu...)
	if len(s) == 0 {
		return 0
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	return s[int(float64(len(s)-1)*p)].Round(time.Millisecond)
}
