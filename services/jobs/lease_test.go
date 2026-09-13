package jobs

import (
	"strings"
	"testing"
	"time"
)

// The generation in the broker message is the fencing token the worker
// presents back to Postgres. Pin the wire behavior down: new messages carry
// it, pre-lease messages decode with 0 (wildcard at the claim fence).

func TestParseJobMessage(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantID  string
		wantGen int
		wantErr bool
	}{
		{name: "id and generation", raw: `{"id":"job_a","execution_generation":7}`,
			wantID: "job_a", wantGen: 7},
		{name: "legacy message without generation", raw: `{"id":"job_b","type":"webhook"}`,
			wantID: "job_b", wantGen: 0},
		{name: "explicit zero stays zero", raw: `{"id":"job_c","execution_generation":0}`,
			wantID: "job_c", wantGen: 0},
		{name: "garbage", raw: `not json`, wantErr: true},
		{name: "missing id", raw: `{"execution_generation":3}`, wantErr: true},
		{name: "empty id", raw: `{"id":"","execution_generation":3}`, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			id, gen, err := ParseJobMessage([]byte(c.raw))
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got (id %q, gen %d)", id, gen)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if id != c.wantID || gen != c.wantGen {
				t.Errorf("got (id %q, gen %d), want (%q, %d)", id, gen, c.wantID, c.wantGen)
			}
		})
	}
}

func TestJobMessageCarriesGeneration(t *testing.T) {
	j := &Job{
		ID: "job_g", Type: "webhook", Payload: `{"url":"http://x"}`,
		Status: StatusQueued, MaxAttempts: 4, ExecutionGeneration: 7,
	}
	raw, err := j.MarshalMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"execution_generation":7`) {
		t.Errorf("generation missing from the wire format: %s", raw)
	}
	id, gen, err := ParseJobMessage(raw)
	if err != nil {
		t.Fatal(err)
	}
	if id != "job_g" || gen != 7 {
		t.Errorf("round trip: got (id %q, gen %d), want (job_g, 7)", id, gen)
	}
}

// sweeperOutcome is the metric label mapping: recovered jobs go back to
// RETRYING unless their attempts are gone, in which case they go DEAD.
func TestSweeperOutcome(t *testing.T) {
	if got := sweeperOutcome(StatusDead); got != "dead" {
		t.Errorf("sweeperOutcome(DEAD) = %q, want dead", got)
	}
	for _, s := range []Status{StatusRetrying, StatusProcessing, StatusQueued} {
		if got := sweeperOutcome(s); got != "retry" {
			t.Errorf("sweeperOutcome(%s) = %q, want retry", s, got)
		}
	}
}

// The fresh lease a recovered job gets must outlive the default sweep
// interval, or the next pass would re-sweep a job whose republished message
// is still in flight.
func TestSweeperRecoverLeaseCoversInterval(t *testing.T) {
	if sweeperRecoverLease < 2*15*time.Second {
		t.Errorf("recover lease %v should be >= 2x the default 15s sweep interval",
			sweeperRecoverLease)
	}
}
