//go:build chaos

package chaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const namespace = "raven"

// chaosKit bundles everything a scenario needs: kubectl, docker, the HTTP
// API client and a per-run id for unique idempotency keys.
type chaosKit struct {
	t       *testing.T
	kubectl string
	docker  string
	api     *apiClient
	runID   string
}

// newKit gates the test (double opt-in), checks the toolchain and the
// cluster, and logs in to the gateway once. It skips cleanly when any
// precondition is missing.
func newKit(t *testing.T) *chaosKit {
	t.Helper()
	if os.Getenv("RAVEN_CHAOS") != "1" {
		t.Skip("chaos tests kill things; set RAVEN_CHAOS=1 to opt in")
	}
	home, _ := os.UserHomeDir()
	dockerBinDir := filepath.Join(home, `AppData\Local\Programs\DockerDesktop\resources\bin`)
	k := &chaosKit{
		t:       t,
		kubectl: findBin("kubectl", filepath.Join(dockerBinDir, "kubectl.exe")),
		docker:  findBin("docker", filepath.Join(dockerBinDir, "docker.exe")),
		runID:   strconv.FormatInt(time.Now().Unix(), 36),
	}
	if k.kubectl == "" {
		t.Skip("kubectl not found (looked in PATH and Docker Desktop resources/bin)")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	out, err := k.kubectlCombined(ctx, "get", "namespace", namespace)
	if err != nil {
		t.Skipf("no reachable cluster with namespace %q: %v (%s)", namespace, err, truncate(out))
	}
	k.api = newAPIClient(envOr("RAVEN_GATEWAY", "http://localhost:8080"))
	lctx, lcancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer lcancel()
	if err := k.api.login(lctx); err != nil {
		t.Skipf("gateway not reachable or demo login failed: %v", err)
	}
	return k
}

func findBin(name string, fallbacks ...string) string {
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	for _, f := range fallbacks {
		if _, err := os.Stat(f); err == nil {
			return f
		}
	}
	return ""
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func truncate(s string) string {
	const max = 300
	if len(s) > max {
		return s[:max] + "..."
	}
	return s
}

// --- kubectl -------------------------------------------------------------

func (k *chaosKit) kubectlCombined(ctx context.Context, args ...string) (string, error) {
	full := append([]string{"-n", namespace}, args...)
	cmd := exec.CommandContext(ctx, k.kubectl, full...)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func (k *chaosKit) deployReplicas(ctx context.Context, name string) (int, error) {
	out, err := k.kubectlCombined(ctx, "get", "deployment", name, "-o", "jsonpath={.spec.replicas}")
	if err != nil {
		return 0, fmt.Errorf("get replicas for %s: %w (%s)", name, err, truncate(out))
	}
	n, err := strconv.Atoi(strings.TrimSpace(out))
	if err != nil {
		return 0, fmt.Errorf("parse replicas for %s: %q: %w", name, out, err)
	}
	return n, nil
}

func (k *chaosKit) scale(ctx context.Context, name string, n int) error {
	out, err := k.kubectlCombined(ctx, "scale", "deployment", name, fmt.Sprintf("--replicas=%d", n))
	if err != nil {
		return fmt.Errorf("scale %s to %d: %w (%s)", name, n, err, truncate(out))
	}
	return nil
}

func (k *chaosKit) rolloutWait(ctx context.Context, resource string, timeout time.Duration) error {
	out, err := k.kubectlCombined(ctx, "rollout", "status", resource, "--timeout="+timeout.String())
	if err != nil {
		return fmt.Errorf("rollout status %s: %w (%s)", resource, err, truncate(out))
	}
	return nil
}

type podListJSON struct {
	Items []struct {
		Metadata struct {
			Name              string `json:"name"`
			DeletionTimestamp string `json:"deletionTimestamp"`
		} `json:"metadata"`
		Status struct {
			Phase string `json:"phase"`
		} `json:"status"`
	} `json:"items"`
}

// runningPods returns Running, non-terminating pod names for app=<app>.
func (k *chaosKit) runningPods(ctx context.Context, app string) ([]string, error) {
	out, err := k.kubectlCombined(ctx, "get", "pods", "-l", "app="+app, "-o", "json")
	if err != nil {
		return nil, fmt.Errorf("list pods app=%s: %w (%s)", app, err, truncate(out))
	}
	var pl podListJSON
	if err := json.Unmarshal([]byte(out), &pl); err != nil {
		return nil, fmt.Errorf("parse pod list: %w", err)
	}
	var names []string
	for _, p := range pl.Items {
		if p.Metadata.DeletionTimestamp == "" && p.Status.Phase == "Running" {
			names = append(names, p.Metadata.Name)
		}
	}
	sort.Strings(names)
	return names, nil
}

// deletePod deletes a pod. force=true is a SIGKILL (--force --grace-period=0);
// force=false is a normal delete, i.e. SIGTERM + the pod's grace period.
func (k *chaosKit) deletePod(ctx context.Context, name string, force bool) error {
	args := []string{"delete", "pod", name}
	if force {
		args = append(args, "--force", "--grace-period=0")
	}
	out, err := k.kubectlCombined(ctx, args...)
	if err != nil {
		return fmt.Errorf("delete pod %s (force=%v): %w (%s)", name, force, err, truncate(out))
	}
	return nil
}

// restoreReplicas registers a cleanup that scales the deployment back to the
// recorded replica count and waits for readiness.
func (k *chaosKit) restoreReplicas(name string, replicas int) {
	k.t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
		defer cancel()
		if err := k.scale(ctx, name, replicas); err != nil {
			k.t.Logf("CLEANUP: scale %s back to %d: %v", name, replicas, err)
		}
		if err := k.rolloutWait(ctx, "deployment/"+name, 3*time.Minute); err != nil {
			k.t.Logf("CLEANUP: %s did not become ready: %v", name, err)
		}
	})
}

// --- docker --------------------------------------------------------------

// dockerRestart restarts a plain Docker container. Kept in the kit for
// compose-less cases; the k8s scenarios restart workloads by deleting pods.
func (k *chaosKit) dockerRestart(ctx context.Context, container string) error {
	if k.docker == "" {
		return errors.New("docker CLI not available")
	}
	cmd := exec.CommandContext(ctx, k.docker, "restart", container)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker restart %s: %w (%s)", container, err, truncate(string(out)))
	}
	return nil
}

// --- port-forward --------------------------------------------------------

// portForward opens `kubectl port-forward` on a free local port and waits
// until the ops endpoint answers. The returned stop func kills the process.
func (k *chaosKit) portForward(resource string, remotePort int) (string, func(), error) {
	for local := 19101; local <= 19108; local++ {
		if portInUse(local) {
			continue
		}
		ctx, cancel := context.WithCancel(context.Background())
		cmd := exec.CommandContext(ctx, k.kubectl, "-n", namespace, "port-forward", resource,
			fmt.Sprintf("%d:%d", local, remotePort))
		if err := cmd.Start(); err != nil {
			cancel()
			continue
		}
		base := fmt.Sprintf("http://127.0.0.1:%d", local)
		if waitHTTP200(base+"/health", 15*time.Second) {
			return base, func() { cancel(); _ = cmd.Wait() }, nil
		}
		cancel()
		_ = cmd.Wait()
	}
	return "", nil, errors.New("port-forward: no usable local port in 19101-19108")
}

func portInUse(port int) bool {
	c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 300*time.Millisecond)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func waitHTTP200(url string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	hc := &http.Client{Timeout: 2 * time.Second}
	for time.Now().Before(deadline) {
		resp, err := hc.Get(url)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return true
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
	return false
}

// --- broker ops API ------------------------------------------------------

type brokerTopics struct {
	Topics []struct {
		Name       string `json:"name"`
		Partitions []struct {
			ID            int   `json:"id"`
			HighWatermark int64 `json:"high_watermark"`
			Groups        map[string]struct {
				Committed int64 `json:"committed"`
				Lag       int64 `json:"lag"`
			} `json:"groups"`
		} `json:"partitions"`
	} `json:"topics"`
}

func fetchTopics(base string) (*brokerTopics, error) {
	hc := &http.Client{Timeout: 5 * time.Second}
	resp, err := hc.Get(base + "/topics")
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	var bt brokerTopics
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET /topics: status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&bt); err != nil {
		return nil, err
	}
	return &bt, nil
}

// highWatermarks returns partition id -> high watermark for one topic.
func highWatermarks(bt *brokerTopics, topic string) map[int]int64 {
	out := map[int]int64{}
	for _, t := range bt.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			out[p.ID] = p.HighWatermark
		}
	}
	return out
}

// committedOffsets returns partition id -> committed offset for one
// topic+consumer group.
func committedOffsets(bt *brokerTopics, topic, group string) map[int]int64 {
	out := map[int]int64{}
	for _, t := range bt.Topics {
		if t.Name != topic {
			continue
		}
		for _, p := range t.Partitions {
			if g, ok := p.Groups[group]; ok {
				out[p.ID] = g.Committed
			}
		}
	}
	return out
}

// sumOffsetDelta counts how many offsets went backwards between two
// partition->offset snapshots (i.e. messages/offsets the restart lost).
func sumOffsetDelta(pre, post map[int]int64) int64 {
	var lost int64
	for part, preHW := range pre {
		if postHW, ok := post[part]; ok && postHW < preHW {
			lost += preHW - postHW
		}
	}
	return lost
}
