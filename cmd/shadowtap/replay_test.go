package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"sigs.k8s.io/yaml"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/replay/report"
)

const replayKey = "replay-cli-test-key-0123456789"

func replayEnv(k string) string {
	if k == "SHADOWTAP_SCRUB_KEY" {
		return replayKey
	}
	return ""
}

func cliEnv(t *testing.T, env func(string) string, args ...string) (string, string, int) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	code := run(ctx, args, env, &out, &errOut)
	return out.String(), errOut.String(), code
}

func api(version string) *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if version == "v2" {
			fmt.Fprint(w, `{"ok":true,"name":"v2"}`)
			return
		}
		fmt.Fprint(w, `{"ok":true}`)
	}))
}

func replayCaptures(n int) []*capture.Exchange {
	var xs []*capture.Exchange
	for i := range n {
		x := exchange(i, "demo-api", "GET", fmt.Sprintf("/api/items/%d", i), 200)
		x.Timing.StartedAt = start.Add(time.Duration(i) * 5 * time.Millisecond)
		x.Response.Headers = capture.Headers{"content-type": {"application/json"}}
		x.Response.Body.SetData([]byte(`{"ok":true}`))
		xs = append(xs, x)
	}
	post := exchange(n, "demo-api", "POST", "/api/items", 201)
	post.Timing.StartedAt = start.Add(time.Duration(n) * 5 * time.Millisecond)
	return append(xs, post)
}

func TestReplayFromKafka(t *testing.T) {
	brokers := cluster(t)
	publish(t, brokers, replayCaptures(5)...)
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, diff.DefaultTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	v1, v2 := api("v1"), api("v2")
	t.Cleanup(v1.Close)
	t.Cleanup(v2.Close)
	dir := t.TempDir()
	base := []string{"replay", "--brokers", strings.Join(brokers, ","), "--speed", "max", "--run-id", "run-test"}

	// v1 against v1: everything matches and the gate passes.
	out, errOut, code := cliEnv(t, replayEnv, append(base, "--target", v1.URL)...)
	if code != exitOK || !strings.Contains(out, "GATE PASSED") || !strings.Contains(out, "match=5") || !strings.Contains(out, "mutating_method=1") {
		t.Fatalf("v1 vs v1: exit %d\n%s\n%s", code, out, errOut)
	}

	// v2 adds a field: every capture mismatches, the gate fails, and the
	// outputs are written.
	reportJSON, reportHTML, diffsOut := filepath.Join(dir, "r.json"), filepath.Join(dir, "r.html"), filepath.Join(dir, "d.ndjson")
	out, errOut, code = cliEnv(t, replayEnv, append(base, "--target", v2.URL, "--format", "json",
		"--report-json", reportJSON, "--report-html", reportHTML, "--diffs-out", diffsOut)...)
	if code != exitGateFailed {
		t.Fatalf("v2: exit %d\n%s\n%s", code, out, errOut)
	}
	var s report.Summary
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatal(err)
	}
	if s.Classes[diff.ClassBodyMismatch] != 5 || s.TopDifferences[0].Pointer != "/name" {
		t.Errorf("summary: %+v", s)
	}
	for _, f := range []string{reportJSON, reportHTML, diffsOut} {
		info, err := os.Stat(f)
		if err != nil || info.Size() == 0 || info.Mode().Perm() != 0o600 {
			t.Errorf("%s: %v %v", f, err, info)
		}
	}
	lines, _ := os.ReadFile(diffsOut)
	if n := strings.Count(string(lines), "\n"); n != 6 {
		t.Errorf("%d result lines, want 6", n)
	}

	// Results can be published to the diffs topic.
	diffsBrokers := strings.Join(c.ListenAddrs(), ",")
	_, errOut, code = cliEnv(t, replayEnv, append(base, "--target", v2.URL, "--max-mismatch-rate", "-1")...)
	if code != exitOK {
		t.Fatalf("disabled gate: exit %d %s", code, errOut)
	}
	pub := []string{"replay", "--brokers", strings.Join(brokers, ","), "--speed", "max", "--target", v2.URL,
		"--max-mismatch-rate", "1", "--publish-diffs", "--file", diffsOutCaptures(t, brokers, dir)}
	// --file replays offline; publishing still needs the diffs brokers.
	pub[2] = diffsBrokers
	if _, errOut, code = cliEnv(t, replayEnv, pub...); code != exitOK {
		t.Fatalf("publish: exit %d %s", code, errOut)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics(diff.DefaultTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var got []*kgo.Record
	for len(got) < 6 {
		f := cl.PollFetches(ctx)
		if err := f.Err0(); err != nil {
			t.Fatalf("after %d records: %v", len(got), err)
		}
		got = append(got, f.Records()...)
	}
	// Records are keyed by capture id and carry routing headers, so the
	// indexer can place them without decoding.
	for _, rec := range got {
		var r diff.Result
		if err := json.Unmarshal(rec.Value, &r); err != nil {
			t.Fatalf("diff record %q: %v", rec.Key, err)
		}
		headers := map[string]string{}
		for _, h := range rec.Headers {
			headers[h.Key] = string(h.Value)
		}
		if string(rec.Key) != r.CaptureID || r.SchemaVersion != diff.SchemaVersion || r.RunID == "" || r.Route == "" ||
			headers["schema-version"] != "1" || headers["class"] != r.Class || headers["run-id"] != r.RunID || headers["content-type"] != "application/json" {
			t.Errorf("record key %q headers %v for %+v", rec.Key, headers, r)
		}
	}
}

// diffsOutCaptures writes the captures topic to an NDJSON file with tail.
func diffsOutCaptures(t *testing.T, brokers []string, dir string) string {
	t.Helper()
	out, errOut, code := cli(t, "tail", "--brokers", strings.Join(brokers, ","), "--from", "earliest", "--timeout", "1s", "--format", "json")
	if code != exitOK {
		t.Fatalf("tail: %d %s", code, errOut)
	}
	path := filepath.Join(dir, "window.ndjson")
	if err := os.WriteFile(path, []byte(out), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestReplayDryRunAndWindow(t *testing.T) {
	brokers := cluster(t)
	publish(t, brokers, replayCaptures(10)...)
	v1 := api("v1")
	t.Cleanup(v1.Close)
	from := start.Add(10 * time.Millisecond).Format(time.RFC3339Nano)
	to := start.Add(30 * time.Millisecond).Format(time.RFC3339Nano)
	out, errOut, code := cliEnv(t, replayEnv, "replay", "--brokers", strings.Join(brokers, ","), "--target", v1.URL,
		"--from", from, "--to", to, "--dry-run", "--min-replayed", "0", "--format", "json")
	if code != exitOK {
		t.Fatalf("exit %d %s", code, errOut)
	}
	var s report.Summary
	if err := json.Unmarshal([]byte(out), &s); err != nil {
		t.Fatal(err)
	}
	if s.Captures != 4 || s.Skipped["dry_run"] != 4 {
		t.Errorf("window/dry run: %+v", s)
	}
}

// The Kubernetes replay Job's arguments must be accepted by this CLI: only
// the broker connection may fail.
func TestReplayJobArgs(t *testing.T) {
	read := func(path string, v any) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", path))
		if err != nil {
			t.Fatal(err)
		}
		docs := strings.Split(string(data), "\n---\n")
		if err := yaml.Unmarshal([]byte(docs[0]), v); err != nil {
			t.Fatal(err)
		}
	}
	var job struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Args []string `json:"args"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	read("deploy/k8s/replay/replay-job.yaml", &job)
	var cm struct {
		Data map[string]string `json:"data"`
	}
	read("deploy/k8s/base/configmap.yaml", &cm)
	tapYAML := filepath.Join(t.TempDir(), "tap.yaml")
	if err := os.WriteFile(tapYAML, []byte(cm.Data["tap.yaml"]), 0o600); err != nil {
		t.Fatal(err)
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	if len(args) == 0 || args[0] != "replay" {
		t.Fatalf("job args %v", args)
	}
	// Later flags win: point the Job at a closed port and the local copy of
	// the tap config.
	args = append(args, "--brokers=127.0.0.1:1", "--tap-config="+tapYAML, "--read-deadline=300ms", "--report-json=", "--report-html=")
	_, errOut, code := cliEnv(t, replayEnv, args...)
	if code != exitError {
		t.Errorf("exit %d, want %d (Kafka unreachable): %s", code, exitError, errOut)
	}
}

func TestReplayUsage(t *testing.T) {
	tests := []struct {
		env  func(string) string
		args []string
	}{
		{replayEnv, []string{"replay"}},
		{replayEnv, []string{"replay", "--target", "https://api.example.com"}},
		{replayEnv, []string{"replay", "--target", "http://orders-prod:8080"}},
		{replayEnv, []string{"replay", "--target", "http://localhost:1", "--method", "POST"}},
		{replayEnv, []string{"replay", "--target", "http://localhost:1", "--speed", "fast"}},
		{replayEnv, []string{"replay", "--target", "http://localhost:1", "--from", "yesterday"}},
		{replayEnv, []string{"replay", "--target", "http://localhost:1", "--set-header", "novalue"}},
		{replayEnv, []string{"replay", "--target", "http://localhost:1", "--path-regex", "("}},
		{func(string) string { return "" }, []string{"replay", "--target", "http://localhost:1"}},
	}
	for _, tt := range tests {
		if _, errOut, code := cliEnv(t, tt.env, tt.args...); code != exitUsage {
			t.Errorf("%v: exit %d, want %d (%s)", tt.args, code, exitUsage, errOut)
		}
	}
	for s, want := range map[string]float64{"1": 1, "4x": 4, "max": 0, "0.5": 0.5} {
		if got, err := parseSpeed(s); err != nil || got != want {
			t.Errorf("parseSpeed(%q) = %v, %v", s, got, err)
		}
	}
}
