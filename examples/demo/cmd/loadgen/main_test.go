package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
	"github.com/Jenil133/Shadowtap/examples/demo/traffic"
)

func TestParseFlags(t *testing.T) {
	defaults := config{
		target: "http://localhost:10000", rps: 20, requests: 200, concurrency: 8,
		seed: 1, people: 1000, timeout: 10 * time.Second,
	}
	tests := []struct {
		name    string
		args    []string
		want    config
		wantErr error
	}{
		{name: "defaults", want: defaults},
		{
			name: "duration run",
			args: []string{"--target", "http://127.0.0.1:8080", "--rps", "0", "--requests", "0", "--duration", "1m",
				"--concurrency", "2", "--seed", "-7", "--people", "5", "--timeout", "2s", "--pii-out", "pii.txt"},
			want: config{target: "http://127.0.0.1:8080", duration: time.Minute, concurrency: 2, seed: -7,
				people: 5, timeout: 2 * time.Second, piiOut: "pii.txt"},
		},
		{name: "no limit", args: []string{"--requests", "0"}, wantErr: errUsage},
		{name: "negative rps", args: []string{"--rps", "-1"}, wantErr: errUsage},
		{name: "infinite rps", args: []string{"--rps", "+Inf"}, wantErr: errUsage},
		{name: "zero concurrency", args: []string{"--concurrency", "0"}, wantErr: errUsage},
		{name: "zero timeout", args: []string{"--timeout", "0s"}, wantErr: errUsage},
		{name: "target without scheme", args: []string{"--target", "localhost:10000"}, wantErr: errUsage},
		{name: "unknown flag", args: []string{"--qps", "5"}, wantErr: errUsage},
		{name: "stray argument", args: []string{"go"}, wantErr: errUsage},
		{name: "help", args: []string{"-h"}, wantErr: flag.ErrHelp},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseFlags(tc.args, io.Discard)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if err == nil && got != tc.want {
				t.Errorf("config = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestPercentile(t *testing.T) {
	ms := func(ns ...int) []time.Duration {
		out := make([]time.Duration, len(ns))
		for i, n := range ns {
			out[i] = time.Duration(n) * time.Millisecond
		}
		return out
	}
	tests := []struct {
		sorted []time.Duration
		p      float64
		want   time.Duration
	}{
		{nil, 50, 0},
		{ms(7), 50, 7 * time.Millisecond},
		{ms(7), 95, 7 * time.Millisecond},
		{ms(1, 2, 3, 4), 50, 2 * time.Millisecond},
		{ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 95, 10 * time.Millisecond},
		{ms(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 90, 9 * time.Millisecond},
		{ms(1, 2, 3), 0, 1 * time.Millisecond},
	}
	for _, tc := range tests {
		if got := percentile(tc.sorted, tc.p); got != tc.want {
			t.Errorf("percentile(%v, %v) = %v, want %v", tc.sorted, tc.p, got, tc.want)
		}
	}
}

func TestDescribeOmitsURL(t *testing.T) {
	err := &url.Error{Op: "Get", URL: "http://demo.test/api/users/lookup/rosa.novak3@example.com", Err: errors.New("connection refused")}
	if got := describe(err); got != "Get: connection refused" {
		t.Errorf("describe = %q", got)
	}
}

func newDemoServer(t *testing.T) *httptest.Server {
	t.Helper()
	a, err := api.New(api.Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler())
	t.Cleanup(srv.Close)
	return srv
}

func TestRunAgainstAPI(t *testing.T) {
	srv := newDemoServer(t)
	piiOut := filepath.Join(t.TempDir(), "pii.txt")
	cfg := config{target: srv.URL, requests: 300, concurrency: 4, seed: 9, people: 200, timeout: 5 * time.Second, piiOut: piiOut}
	sum, err := run(context.Background(), cfg)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if sum.requests != 300 || sum.transportErrors != 0 {
		t.Errorf("requests %d, transport errors %d", sum.requests, sum.transportErrors)
	}
	total := 0
	for code, n := range sum.statuses {
		total += n
		if code >= 500 || code == http.StatusBadRequest {
			t.Errorf("%d responses with status %d", n, code)
		}
	}
	if total != 300 {
		t.Errorf("status counts add up to %d", total)
	}
	if sum.unexpected > 15 {
		t.Errorf("%d of 300 statuses differ from the model", sum.unexpected)
	}
	if sum.p50 <= 0 || sum.p95 < sum.p50 || sum.max < sum.p95 {
		t.Errorf("latencies p50 %v p95 %v max %v", sum.p50, sum.p95, sum.max)
	}

	data, err := os.ReadFile(piiOut)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != sum.ledgerValues || len(lines) < 100 {
		t.Errorf("ledger has %d lines, summary says %d", len(lines), sum.ledgerValues)
	}
	if first := traffic.People(9, 1)[0]; !strings.Contains(string(data), "\n"+first.Email+"\n") {
		t.Errorf("ledger lacks the first person's email %s", first.Email)
	}

	var out bytes.Buffer
	sum.write(&out)
	for _, want := range []string{"requests:          300", "status codes:      ", "transport errors:  0 (0.0%)", "latency:", "pii ledger:        "} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("summary lacks %q:\n%s", want, out.String())
		}
	}
}

func TestRunPacing(t *testing.T) {
	srv := newDemoServer(t)
	start := time.Now()
	sum, err := run(context.Background(), config{target: srv.URL, rps: 100, requests: 21, concurrency: 2, seed: 1, people: 50, timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	// 21 requests at 100/s with a burst of one take at least 20 intervals.
	if elapsed := time.Since(start); elapsed < 180*time.Millisecond {
		t.Errorf("21 requests at 100/s took only %v", elapsed)
	}
	if sum.requests != 21 {
		t.Errorf("sent %d requests, want 21", sum.requests)
	}
}

func TestRunDuration(t *testing.T) {
	srv := newDemoServer(t)
	sum, err := run(context.Background(), config{target: srv.URL, rps: 50, duration: 300 * time.Millisecond, concurrency: 2, seed: 1, people: 50, timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if sum.requests < 5 || sum.requests > 20 {
		t.Errorf("sent %d requests in 300ms at 50/s", sum.requests)
	}
}

func TestRunStopsWhenCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a, err := api.New(api.Options{})
	if err != nil {
		t.Fatal(err)
	}
	var served atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if served.Add(1) == 10 {
			cancel()
		}
		a.Handler().ServeHTTP(w, r)
	}))
	defer srv.Close()

	sum, err := run(ctx, config{target: srv.URL, requests: 1_000_000, concurrency: 2, seed: 1, people: 50, timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if sum.requests < 10 || sum.requests > 100 || sum.transportErrors != 0 {
		t.Errorf("after cancellation: %d requests, %d transport errors", sum.requests, sum.transportErrors)
	}
}

func TestRealMain(t *testing.T) {
	srv := newDemoServer(t)
	var stdout, stderr bytes.Buffer
	if code := realMain([]string{"--target", srv.URL, "--requests", "40", "--rps", "0"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d; stderr: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "requests:          40") {
		t.Errorf("summary missing:\n%s", stdout.String())
	}

	// A target that refuses connections fails every request in transport.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dead := "http://" + ln.Addr().String()
	ln.Close()
	stdout.Reset()
	stderr.Reset()
	if code := realMain([]string{"--target", dead, "--requests", "5", "--rps", "0", "--timeout", "2s"}, &stdout, &stderr); code != 1 {
		t.Errorf("exit %d against a dead target, want 1", code)
	}
	if !strings.Contains(stdout.String(), "transport errors:  5 (100.0%)") || !strings.Contains(stderr.String(), "failed in transport") {
		t.Errorf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String(), "@") {
		t.Errorf("summary leaks request URLs:\n%s", stdout.String())
	}

	if code := realMain([]string{"--requests", "-1"}, &stdout, &stderr); code != 2 {
		t.Errorf("exit %d for a usage error, want 2", code)
	}
}
