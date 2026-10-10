package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/index"
	"github.com/Jenil133/Shadowtap/internal/index/indextest"
)

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

// syncBuffer is a bytes.Buffer safe for the logger's goroutines.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func httpGet(url string) (int, string, error) {
	resp, err := http.Get(url)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b), err
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func testExchange() *capture.Exchange {
	started := time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            "0199b3f0-1234-7abc-8def-0123456789ab",
		Service:       "demo-api",
		TraceID:       "4bf92f3577b34da6a3ce929d0e0e4736",
		Source:        capture.Source{Tap: "tap-0", Mode: capture.ModeObserve},
		Sampling:      capture.Sampling{Rate: 1, Rule: "default"},
		Timing:        capture.Timing{StartedAt: started, DurationMicros: 1000},
		Request:       capture.Request{Method: "GET", Path: "/api/users/42"},
		Response:      capture.Response{Status: 200},
	}
	return x
}

func TestServeIndexesAndShutsDown(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, capture.DefaultTopic, diff.DefaultTopic, index.DefaultSpansTopic, index.DefaultLogsTopic, index.DefaultDLQTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	es := indextest.NewServer()
	defer es.Close()

	cfg := index.DefaultConfig()
	cfg.Kafka.Brokers = cluster.ListenAddrs()
	cfg.Elasticsearch.URLs = []string{es.URL()}
	cfg.Batch.MaxIntervalMS = 50
	cfg.Retry = index.RetryConfig{MaxAttempts: 2, InitialBackoffMS: 10, MaxBackoffMS: 50}
	cfg.ShutdownTimeoutMS = 10_000
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	var logs syncBuffer
	a, err := newApp(cfg, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	httpLn, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	served := make(chan error, 1)
	go func() { served <- a.serve(ctx, httpLn) }()

	base := "http://" + httpLn.Addr().String()
	if code, body, err := httpGet(base + "/healthz"); err != nil || code != http.StatusOK || body != "ok\n" {
		t.Errorf("/healthz = %d %q %v", code, body, err)
	}
	waitFor(t, 20*time.Second, "readiness", func() bool {
		code, _, _ := httpGet(base + "/readyz")
		return code == http.StatusOK
	})

	x := testExchange()
	value, err := capture.Encode(x)
	if err != nil {
		t.Fatal(err)
	}
	producer, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	rec := &kgo.Record{Topic: capture.DefaultTopic, Key: x.Key(), Value: value, Timestamp: x.Timing.StartedAt}
	for _, h := range x.RecordHeaders() {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	if err := producer.ProduceSync(ctx, rec).FirstErr(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 20*time.Second, "the capture to be indexed", func() bool {
		_, ok := es.Doc(index.DefaultCapturesIndex, x.ID)
		return ok
	})

	_, metrics, err := httpGet(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"shadowtap_build_info", "shadowtap_indexer_records_consumed_total", "shadowtap_indexer_documents_total", "shadowtap_indexer_bulk_seconds", "go_goroutines"} {
		if !strings.Contains(metrics, name) {
			t.Errorf("metrics missing %s", name)
		}
	}

	cancel()
	select {
	case err := <-served:
		if err != nil {
			t.Fatalf("serve: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
	if _, _, err := httpGet(base + "/healthz"); err == nil {
		t.Error("admin server still answering after shutdown")
	}
	for _, want := range []string{"shadowtap-indexer started", "elasticsearch bootstrapped", "shutting down", "shutdown complete"} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("missing log %q:\n%s", want, logs.String())
		}
	}

	// The shutdown committed what was indexed.
	admin := kadm.NewClient(producer)
	offsets, err := admin.FetchOffsets(context.Background(), cfg.Kafka.Group)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := offsets.Lookup(capture.DefaultTopic, rec.Partition)
	if !ok || o.At != rec.Offset+1 {
		t.Errorf("committed offset for partition %d = %+v, want %d", rec.Partition, o, rec.Offset+1)
	}
}

func TestRunFlagsAndErrors(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer

	if err := run(ctx, []string{"--version"}, env(nil), &out, io.Discard); err != nil || strings.TrimSpace(out.String()) != version {
		t.Errorf("--version: %q, %v", out.String(), err)
	}

	out.Reset()
	err := run(ctx, []string{"--print-config"}, env(map[string]string{
		index.EnvESUsername: "elastic",
		index.EnvESPassword: "super-secret",
		index.EnvESAPIKey:   "also-secret",
	}), &out, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	s := out.String()
	if strings.Contains(s, "super-secret") || strings.Contains(s, "also-secret") {
		t.Errorf("--print-config leaks secrets:\n%s", s)
	}
	for _, want := range []string{"username: elastic", "group: shadowtap-indexer", "spans: traces-shadowtap-default", "service_name: shadowtap-indexer"} {
		if !strings.Contains(s, want) {
			t.Errorf("--print-config output lacks %q:\n%s", want, s)
		}
	}

	if err := run(ctx, []string{"-h"}, env(nil), io.Discard, io.Discard); err != nil {
		t.Errorf("-h: %v", err)
	}
	if err := run(ctx, []string{"--bogus"}, env(nil), io.Discard, io.Discard); err == nil {
		t.Error("unknown flag accepted")
	}
	if err := run(ctx, []string{"--config", "/nonexistent/indexer.yaml"}, env(nil), io.Discard, io.Discard); err == nil {
		t.Error("missing config file accepted")
	}
	if err := run(ctx, nil, env(map[string]string{index.EnvLogLevel: "loud"}), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "log.level") {
		t.Errorf("invalid log level: %v", err)
	}
	if err := run(ctx, nil, env(map[string]string{index.EnvESURL: "not a url"}), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "elasticsearch.urls") {
		t.Errorf("invalid Elasticsearch URL: %v", err)
	}

	// A listener failure still shuts the wired app down cleanly.
	err = run(ctx, nil, env(map[string]string{index.EnvHTTPAddr: "127.0.0.1:-1"}), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen http") {
		t.Errorf("bad listen address: %v", err)
	}
}
