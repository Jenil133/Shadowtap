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

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/twmb/franz-go/pkg/kfake"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/config"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/tap/taptest"
)

const testScrubKey = "main-test-scrub-key-0123456789"

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

func TestServeCapturesScrubsAndShutsDown(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, capture.DefaultTopic))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()

	cfg := config.DefaultTap()
	cfg.Scrub.Key = []byte(testScrubKey)
	cfg.Kafka.Brokers = cluster.ListenAddrs()
	cfg.Kafka.LingerMS = 1
	cfg.Sampling.DefaultRate = 1
	cfg.Tap.Name = "tap-test"
	cfg.Tap.Workers = 2
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	var logs syncBuffer
	a, err := newApp(cfg, slog.New(slog.NewJSONHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	grpcLn, err := net.Listen("tcp", "127.0.0.1:0")
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
	go func() { served <- a.serve(ctx, grpcLn, httpLn) }()

	base := "http://" + httpLn.Addr().String()
	waitFor(t, 10*time.Second, "readiness", func() bool {
		code, _, _ := httpGet(base + "/readyz")
		return code == http.StatusOK
	})

	conn, err := grpc.NewClient(grpcLn.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	health, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{
		Service: extprocv3.ExternalProcessor_ServiceDesc.ServiceName,
	})
	if err != nil || health.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("gRPC health = %v, %v", health.GetStatus(), err)
	}

	const (
		email = "alice.smith@example.com"
		card  = "4111 1111 1111 1111"
		pw    = "correct-horse-battery"
	)
	ex := taptest.Exchange{
		Method:    "POST",
		Scheme:    "http",
		Authority: "demo-api:8080",
		Path:      "/api/users?email=alice.smith%40example.com",
		RequestHeaders: [][2]string{
			{"content-type", "application/json"},
			{"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
		},
		RequestBody:     []byte(`{"email":"` + email + `","password":"` + pw + `","card_number":"` + card + `"}`),
		Status:          201,
		ResponseHeaders: [][2]string{{"content-type", "application/json"}},
		ResponseBody:    []byte(`{"id":"u_1","email":"` + email + `"}`),
	}
	opts := taptest.Options{Mode: capture.ModeInline, Metadata: map[string]string{"x-shadowtap-service": "demo-api"}}
	if _, err := taptest.Run(ctx, extprocv3.NewExternalProcessorClient(conn), ex, opts); err != nil {
		t.Fatal(err)
	}

	consumer, err := kafka.NewConsumer(cfg.Kafka, kafka.ConsumeOptions{Start: "earliest"})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	nctx, ncancel := context.WithTimeout(ctx, 10*time.Second)
	msg, err := consumer.Next(nctx)
	ncancel()
	if err != nil {
		t.Fatal(err)
	}
	if msg.DecodeErr != nil {
		t.Fatal(msg.DecodeErr)
	}
	x := msg.Exchange
	if x.Service != "demo-api" || x.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" || x.Source.Mode != capture.ModeInline || x.Source.Tap != "tap-test" {
		t.Errorf("unexpected capture: %+v", x)
	}
	for _, secret := range []string{email, card, pw, "alice.smith%40example.com"} {
		if strings.Contains(string(msg.Raw), secret) {
			t.Errorf("raw value %q reached Kafka", secret)
		}
	}
	if x.Redactions["email"] == 0 {
		t.Errorf("redactions = %v", x.Redactions)
	}

	_, metrics, err := httpGet(base + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"shadowtap_build_info", "shadowtap_tap_exchanges_total", "shadowtap_kafka_records_total", "go_goroutines"} {
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
	if !strings.Contains(logs.String(), "shutdown complete") {
		t.Errorf("missing shutdown log:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), email) || strings.Contains(logs.String(), testScrubKey) {
		t.Error("logs contain PII or the scrub key")
	}
}

func TestRunFlagsAndErrors(t *testing.T) {
	ctx := context.Background()
	var out bytes.Buffer

	if err := run(ctx, []string{"--version"}, env(nil), &out, io.Discard); err != nil || strings.TrimSpace(out.String()) != version {
		t.Errorf("--version: %q, %v", out.String(), err)
	}

	out.Reset()
	err := run(ctx, []string{"--print-config"}, env(map[string]string{config.EnvScrubKey: testScrubKey}), &out, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), testScrubKey) || !strings.Contains(out.String(), "default_rate") {
		t.Errorf("--print-config output:\n%s", out.String())
	}

	if err := run(ctx, []string{"-h"}, env(nil), io.Discard, io.Discard); err != nil {
		t.Errorf("-h: %v", err)
	}
	if err := run(ctx, []string{"--bogus"}, env(nil), io.Discard, io.Discard); err == nil {
		t.Error("unknown flag accepted")
	}
	if err := run(ctx, nil, env(nil), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), "scrub key") {
		t.Errorf("missing key: %v", err)
	}
	if err := run(ctx, []string{"--config", "/nonexistent/tap.yaml"}, env(nil), io.Discard, io.Discard); err == nil {
		t.Error("missing config file accepted")
	}

	// A listener failure still shuts the wired app down cleanly.
	err = run(ctx, nil, env(map[string]string{
		config.EnvScrubKey: testScrubKey,
		config.EnvGRPCAddr: "127.0.0.1:-1",
	}), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "listen grpc") {
		t.Errorf("bad listen address: %v", err)
	}
}
