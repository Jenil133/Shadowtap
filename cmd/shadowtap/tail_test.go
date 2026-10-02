package main

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/kafka"
)

var start = time.Date(2026, 10, 3, 9, 0, 0, 0, time.UTC)

func exchange(i int, service, method, path string, status int) *capture.Exchange {
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            capture.NewID(),
		Service:       service,
		TraceID:       strings.Repeat(string(rune('a'+i)), 32),
		Source:        capture.Source{Tap: "tap-0", Mode: capture.ModeObserve},
		Sampling:      capture.Sampling{Rate: 1, Rule: "default", KeySource: "traceparent"},
		Timing:        capture.Timing{StartedAt: start.Add(time.Duration(i) * time.Minute), DurationMicros: 1500},
		Request:       capture.Request{Method: method, Path: path, Headers: capture.Headers{"accept": {"*/*"}}},
		Response:      capture.Response{Status: status},
	}
	x.Response.Body.ContentType = "application/json"
	x.Response.Body.SetData([]byte(`{"ok":true,"email":"user-0123456789@redacted.invalid"}`))
	return x
}

func cluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(3, capture.DefaultTopic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func publish(t *testing.T, brokers []string, xs ...*capture.Exchange) {
	t.Helper()
	cfg := kafka.DefaultConfig()
	cfg.Brokers = brokers
	cfg.LingerMS = 1
	p, err := kafka.NewProducer(cfg, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range xs {
		if err := p.Publish(context.Background(), x); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := p.Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func cli(t *testing.T, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	code = run(ctx, args, func(string) string { return "" }, &out, &errOut)
	return out.String(), errOut.String(), code
}

func lines(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}

func TestTailFormatsAndFilters(t *testing.T) {
	brokers := cluster(t)
	publish(t, brokers,
		exchange(0, "demo-api", "GET", "/api/users", 200),
		exchange(1, "orders", "POST", "/api/orders", 201),
		exchange(2, "demo-api", "POST", "/api/users", 503),
	)
	base := []string{"tail", "--brokers", strings.Join(brokers, ","), "--from", "earliest", "--timeout", "1s"}

	out, errOut, code := cli(t, append(base, "--format", "json")...)
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	got := lines(out)
	if len(got) != 3 {
		t.Fatalf("json lines = %d:\n%s", len(got), out)
	}
	for _, l := range got {
		if _, err := capture.Decode([]byte(l)); err != nil {
			t.Errorf("line does not decode: %v", err)
		}
	}
	if !strings.Contains(errOut, "3 records read, 3 matched") {
		t.Errorf("stderr summary: %s", errOut)
	}

	filters := []struct {
		args []string
		want int
	}{
		{[]string{"--service", "demo-api"}, 2},
		{[]string{"--status", "5xx"}, 1},
		{[]string{"--status", "201"}, 1},
		{[]string{"--method", "post"}, 2},
		{[]string{"--path-prefix", "/api/orders"}, 1},
		{[]string{"--trace-id", strings.Repeat("B", 32)}, 1},
		{[]string{"--until", start.Add(30 * time.Second).Format(time.RFC3339)}, 1},
		{[]string{"--limit", "2"}, 2},
	}
	for _, f := range filters {
		out, errOut, code := cli(t, append(append(base, "--format", "json"), f.args...)...)
		if code != exitOK || len(lines(out)) != f.want {
			t.Errorf("%v: exit %d, %d lines, want %d (%s)", f.args, code, len(lines(out)), f.want, errOut)
		}
	}

	out, _, _ = cli(t, append(base, "--format", "summary", "--status", "503")...)
	if !strings.Contains(out, "demo-api observe POST /api/users -> 503 1.5ms") || !strings.Contains(out, "trace=cccc") {
		t.Errorf("summary output: %q", out)
	}

	out, _, _ = cli(t, append(base, "--format", "pretty", "--service", "orders")...)
	var x capture.Exchange
	if err := json.Unmarshal([]byte(out), &x); err != nil || x.Service != "orders" || !strings.Contains(out, "\n  \"id\"") {
		t.Errorf("pretty output (%v): %s", err, out)
	}
}

func TestTailCheckPII(t *testing.T) {
	brokers := cluster(t)
	base := []string{"tail", "--brokers", strings.Join(brokers, ","), "--from", "earliest", "--timeout", "1s", "--check-pii"}

	publish(t, brokers, exchange(0, "demo-api", "GET", "/api/users/user-0123456789@redacted.invalid", 200))
	out, errOut, code := cli(t, base...)
	if code != exitOK || !strings.Contains(errOut, "PII audit passed") || out != "" {
		t.Fatalf("clean audit: exit %d out=%q err=%q", code, out, errOut)
	}

	const leakedEmail = "leaked.person@example.com"
	const leakedCard = "4111111111111111"
	leak := exchange(1, "demo-api", "POST", "/api/orders?card="+leakedCard, 201)
	leak.Request.Body.SetData([]byte(`{"email":"` + leakedEmail + `"}`))
	leak.Request.Headers["x-user-email"] = []string{leakedEmail}
	publish(t, brokers, leak)

	out, errOut, code = cli(t, base...)
	if code != exitPII {
		t.Fatalf("leaky audit exit = %d, want %d (%s)", code, exitPII, errOut)
	}
	for _, want := range []string{"field=request.body kind=email", "field=request.path kind=card", "field=request.headers.x-user-email kind=email"} {
		if !strings.Contains(out, want) {
			t.Errorf("audit output missing %q:\n%s", want, out)
		}
	}
	if strings.Contains(out+errOut, leakedEmail) || strings.Contains(out+errOut, leakedCard) {
		t.Error("audit printed a raw PII value")
	}
	if !strings.Contains(errOut, "PII audit FAILED") {
		t.Errorf("stderr: %s", errOut)
	}
}

func TestTailCheckPIISeesThroughEncoding(t *testing.T) {
	brokers := cluster(t)
	hidden := exchange(0, "demo-api", "GET", "/api/users?email=hidden.person%40example.com", 200)
	hidden.Request.Body.ContentType = "application/json"
	hidden.Request.Body.SetData([]byte(`{"contact":"escaped.person\u0040example.com"}`))
	publish(t, brokers, hidden)

	out, errOut, code := cli(t, "tail", "--brokers", strings.Join(brokers, ","), "--from", "earliest", "--timeout", "1s", "--check-pii")
	if code != exitPII {
		t.Fatalf("exit %d, want %d (%s)", code, exitPII, errOut)
	}
	for _, want := range []string{"field=request.path kind=email", "field=request.body kind=email"} {
		if strings.Count(out, want) != 1 {
			t.Errorf("want exactly one %q:\n%s", want, out)
		}
	}
}

func TestTailSkipsUndecodableRecords(t *testing.T) {
	brokers := cluster(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.DefaultProduceTopic(capture.DefaultTopic))
	if err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Value: []byte("not json")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()
	publish(t, brokers, exchange(0, "demo-api", "GET", "/", 200))

	out, errOut, code := cli(t, "tail", "--brokers", strings.Join(brokers, ","), "--from", "earliest", "--timeout", "1s", "--format", "json")
	if code != exitOK || len(lines(out)) != 1 || !strings.Contains(errOut, "1 undecodable") {
		t.Errorf("exit %d, %d lines, stderr %q", code, len(lines(out)), errOut)
	}
}

func TestUsage(t *testing.T) {
	tests := []struct {
		args []string
		code int
	}{
		{nil, exitUsage},
		{[]string{"bogus"}, exitUsage},
		{[]string{"help"}, exitOK},
		{[]string{"version"}, exitOK},
		{[]string{"tail", "-h"}, exitOK},
		{[]string{"tail", "--format", "xml"}, exitUsage},
		{[]string{"tail", "--status", "abc"}, exitUsage},
		{[]string{"tail", "--status", "6xx"}, exitUsage},
		{[]string{"tail", "--until", "yesterday"}, exitUsage},
		{[]string{"tail", "extra"}, exitUsage},
		{[]string{"tail", "--brokers", " , "}, exitError},
		{[]string{"tail", "--from", "sometime"}, exitError},
	}
	for _, tt := range tests {
		if _, errOut, code := cli(t, tt.args...); code != tt.code {
			t.Errorf("%v: exit %d, want %d (%s)", tt.args, code, tt.code, errOut)
		}
	}
}
