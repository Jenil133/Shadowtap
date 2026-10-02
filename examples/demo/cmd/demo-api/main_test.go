package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestParseFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    config
		wantErr error
	}{
		{
			name: "defaults",
			want: config{addr: ":8080", apiVersion: "v1", logLevel: slog.LevelInfo, shutdownTimeout: 10 * time.Second},
		},
		{
			name: "all flags",
			args: []string{"--addr", "127.0.0.1:9000", "--version", "v1", "--log-level", "DEBUG", "--shutdown-timeout", "3s"},
			want: config{addr: "127.0.0.1:9000", apiVersion: "v1", logLevel: slog.LevelDebug, shutdownTimeout: 3 * time.Second},
		},
		{name: "bad log level", args: []string{"--log-level", "loud"}, wantErr: errUsage},
		{name: "bad timeout", args: []string{"--shutdown-timeout", "0s"}, wantErr: errUsage},
		{name: "unknown flag", args: []string{"--port", "80"}, wantErr: errUsage},
		{name: "stray argument", args: []string{"serve"}, wantErr: errUsage},
		{name: "help", args: []string{"--help"}, wantErr: flag.ErrHelp},
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

func TestRealMainExitCodes(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want int
	}{
		{[]string{"--help"}, 0},
		{[]string{"--log-level", "loud"}, 2},
		{[]string{"--addr", "127.0.0.1:0", "--version", "v9"}, 1},
		{[]string{"--addr", "256.0.0.1:0"}, 1},
	} {
		var stderr bytes.Buffer
		if got := realMain(tc.args, &stderr); got != tc.want {
			t.Errorf("realMain(%q) = %d, want %d; stderr: %s", tc.args, got, tc.want, stderr.String())
		}
	}
}

// syncBuffer collects log output written by the server goroutines.
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

// listenAddr waits for run's startup log line and returns the bound address.
func listenAddr(t *testing.T, logs *syncBuffer) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		sc := bufio.NewScanner(strings.NewReader(logs.String()))
		for sc.Scan() {
			var line struct {
				Msg  string `json:"msg"`
				Addr string `json:"addr"`
			}
			if json.Unmarshal(sc.Bytes(), &line) == nil && line.Msg == "demo-api listening" {
				return line.Addr
			}
		}
		time.Sleep(10 * time.Millisecond) // polling a log, bounded by the deadline
	}
	t.Fatalf("server never logged its address; logs:\n%s", logs.String())
	return ""
}

func TestRunServesUntilCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var logs syncBuffer
	done := make(chan error, 1)
	go func() { done <- run(ctx, []string{"--addr", "127.0.0.1:0"}, &logs) }()

	addr := listenAddr(t, &logs)
	resp, err := http.Get("http://" + addr + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || !strings.Contains(string(body), `"version":"v1"`) {
		t.Errorf("healthz: %d %s", resp.StatusCode, body)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return after cancellation")
	}
	for _, want := range []string{`"build_version":"dev"`, `"route":"GET /healthz"`, `"msg":"stopped"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %s:\n%s", want, logs.String())
		}
	}
}

func TestServeWaitsForInFlightRequests(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
		io.WriteString(w, "finished")
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, h, slog.New(slog.DiscardHandler), 5*time.Second) }()

	type result struct {
		body string
		err  error
	}
	got := make(chan result, 1)
	go func() {
		resp, err := http.Get("http://" + ln.Addr().String() + "/slow")
		if err != nil {
			got <- result{err: err}
			return
		}
		defer resp.Body.Close()
		b, err := io.ReadAll(resp.Body)
		got <- result{string(b), err}
	}()

	<-entered
	cancel() // shut down while the request is in flight
	select {
	case err := <-done:
		t.Fatalf("serve returned before the in-flight request finished: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(release)

	if r := <-got; r.err != nil || r.body != "finished" {
		t.Errorf("in-flight request: %q, %v", r.body, r.err)
	}
	if err := <-done; err != nil {
		t.Errorf("serve: %v", err)
	}
}

func TestServeShutdownTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	defer close(release)
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-release
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serve(ctx, ln, h, slog.New(slog.DiscardHandler), 50*time.Millisecond) }()
	go func() {
		if resp, err := http.Get("http://" + ln.Addr().String() + "/stuck"); err == nil {
			resp.Body.Close()
		}
	}()
	<-entered
	cancel()
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("serve = %v, want a deadline error", err)
	}
}
