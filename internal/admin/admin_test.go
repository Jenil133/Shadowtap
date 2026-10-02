package admin

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func serve(t *testing.T, ready func(context.Context) error) string {
	t.Helper()
	reg := prometheus.NewRegistry()
	c := prometheus.NewCounter(prometheus.CounterOpts{Name: "shadowtap_test_total", Help: "test"})
	reg.MustRegister(c)
	c.Inc()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(reg, ready, nil)
	done := make(chan error, 1)
	go func() { done <- s.Serve(ln) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := s.Shutdown(ctx); err != nil {
			t.Error(err)
		}
		if err := <-done; err != nil {
			t.Errorf("Serve returned %v after Shutdown", err)
		}
	})
	return "http://" + ln.Addr().String()
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestEndpoints(t *testing.T) {
	var failing atomic.Bool // the handler runs on the server's goroutine
	base := serve(t, func(context.Context) error {
		if failing.Load() {
			return errors.New("kafka unreachable")
		}
		return nil
	})

	if code, body := get(t, base+"/healthz"); code != 200 || body != "ok\n" {
		t.Errorf("healthz = %d %q", code, body)
	}
	if code, _ := get(t, base+"/readyz"); code != 200 {
		t.Errorf("readyz = %d", code)
	}
	failing.Store(true)
	if code, body := get(t, base+"/readyz"); code != 503 || !strings.Contains(body, "kafka unreachable") {
		t.Errorf("readyz = %d %q", code, body)
	}
	if code, body := get(t, base+"/metrics"); code != 200 || !strings.Contains(body, "shadowtap_test_total 1") {
		t.Errorf("metrics = %d %q", code, body)
	}
	resp, err := http.Post(base+"/healthz", "text/plain", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("POST /healthz = %d", resp.StatusCode)
	}
}

func TestReadinessCachesAndDrains(t *testing.T) {
	calls := 0
	failing := false
	r := NewReadiness(func(ctx context.Context) error {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Error("check called without a deadline")
		}
		if failing {
			return errors.New("down")
		}
		return nil
	}, 5*time.Second)
	now := time.Unix(1000, 0)
	r.now = func() time.Time { return now }

	ctx := context.Background()
	for range 3 {
		if err := r.Check(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if calls != 1 {
		t.Errorf("check called %d times within ttl", calls)
	}

	failing = true
	now = now.Add(6 * time.Second)
	if err := r.Check(ctx); err == nil || calls != 2 {
		t.Errorf("expected refreshed failure, err=%v calls=%d", err, calls)
	}
	if err := r.Check(ctx); err == nil || calls != 2 {
		t.Errorf("failure not cached, err=%v calls=%d", err, calls)
	}

	failing = false
	now = now.Add(6 * time.Second)
	r.SetDraining()
	if err := r.Check(ctx); !errors.Is(err, errDraining) || calls != 2 {
		t.Errorf("draining: err=%v calls=%d", err, calls)
	}
}
