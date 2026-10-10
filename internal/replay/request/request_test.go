package request

import (
	"context"
	"io"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/tracectx"
)

func target(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := ValidateTarget(raw, nil)
	if err != nil {
		t.Fatal(err)
	}
	return u
}

func captured() *capture.Exchange {
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            "0192f0a0-0000-7000-8000-000000000001",
		Service:       "demo-api",
		TraceID:       "4bf92f3577b34da6a3ce929d0e0e4736",
		Timing:        capture.Timing{StartedAt: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)},
		Request: capture.Request{
			Method:    "POST",
			Authority: "demo-api.demo.svc.cluster.local:8080",
			Path:      "/api/users/lookup/user-0123456789%40redacted.invalid?x=1&y=a%2Fb",
			Headers: capture.Headers{
				"content-type":       {"application/json"},
				"content-encoding":   {"gzip"},
				"content-length":     {"999"},
				"accept":             {"application/json"},
				"authorization":      {"Bearer tok_0123456789abcdef"},
				"connection":         {"keep-alive, x-hop"},
				"x-hop":              {"1"},
				"keep-alive":         {"timeout=5"},
				"transfer-encoding":  {"chunked"},
				"x-request-id":       {"r-1"},
				"traceparent":        {"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
				"x-b3-traceid":       {"4bf92f3577b34da6a3ce929d0e0e4736"},
				"x-envoy-attempt":    {"1"},
				"x-forwarded-proto":  {"http"},
				"x-forwarded-for":    {"240.1.2.3"},
				"x-custom":           {"a", "b"},
				"x-shadowtap-replay": {"kept"},
			},
		},
		Response: capture.Response{Status: 201},
	}
	x.Request.Body = capture.Body{ContentType: "application/json", Size: 17, DecodedFrom: "gzip"}
	x.Request.Body.SetData([]byte(`{"name":"tok_ab"}`))
	return x
}

func TestBuild(t *testing.T) {
	x := captured()
	b, err := Build(context.Background(), target(t, "http://candidate:8080/base/"), x, Options{
		RunID:          "run-1",
		SetHeaders:     [][2]string{{"Authorization", "Bearer candidate-test-token"}},
		DropHeaders:    []string{"X-Forwarded-For"},
		NewTraceparent: func() string { return "00-11111111111111111111111111111111-2222222222222222-01" },
	})
	if err != nil {
		t.Fatal(err)
	}
	r := b.Request
	if r.Method != "POST" {
		t.Errorf("method %s", r.Method)
	}
	if got, want := r.URL.String(), "http://candidate:8080/base/api/users/lookup/user-0123456789%40redacted.invalid?x=1&y=a%2Fb"; got != want {
		t.Errorf("url\n got %s\nwant %s", got, want)
	}
	for _, gone := range []string{"Content-Encoding", "Content-Length", "Connection", "X-Hop", "Keep-Alive", "Transfer-Encoding",
		"X-Request-Id", "X-B3-Traceid", "X-Envoy-Attempt", "X-Forwarded-For"} {
		if v := r.Header.Values(gone); len(v) > 0 {
			t.Errorf("%s kept: %v", gone, v)
		}
	}
	checks := map[string]string{
		"Authorization":       "Bearer candidate-test-token",
		"Content-Type":        "application/json",
		"X-Forwarded-Proto":   "http",
		"Traceparent":         "00-11111111111111111111111111111111-2222222222222222-01",
		HeaderRun:             "run-1",
		HeaderCaptureID:       x.ID,
		HeaderOriginalTraceID: x.TraceID,
		"X-Shadowtap-Replay":  "kept",
		"Accept":              "application/json",
	}
	for k, want := range checks {
		if got := r.Header.Get(k); got != want {
			t.Errorf("%s = %q, want %q", k, got, want)
		}
	}
	if v := r.Header.Values("X-Custom"); len(v) != 2 {
		t.Errorf("multi-value header: %v", v)
	}
	if b.TraceID != "11111111111111111111111111111111" {
		t.Errorf("TraceID %s", b.TraceID)
	}
	body, _ := io.ReadAll(r.Body)
	if string(body) != `{"name":"tok_ab"}` || r.ContentLength != int64(len(body)) {
		t.Errorf("body %q, content length %d", body, r.ContentLength)
	}
	if r.Host != "" && r.Host != "candidate:8080" {
		t.Errorf("host %q", r.Host)
	}
}

func TestBuildKeepsEncodingOfUndecodedBody(t *testing.T) {
	x := captured()
	x.Request.Body.DecodedFrom = ""
	b, err := Build(context.Background(), target(t, "http://localhost:9"), x, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if b.Request.Header.Get("Content-Encoding") != "gzip" {
		t.Error("content-encoding of a body sent as stored was dropped")
	}
	tc, ok := tracectx.ParseTraceparent(b.Request.Header.Get("traceparent"))
	if !ok || tc.TraceID != b.TraceID || !tc.Sampled {
		t.Errorf("random traceparent %q invalid", b.Request.Header.Get("traceparent"))
	}
}

func TestBuildWithoutBody(t *testing.T) {
	x := captured()
	x.Request.Method, x.Request.Body = "GET", capture.Body{}
	b, err := Build(context.Background(), target(t, "http://localhost:9"), x, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if b.Request.ContentLength != 0 {
		t.Errorf("content length %d", b.Request.ContentLength)
	}
}

func TestBuildRefusesIncompleteBodies(t *testing.T) {
	for _, body := range []capture.Body{{Size: 10, Truncated: true}, {Size: 10, Omitted: capture.OmittedBinary}} {
		x := captured()
		x.Request.Body = body
		if _, err := Build(context.Background(), target(t, "http://localhost:9"), x, Options{}); err != ErrBodyUnavailable {
			t.Errorf("%+v: err = %v", body, err)
		}
	}
	x := captured()
	x.Request.Path = "relative"
	if _, err := Build(context.Background(), target(t, "http://localhost:9"), x, Options{}); err == nil {
		t.Error("relative path accepted")
	}
}

func TestValidateTarget(t *testing.T) {
	tests := []struct {
		raw   string
		allow []string
		ok    bool
	}{
		{"http://localhost:8080", nil, true},
		{"http://127.0.0.1:8080", nil, true},
		{"http://[::1]:8080", nil, true},
		{"http://demo-api:8080", nil, true},
		{"http://demo-api-v2.demo.svc.cluster.local:8080", nil, true},
		{"http://demo-api.demo.svc:8080/base", nil, true},
		{"https://staging.example.com", nil, false},
		{"https://staging.example.com", []string{"*.example.com"}, true},
		{"https://staging.example.com:8443", []string{"staging.example.com:8443"}, true},
		{"https://api.prod.example.com", []string{"*.example.com"}, false},
		{"https://api.prod.example.com", []string{"api.prod.example.com"}, true},
		{"http://orders-prod:8080", nil, false},
		{"http://10.0.0.5:8080", nil, false},
		{"ftp://localhost", nil, false},
		{"http://user:pw@localhost", nil, false},
		{"http://localhost/?x=1", nil, false},
		{"http://localhost/#f", nil, false},
		{"localhost:8080", nil, false},
		{"http://", nil, false},
	}
	for _, tt := range tests {
		_, err := ValidateTarget(tt.raw, tt.allow)
		if (err == nil) != tt.ok {
			t.Errorf("ValidateTarget(%q, %v) = %v, want ok=%v", tt.raw, tt.allow, err, tt.ok)
		}
		if err != nil && strings.Contains(err.Error(), "pw") {
			t.Errorf("error leaks credentials: %v", err)
		}
	}
}
