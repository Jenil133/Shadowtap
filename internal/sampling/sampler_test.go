package sampling

import (
	"fmt"
	"math"
	"strings"
	"testing"
)

func headers(kv ...string) func(string) string {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return func(k string) string { return m[k] }
}

func traceparent(i int) string {
	return fmt.Sprintf("00-%032x-00f067aa0ba902b7-01", uint64(i)*0x9e3779b97f4a7c15+1)
}

func mustNew(t *testing.T, cfg Config) *Sampler {
	t.Helper()
	s, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestRateAccuracy(t *testing.T) {
	const n = 200_000
	for _, r := range []float64{0.01, 0.1, 0.5} {
		s := mustNew(t, Config{DefaultRate: r})
		sampled := 0
		for i := range n {
			if s.Decide(Request{Method: "GET", Path: "/", Header: headers("traceparent", traceparent(i))}).Sampled {
				sampled++
			}
		}
		if got := float64(sampled) / n; math.Abs(got-r) > 0.005 {
			t.Errorf("rate %v: sampled fraction %.4f", r, got)
		}
	}
}

func TestDeterministicAndNested(t *testing.T) {
	low := mustNew(t, Config{DefaultRate: 0.05})
	high := mustNew(t, Config{DefaultRate: 0.5})
	other := mustNew(t, Config{DefaultRate: 0.05}) // another tap, same config
	lowCount := 0
	for i := range 20_000 {
		req := Request{Method: "GET", Path: "/x", Header: headers("traceparent", traceparent(i))}
		a, b := low.Decide(req), high.Decide(req)
		if a.Sampled != other.Decide(req).Sampled || a.Sampled != low.Decide(req).Sampled {
			t.Fatalf("decision for %d not deterministic", i)
		}
		if a.Sampled {
			lowCount++
			if !b.Sampled {
				t.Fatalf("trace %d sampled at 5%% but not at 50%%", i)
			}
		}
	}
	if lowCount == 0 {
		t.Fatal("nothing sampled at 5%")
	}
}

func TestTraceConsistentAcrossServices(t *testing.T) {
	cfg := Config{DefaultRate: 0.2, Rules: []Rule{{Name: "orders", Services: []string{"orders"}, Rate: 0.2}}}
	s := mustNew(t, cfg)
	for i := range 5000 {
		h := headers("traceparent", traceparent(i), "x-request-id", fmt.Sprintf("req-%d", i))
		gw := s.Decide(Request{Service: "gateway", Method: "GET", Path: "/a", Header: h})
		orders := s.Decide(Request{Service: "orders", Method: "POST", Path: "/b", Header: h})
		if gw.Sampled != orders.Sampled {
			t.Fatalf("trace %d: gateway=%v orders=%v", i, gw.Sampled, orders.Sampled)
		}
	}
}

func TestKeySources(t *testing.T) {
	s := mustNew(t, Config{DefaultRate: 1})
	tests := []struct {
		h    func(string) string
		want string
	}{
		{headers("traceparent", traceparent(1), "x-request-id", "r"), KeySourceTraceparent},
		{headers("b3", "80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1"), KeySourceB3},
		{headers("x-b3-traceid", "463ac35c9f6413ad48485a3953bb6124"), KeySourceB3Multi},
		{headers("x-request-id", "r"), KeySourceRequestID},
		{headers(), KeySourceRandom},
		{nil, KeySourceRandom},
	}
	for i, tt := range tests {
		d := s.Decide(Request{Method: "GET", Path: "/", Header: tt.h})
		if d.KeySource != tt.want || !d.Sampled {
			t.Errorf("case %d: %+v, want key source %s", i, d, tt.want)
		}
	}
}

func TestRequestIDSamplingIsDeterministic(t *testing.T) {
	s := mustNew(t, Config{DefaultRate: 0.3})
	for i := range 1000 {
		h := headers("x-request-id", fmt.Sprintf("id-%d", i))
		if s.Decide(Request{Method: "GET", Path: "/", Header: h}).Sampled != s.Decide(Request{Method: "GET", Path: "/", Header: h}).Sampled {
			t.Fatal("x-request-id decision not deterministic")
		}
	}
}

func TestExclusions(t *testing.T) {
	s := mustNew(t, Config{DefaultRate: 1, ExcludePaths: []string{"/healthz"}})
	tests := []struct {
		req  Request
		rule string
	}{
		{Request{Method: "GET", Path: "/healthz"}, RuleExcludePath},
		{Request{Method: "GET", Path: "/healthz?verbose=1"}, RuleExcludePath},
		{Request{Method: "CONNECT", Path: "/"}, RuleUpgrade},
		{Request{Method: "GET", Path: "/ws", Header: headers("upgrade", "websocket")}, RuleUpgrade},
	}
	for _, tt := range tests {
		d := s.Decide(tt.req)
		if d.Sampled || d.Reason != ReasonExcluded || d.Rule != tt.rule {
			t.Errorf("%+v: got %+v", tt.req, d)
		}
	}
	if d := s.Decide(Request{Method: "GET", Path: "/healthz/deep"}); !d.Sampled {
		t.Errorf("/healthz/deep should not be excluded: %+v", d)
	}
}

func TestRulePrecedence(t *testing.T) {
	cfg := Config{
		DefaultRate: 1,
		Rules: []Rule{
			{Name: "no-admin", PathPrefix: "/admin", Rate: 0},
			{Name: "orders-post", Methods: []string{"post"}, PathRegex: `^/api/orders(/|$)`, Rate: 1},
			{Name: "users-svc", Services: []string{"users"}, Rate: 0},
			{Name: "catch-all", Rate: 0.5},
		},
	}
	s := mustNew(t, cfg)
	tests := []struct {
		req     Request
		rule    string
		sampled bool
	}{
		{Request{Service: "x", Method: "GET", Path: "/admin/x"}, "no-admin", false},
		{Request{Service: "users", Method: "POST", Path: "/api/orders/1"}, "orders-post", true},
		{Request{Service: "users", Method: "GET", Path: "/api/orders/1"}, "users-svc", false},
		{Request{Service: "x", Method: "GET", Path: "/api/ordersx"}, "catch-all", false},
	}
	for _, tt := range tests {
		tt.req.Header = headers("x-request-id", "fixed")
		d := s.Decide(tt.req)
		if d.Rule != tt.rule {
			t.Errorf("%+v: rule %q, want %q", tt.req, d.Rule, tt.rule)
		}
		if tt.rule != "catch-all" && d.Sampled != tt.sampled {
			t.Errorf("%+v: sampled %v, want %v", tt.req, d.Sampled, tt.sampled)
		}
		if tt.rule == "catch-all" && d.Rate != 0.5 {
			t.Errorf("catch-all rate = %v", d.Rate)
		}
	}
}

func TestRateLimit(t *testing.T) {
	s := mustNew(t, Config{DefaultRate: 1, MaxPerSecond: 1, Burst: 2})
	var reasons []string
	for range 4 {
		reasons = append(reasons, s.Decide(Request{Method: "GET", Path: "/"}).Reason)
	}
	want := []string{ReasonSampled, ReasonSampled, ReasonRateLimited, ReasonRateLimited}
	if strings.Join(reasons, ",") != strings.Join(want, ",") {
		t.Errorf("reasons = %v, want %v", reasons, want)
	}
	// Default burst is ceil(max_per_second).
	s = mustNew(t, Config{DefaultRate: 1, MaxPerSecond: 0.5})
	if s.Decide(Request{Method: "GET", Path: "/"}).Reason != ReasonSampled {
		t.Error("first request should pass with default burst")
	}
}

func TestEdgeRates(t *testing.T) {
	never := mustNew(t, Config{DefaultRate: 0})
	always := mustNew(t, Config{DefaultRate: 1})
	for i := range 1000 {
		req := Request{Method: "GET", Path: "/", Header: headers("traceparent", traceparent(i))}
		if never.Decide(req).Sampled {
			t.Fatal("rate 0 sampled")
		}
		if d := always.Decide(req); !d.Sampled || d.Rate != 1 {
			t.Fatalf("rate 1 decision %+v", d)
		}
	}
	if d := never.Decide(Request{Method: "GET", Path: "/"}); d.Reason != ReasonUnsampled || d.Rule != RuleDefault {
		t.Errorf("unsampled decision = %+v", d)
	}
}

func TestValidate(t *testing.T) {
	bad := []Config{
		{DefaultRate: -0.1},
		{DefaultRate: 1.5},
		{MaxPerSecond: -1},
		{Burst: -1},
		{ExcludePaths: []string{"healthz"}},
		{Rules: []Rule{{Rate: 0.5}}},
		{Rules: []Rule{{Name: "a"}, {Name: "a"}}},
		{Rules: []Rule{{Name: "a", Rate: 2}}},
		{Rules: []Rule{{Name: "a", PathRegex: "("}}},
	}
	for i, cfg := range bad {
		if _, err := New(cfg); err == nil {
			t.Errorf("config %d validated: %+v", i, cfg)
		}
	}
	if err := DefaultConfig().Validate(); err != nil {
		t.Errorf("default config invalid: %v", err)
	}
}

func TestSaltChangesDecisions(t *testing.T) {
	a := mustNew(t, Config{DefaultRate: 0.5})
	b := mustNew(t, Config{DefaultRate: 0.5, Salt: "other"})
	diff := 0
	for i := range 1000 {
		req := Request{Method: "GET", Path: "/", Header: headers("traceparent", traceparent(i))}
		if a.Decide(req).Sampled != b.Decide(req).Sampled {
			diff++
		}
	}
	if diff < 300 {
		t.Errorf("salt changed only %d of 1000 decisions", diff)
	}
}

func BenchmarkDecide(b *testing.B) {
	s, _ := New(Config{DefaultRate: 0.05, Rules: []Rule{{Name: "a", PathPrefix: "/api", Rate: 0.1}}})
	req := Request{Service: "svc", Method: "GET", Path: "/api/users?x=1", Header: headers("traceparent", traceparent(42))}
	for b.Loop() {
		s.Decide(req)
	}
}
