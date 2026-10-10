package tracequery

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	originalTrace = "4bf92f3577b34da6a3ce929d0e0e4736"
	replayTrace   = "7d3e8a1b2c4f5e6d7a8b9c0d1e2f3a4b"
	captureID     = "0199b3f0-1234-7abc-8def-0123456789ab"
)

func newTestClient(t *testing.T, url string) *Client {
	t.Helper()
	c, err := New(Config{URL: url, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestNewConfig(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr bool
		wantURL string
	}{
		{name: "default url", cfg: Config{}, wantURL: DefaultURL},
		{name: "several urls", cfg: Config{URL: "http://a:9200, http://b:9200"}, wantURL: "http://a:9200"},
		{name: "basic auth", cfg: Config{URL: "https://es:9200", Username: "u", Password: "p"}, wantURL: "https://es:9200"},
		{name: "api key", cfg: Config{URL: "https://es:9200", APIKey: "abc", Insecure: true}, wantURL: "https://es:9200"},
		{name: "bad scheme", cfg: Config{URL: "es:9200"}, wantErr: true},
		{name: "not a url", cfg: Config{URL: "http://[::1"}, wantErr: true},
		{name: "only commas", cfg: Config{URL: " , "}, wantErr: true},
		{name: "missing ca file", cfg: Config{URL: "https://es:9200", CAFile: t.TempDir() + "/missing.pem"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && c.URL() != tt.wantURL {
				t.Errorf("URL() = %q, want %q", c.URL(), tt.wantURL)
			}
		})
	}
}

func TestQueryValidate(t *testing.T) {
	tests := []struct {
		name    string
		q       Query
		wantID  string
		wantErr string
	}{
		{name: "w3c id", q: Query{TraceID: "4BF92F3577B34DA6A3CE929D0E0E4736"}, wantID: originalTrace},
		{name: "b3 64-bit id", q: Query{TraceID: " 0a1b2c3d4e5f6071 "}, wantID: "0a1b2c3d4e5f6071"},
		{name: "request id", q: Query{RequestID: "req-1"}},
		{name: "nothing", q: Query{}, wantErr: "required"},
		{name: "two selectors", q: Query{TraceID: originalTrace, RequestID: "r"}, wantErr: "one of"},
		{name: "short id", q: Query{TraceID: "abc"}, wantErr: "32 (or 16) hex"},
		{name: "non hex", q: Query{TraceID: strings.Repeat("g", 32)}, wantErr: "hex"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := tt.q
			err := q.Validate()
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if q.TraceID != tt.wantID {
				t.Errorf("TraceID = %q, want %q", q.TraceID, tt.wantID)
			}
		})
	}
}

func TestLookupByTraceID(t *testing.T) {
	fake, srv := newFakeES(t)
	c := newTestClient(t, srv.URL)
	res, err := c.Lookup(context.Background(), Query{TraceID: strings.ToUpper(originalTrace)})
	if err != nil {
		t.Fatal(err)
	}
	tr := res.Trace
	if tr.ID != originalTrace || len(tr.Spans) != 11 || len(tr.Logs) != 7 || len(tr.Captures) != 1 || len(tr.Diffs) != 1 {
		t.Fatalf("spans %d logs %d captures %d diffs %d", len(tr.Spans), len(tr.Logs), len(tr.Captures), len(tr.Diffs))
	}
	if got := strings.Join(tr.Services(), ","); got != "gateway.demo,shadowtap-tap,gateway,users.demo,users,orders.demo,orders" {
		t.Errorf("services = %s", got)
	}
	if tr.Duration != 28*time.Millisecond {
		t.Errorf("duration = %s, want 28ms (first span start to the tap's publish end)", tr.Duration)
	}
	// The sidecar root and the tap's capture span both name the client's span,
	// which was never exported, so the tree hangs off a synthetic root.
	if !tr.Root.Synthetic || len(tr.Root.Children) != 2 {
		t.Fatalf("root synthetic=%v children=%d", tr.Root.Synthetic, len(tr.Root.Children))
	}
	sidecar, tap := tr.Root.Children[0], tr.Root.Children[1]
	if sidecar.SpanID != "0a1b2c3d4e5f6071" || !sidecar.Orphan || tap.SpanID != "3a4b5c6d7e8f9011" || !tap.Orphan {
		t.Errorf("top spans = %s (orphan %v), %s (orphan %v)", sidecar.SpanID, sidecar.Orphan, tap.SpanID, tap.Orphan)
	}
	// Nested and dotted documents resolve to the same fields.
	gw := sidecar.Children[0]
	if gw.Service != "gateway" || gw.Kind != "SERVER" || gw.Status != "OK" || gw.HTTPStatus != 200 || len(gw.Children) != 2 {
		t.Errorf("gateway span = %+v", gw)
	}
	if gw.Children[0].Name != "GET users" || gw.Children[1].Name != "GET orders" {
		t.Errorf("gateway children = %s, %s", gw.Children[0].Name, gw.Children[1].Name)
	}
	cache := gw.Children[1].Children[0].Children[0].Children[0]
	if cache.Name != "cache GET user-orders" || cache.Status != "ERROR" || cache.StatusMessage != "deadline exceeded" {
		t.Errorf("cache span = %+v", cache)
	}
	if len(cache.Logs) != 1 || cache.Logs[0].Level != "WARN" {
		t.Errorf("cache logs = %+v", cache.Logs)
	}
	if un := tr.UnattachedLogs(); len(un) != 1 || un[0].Message != "accepted request" {
		t.Errorf("unattached logs = %+v", un)
	}
	cp := tr.Captures[0]
	if cp.ID != captureID || cp.RequestID != "req-8c1d2e3f" || cp.Status != 200 || cp.Redactions["email"] != 1 || cp.Response.Size != 104 || !strings.Contains(cp.Response.Content, "redacted.invalid") {
		t.Errorf("capture = %+v", cp)
	}
	d := tr.Diffs[0]
	if d.Class != "body_mismatch" || d.ReplayTraceID != replayTrace || len(d.Differences) != 2 || len(d.Reported()) != 1 || d.Reported()[0].Pointer != "/orders/0/total" || d.Differences[1].Suppressed != "noise" {
		t.Errorf("diff = %+v", d)
	}
	// Latencies are stored in microseconds; the start is the original
	// request's, the timestamp the replay's.
	if d.ProdLatency != 23600*time.Microsecond || d.CandidateLatency != 29*time.Millisecond || d.BaselineLatency != 24100*time.Microsecond || d.ScheduleLag != 1200*time.Microsecond {
		t.Errorf("diff latencies = prod %s candidate %s baseline %s lag %s", d.ProdLatency, d.CandidateLatency, d.BaselineLatency, d.ScheduleLag)
	}
	if d.CandidateStatus != 200 || d.BaselineStatus != 200 || !d.Start.Equal(tr.Start.Add(100*time.Microsecond)) || !d.SentAt.Equal(time.Date(2026, 10, 3, 10, 0, 0, 0, time.UTC)) {
		t.Errorf("diff status/times = %+v", d)
	}
	if cp.Tap != "tap-0" || cp.Mode != "observe" || cp.Route != "GET /api/users/{id}/orders" {
		t.Errorf("capture source/route = %q %q %q", cp.Tap, cp.Mode, cp.Route)
	}
	if res.Related != nil {
		t.Errorf("related traces fetched without FollowReplay: %d", len(res.Related))
	}

	// One round trip with four searches, each bounded and sorted.
	if fake.requestCount() != 1 {
		t.Fatalf("requests = %d, want 1", fake.requestCount())
	}
	req := fake.lastRequest()
	if len(req.lines) != 8 {
		t.Fatalf("msearch lines = %d, want 8", len(req.lines))
	}
	for i := 0; i < 8; i += 2 {
		if req.lines[i]["ignore_unavailable"] != true {
			t.Errorf("search %d does not ignore missing indices: %v", i/2, req.lines[i])
		}
		if _, ok := req.lines[i+1]["sort"]; !ok {
			t.Errorf("search %d is unsorted", i/2)
		}
	}
	if req.lines[0]["index"] != IndexTraces || req.lines[6]["index"] != IndexDiffs {
		t.Errorf("indices = %v, %v", req.lines[0]["index"], req.lines[6]["index"])
	}
}

func TestLookupFollowReplay(t *testing.T) {
	_, srv := newFakeES(t)
	c := newTestClient(t, srv.URL)

	res, err := c.Lookup(context.Background(), Query{TraceID: originalTrace, FollowReplay: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Related) != 1 || res.Related[0].Relation != "replay" || res.Related[0].Trace.ID != replayTrace {
		t.Fatalf("related = %+v", res.Related)
	}
	rt := res.Related[0].Trace
	if len(rt.Spans) != 2 || rt.Root.Synthetic || rt.Root.Service != "shadowtap-replay" || len(rt.Diffs) != 1 || len(rt.Logs) != 1 {
		t.Errorf("replay trace: spans %d root %q diffs %d logs %d", len(rt.Spans), rt.Root.Service, len(rt.Diffs), len(rt.Logs))
	}
	if got := rt.Root.Links; len(got) != 1 || got[0].TraceID != originalTrace || got[0].SpanID != "b2c3d4e5f6071829" {
		t.Errorf("replay span links = %+v", got)
	}

	// And back: from the replay trace to the original, through the diff and
	// the span link (deduplicated).
	res, err = c.Lookup(context.Background(), Query{TraceID: replayTrace, FollowReplay: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Related) != 1 || res.Related[0].Relation != "original" || res.Related[0].Trace.ID != originalTrace {
		t.Fatalf("related = %+v", res.Related)
	}
}

func TestLookupByRequestAndCaptureID(t *testing.T) {
	fake, srv := newFakeES(t)
	c := newTestClient(t, srv.URL)

	tests := []struct {
		name     string
		q        Query
		wantID   string
		spans    int
		captures int
	}{
		{name: "request id", q: Query{RequestID: "req-8c1d2e3f"}, wantID: originalTrace, spans: 11, captures: 1},
		{name: "capture id", q: Query{CaptureID: captureID}, wantID: originalTrace, spans: 11, captures: 1},
		{name: "capture without trace context", q: Query{RequestID: "req-notrace"}, wantID: "", spans: 0, captures: 1},
		{name: "trace with only a capture", q: Query{RequestID: "req-only-capture"}, wantID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", spans: 0, captures: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := c.Lookup(context.Background(), tt.q)
			if err != nil {
				t.Fatal(err)
			}
			if res.Trace.ID != tt.wantID || len(res.Trace.Spans) != tt.spans || len(res.Trace.Captures) != tt.captures {
				t.Errorf("trace %q spans %d captures %d, want %q/%d/%d", res.Trace.ID, len(res.Trace.Spans), len(res.Trace.Captures), tt.wantID, tt.spans, tt.captures)
			}
			if res.Query.TraceID != tt.wantID {
				t.Errorf("resolved trace id = %q, want %q", res.Query.TraceID, tt.wantID)
			}
		})
	}
	if fake.lastRequest().lines[1]["size"] == nil {
		t.Error("capture lookup is unbounded")
	}
}

func TestLookupNotFound(t *testing.T) {
	_, srv := newFakeES(t)
	c := newTestClient(t, srv.URL)
	for _, q := range []Query{
		{TraceID: strings.Repeat("0", 32)},
		{RequestID: "req-unknown"},
		{CaptureID: "0199b3f0-0000-0000-0000-000000000000"},
	} {
		res, err := c.Lookup(context.Background(), q)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("%+v: err = %v, result %v; want ErrNotFound", q, err, res)
		}
	}
}

func TestLookupErrors(t *testing.T) {
	tests := []struct {
		name        string
		status      int
		body        string
		wantMessage string
		unreachable bool
		// notES answers without the X-Elastic-Product header, as a proxy or
		// an unrelated service would.
		notES bool
	}{
		{name: "unauthorized", status: 401, body: `{"error":{"type":"security_exception","reason":"missing authentication credentials"},"status":401}`, wantMessage: "rejected the credentials"},
		{name: "forbidden", status: 403, body: `{"error":{"type":"security_exception","reason":"unauthorized"},"status":403}`, wantMessage: "lack read access"},
		{name: "server error", status: 500, body: `{"error":{"type":"search_phase_execution_exception","reason":"all shards failed"},"status":500}`, wantMessage: "all shards failed"},
		{name: "not elasticsearch", status: 200, body: `<html>proxy</html>`, notES: true, wantMessage: "not Elasticsearch"},
		{name: "elasticsearch header but not msearch", status: 200, body: `<html>proxy</html>`, wantMessage: "something other than an msearch response"},
		{name: "wrong response count", status: 200, body: `{"responses":[{"status":200,"hits":{"hits":[]}}]}`, wantMessage: "1 responses for 4 searches"},
		{name: "per-search error", status: 200, body: `{"responses":[{"status":200,"hits":{"hits":[]}},{"status":400,"error":{"type":"illegal_argument_exception","reason":"bad sort"}},{"status":200,"hits":{"hits":[]}},{"status":200,"hits":{"hits":[]}}]}`, wantMessage: "bad sort"},
		{name: "unreachable", unreachable: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var url string
			if tt.unreachable {
				srv := httptest.NewServer(http.NotFoundHandler())
				url = srv.URL
				srv.Close()
			} else {
				fake, srv := newFakeES(t)
				fake.setResponder(func(w http.ResponseWriter, _ *http.Request) {
					if tt.notES {
						w.Header().Del("X-Elastic-Product")
					}
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(tt.status)
					_, _ = w.Write([]byte(tt.body))
				})
				url = srv.URL
			}
			c := newTestClient(t, url)
			_, err := c.Lookup(context.Background(), Query{TraceID: originalTrace})
			if err == nil {
				t.Fatal("no error")
			}
			if errors.Is(err, ErrNotFound) {
				t.Fatalf("an error is reported as not found: %v", err)
			}
			var unreachable *UnreachableError
			if errors.As(err, &unreachable) != tt.unreachable {
				t.Errorf("unreachable = %v, want %v: %v", !tt.unreachable, tt.unreachable, err)
			}
			if tt.wantMessage != "" && !strings.Contains(err.Error(), tt.wantMessage) {
				t.Errorf("err = %q, want it to mention %q", err, tt.wantMessage)
			}
		})
	}
}

func TestLookupMissingIndicesAreEmpty(t *testing.T) {
	fake, srv := newFakeES(t)
	fake.setResponder(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"responses":[
			{"status":404,"error":{"type":"index_not_found_exception","reason":"no such index [traces-shadowtap-*]"}},
			{"status":404,"error":{"type":"index_not_found_exception","reason":"no such index [logs-shadowtap-*]"}},
			{"status":404,"error":{"type":"index_not_found_exception","reason":"no such index [shadowtap-captures]"}},
			{"status":404,"error":{"type":"index_not_found_exception","reason":"no such index [shadowtap-diffs]"}}]}`))
	})
	c := newTestClient(t, srv.URL)
	_, err := c.Lookup(context.Background(), Query{TraceID: originalTrace})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("a fresh cluster must answer not found, got %v", err)
	}
}

func TestLookupTimeout(t *testing.T) {
	fake, srv := newFakeES(t)
	fake.setResponder(func(w http.ResponseWriter, r *http.Request) {
		// Drain the body first: the server only watches for the client
		// hanging up once the request has been read, and a handler that
		// never returns would block the server's Close at cleanup.
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done() // hang until the client gives up
	})
	c, err := New(Config{URL: srv.URL, Timeout: 50 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Lookup(context.Background(), Query{TraceID: originalTrace})
	var unreachable *UnreachableError
	if !errors.As(err, &unreachable) || !strings.Contains(err.Error(), "no answer within") {
		t.Fatalf("err = %v, want an unreachable error mentioning the timeout", err)
	}

	// A cancelled caller is reported as such, not as an outage.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.Lookup(ctx, Query{TraceID: originalTrace}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

func TestClientSendsCredentials(t *testing.T) {
	fake, srv := newFakeES(t)
	var got http.Header
	fake.setResponder(func(w http.ResponseWriter, r *http.Request) {
		got = r.Header.Clone()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"responses":[{"status":200,"hits":{"hits":[]}},{"status":200,"hits":{"hits":[]}},{"status":200,"hits":{"hits":[]}},{"status":200,"hits":{"hits":[]}}]}`))
	})
	for _, tt := range []struct {
		name string
		cfg  Config
		want string
	}{
		{name: "basic", cfg: Config{URL: srv.URL, Username: "elastic", Password: "secret"}, want: "Basic ZWxhc3RpYzpzZWNyZXQ="},
		{name: "api key wins", cfg: Config{URL: srv.URL, Username: "elastic", Password: "secret", APIKey: "aWQ6a2V5"}, want: "ApiKey aWQ6a2V5"},
		{name: "none", cfg: Config{URL: srv.URL}, want: ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := New(tt.cfg)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Lookup(context.Background(), Query{TraceID: originalTrace}); !errors.Is(err, ErrNotFound) {
				t.Fatalf("err = %v", err)
			}
			// The scheme is case-insensitive (RFC 9110) and the library has
			// spelled it both ApiKey and APIKey across versions.
			if !strings.EqualFold(got.Get("Authorization"), tt.want) {
				t.Errorf("Authorization = %q, want %q", got.Get("Authorization"), tt.want)
			}
		})
	}
}
