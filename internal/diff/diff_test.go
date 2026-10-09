package diff

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/scrub"
)

var testKey = []byte("diff-test-key-0123456789")

func newEngine(t *testing.T, cfg Config) *Engine {
	t.Helper()
	s, err := scrub.New(scrub.Config{Key: testKey})
	if err != nil {
		t.Fatal(err)
	}
	e, err := New(cfg, s, Options{})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

func diffsOf(t *testing.T, cfg Config, a, b string) []Difference {
	t.Helper()
	c, err := compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	pa, err := parseJSON([]byte(a))
	if err != nil {
		t.Fatal(err)
	}
	pb, err := parseJSON([]byte(b))
	if err != nil {
		t.Fatal(err)
	}
	d, _ := c.diffJSON(pa, pb, nil)
	return d
}

func summary(ds []Difference) []string {
	var out []string
	for _, d := range ds {
		out = append(out, d.Kind+" "+d.Pointer)
	}
	return out
}

func TestJSONDiff(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
		a, b string
		want []string
	}{
		{"equal", Config{}, `{"a":1,"b":[1,2]}`, `{"b":[1,2],"a":1}`, nil},
		{"changed added removed", Config{}, `{"a":1,"b":"x","c":true}`, `{"a":2,"c":true,"d":null}`,
			[]string{"changed /a", "removed /b", "added /d"}},
		{"type", Config{}, `{"a":"1"}`, `{"a":1}`, []string{"type /a"}},
		{"null vs value", Config{}, `{"a":null}`, `{"a":0}`, []string{"type /a"}},
		{"nested and escaped", Config{}, `{"a/b":{"c~d":[1,{"e":1}]}}`, `{"a/b":{"c~d":[1,{"e":2}]}}`, []string{"changed /a~1b/c~0d/1/e"}},
		{"array length", Config{}, `[1,2,3]`, `[1,2]`, []string{"removed /2"}},
		{"numbers by value", Config{}, `{"a":1.0,"b":100}`, `{"a":1,"b":1e2}`, nil},
		{"float tolerance", Config{FloatTolerance: 0.01}, `{"a":100}`, `{"a":100.5}`, nil},
		{"outside tolerance", Config{FloatTolerance: 0.001}, `{"a":100}`, `{"a":100.5}`, []string{"changed /a"}},
		{"ignore pointer", Config{IgnorePointers: []string{"/meta/generated_at", "/users/*/updated"}},
			`{"meta":{"generated_at":"1","n":1},"users":[{"updated":1,"id":1}]}`,
			`{"meta":{"generated_at":"2","n":1},"users":[{"updated":2,"id":1}]}`, nil},
		{"ignore subtree", Config{IgnorePointers: []string{"/debug/**"}}, `{"debug":{"a":{"b":1}}}`, `{"debug":{"a":{"b":2},"c":1}}`, nil},
		{"ignored key added", Config{IgnorePointers: []string{"/x"}}, `{}`, `{"x":1}`, nil},
		{"unordered", Config{UnorderedArrays: []string{"/tags"}}, `{"tags":["a","b","b"]}`, `{"tags":["b","a","c"]}`,
			[]string{"removed /tags/2", "added /tags/2"}},
		{"keyed arrays", Config{ArrayKeys: map[string]string{"/users": "id"}},
			`{"users":[{"id":"a","n":1},{"id":"b","n":2}]}`, `{"users":[{"id":"b","n":2},{"id":"a","n":9},{"id":"c"}]}`,
			[]string{"changed /users/0/n", "added /users/2"}},
		{"index order matters by default", Config{}, `[{"id":1},{"id":2}]`, `[{"id":2},{"id":1}]`,
			[]string{"changed /0/id", "changed /1/id"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := summary(diffsOf(t, tt.cfg, tt.a, tt.b))
			if !slices.Equal(got, tt.want) {
				t.Errorf("diffs = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestJSONDiffValuesAndCap(t *testing.T) {
	d := diffsOf(t, Config{}, `{"a":"`+strings.Repeat("x", 500)+`"}`, `{"a":"y"}`)
	if len(d) != 1 || len(d[0].Prod) > DefaultMaxValueBytes+len("…") || d[0].Candidate != `"y"` {
		t.Errorf("values not rendered and clipped: %+v", d)
	}
	var a, b strings.Builder
	a.WriteString("[")
	b.WriteString("[")
	for i := range 50 {
		if i > 0 {
			a.WriteString(",")
			b.WriteString(",")
		}
		a.WriteString("1")
		b.WriteString("2")
	}
	a.WriteString("]")
	b.WriteString("]")
	c, _ := compile(Config{MaxDifferences: 10})
	pa, _ := parseJSON([]byte(a.String()))
	pb, _ := parseJSON([]byte(b.String()))
	got, truncated := c.diffJSON(pa, pb, nil)
	if len(got) != 10 || !truncated {
		t.Errorf("%d diffs, truncated %v", len(got), truncated)
	}
	if got[3].template != "/*" {
		t.Errorf("template %q", got[3].template)
	}
}

func TestTextDiff(t *testing.T) {
	if d := diffText("a\nb\nc\n", "a\nb\nc\n"); d != nil {
		t.Errorf("equal texts differ: %v", d)
	}
	d := diffText("a\nb\nc\nd\n", "a\nB\nc\nd\ne\n")
	if len(d) != 1 || d[0].Pointer != "line 2" || d[0].Detail != "-1 +2 lines" || d[0].Prod != "b" || d[0].Candidate != "B" {
		t.Errorf("text diff = %+v", d)
	}
	if n, ok := myers(strings.Split("abcabba", ""), strings.Split("cbabac", ""), 100); !ok || n != 5 {
		t.Errorf("myers = %d %v, want 5", n, ok)
	}
	if _, ok := myers(strings.Split("aaaa", ""), strings.Split("bbbb", ""), 3); ok {
		t.Error("myers ignored its limit")
	}
}

func TestRouteTemplate(t *testing.T) {
	tests := map[string]string{
		"/api/users/u_3f2a1b9c0d4e?x=1":                        "GET /api/users/{id}",
		"/api/orders/o_0123456789ab":                           "GET /api/orders/{id}",
		"/api/users/lookup/user-0123456789%40redacted.invalid": "GET /api/users/lookup/{id}",
		"/v1/items/42/parts/7":                                 "GET /v1/items/{id}/parts/{id}",
		"/x/0192f0a0-1c2d-7000-8000-000000000001":              "GET /x/{id}",
		"/x/01ARZ3NDEKTSV4RRFFQ69G5FAV":                        "GET /x/{id}",
		"/x/deadbeefcafe1234":                                  "GET /x/{id}",
		"/x/tok_0123456789abcdef":                              "GET /x/{id}",
		"/api/users":                                           "GET /api/users",
		"/api/lookup-users/healthz":                            "GET /api/lookup-users/healthz",
		// Malformed paths are made absolute, so templates stay well-formed
		// and a template is its own template.
		"0":    "GET /{id}",
		"a/7":  "GET /a/{id}",
		"?x=1": "GET /",
		"":     "GET /",
	}
	for p, want := range tests {
		if got := RouteTemplate("get", p); got != want {
			t.Errorf("RouteTemplate(%s) = %s, want %s", p, got, want)
		}
	}
}

func recorded(status int, body string, headers capture.Headers) *capture.Exchange {
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            "cap-1",
		Service:       "demo-api",
		Timing:        capture.Timing{StartedAt: time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC), DurationMicros: 1500},
		Request:       capture.Request{Method: "GET", Path: "/api/users/u_3f2a1b9c0d4e"},
		Response:      capture.Response{Status: status, Headers: headers},
	}
	x.Response.Body = capture.Body{ContentType: headers.Get("content-type"), Size: int64(len(body))}
	x.Response.Body.SetData([]byte(body))
	return x
}

func live(status int, body string, headers http.Header) Live {
	return Live{Side: &Side{Status: status, Headers: toHeaders(headers), Body: []byte(body), ContentType: headers.Get("content-type")}, Latency: 2 * time.Millisecond}
}

func toHeaders(h http.Header) capture.Headers {
	out := capture.Headers{}
	for k, vs := range h {
		for _, v := range vs {
			out.Add(k, v)
		}
	}
	return out
}

var jsonHeaders = capture.Headers{"content-type": {"application/json"}, "date": {"Mon"}}

func TestCompareClasses(t *testing.T) {
	e := newEngine(t, Config{})
	x := recorded(200, `{"id":"u_1","phone":"tok_0123456789abcdef"}`, jsonHeaders)
	h := http.Header{"Content-Type": {"application/json; charset=utf-8"}, "Date": {"Tue"}}
	tests := []struct {
		name string
		live Live
		want string
	}{
		{"match", live(200, `{"phone":"tok_0123456789abcdef","id":"u_1"}`, h), ClassMatch},
		{"body", live(200, `{"id":"u_1","phone_number":"tok_0123456789abcdef"}`, h), ClassBodyMismatch},
		{"header", live(200, `{"id":"u_1","phone":"tok_0123456789abcdef"}`, http.Header{"Content-Type": {"text/plain"}}), ClassHeaderMismatch},
		{"status", live(500, `{"id":"u_1"}`, h), ClassStatusMismatch},
		{"error", Live{Err: errors.New("connection refused")}, ClassError},
	}
	for _, tt := range tests {
		r := e.Compare("run", x, tt.live, nil, nil)
		e.Finalize(r)
		if r.Class != tt.want {
			t.Errorf("%s: class %s, want %s (%+v)", tt.name, r.Class, tt.want, r.Differences)
		}
		if r.Route != "GET /api/users/{id}" || r.Latency.ProdMicros != 1500 {
			t.Errorf("%s: route %q latency %+v", tt.name, r.Route, r.Latency)
		}
	}
}

func TestHeaderComparison(t *testing.T) {
	e := newEngine(t, Config{CompareHeaders: []string{"x-api-version"}, IgnoreHeaders: []string{"cache-control"}})
	a := capture.Headers{
		"location":      {"http://prod.example/api/users/1"},
		"set-cookie":    {"session=tok_1; Path=/", "theme=dark"},
		"cache-control": {"no-store"},
		"x-api-version": {"1"},
		"x-other":       {"a"},
	}
	b := capture.Headers{
		"location":      {"/api/users/1"},
		"set-cookie":    {"theme=light", "session=tok_2; HttpOnly"},
		"cache-control": {"max-age=60"},
		"x-api-version": {"2"},
		"x-other":       {"b"},
	}
	got := summary(e.compareHeaders(a, b, e.cfg.ignoreHeaders))
	if want := []string{"changed x-api-version"}; !slices.Equal(got, want) {
		t.Errorf("header diffs = %v, want %v", got, want)
	}
}

func TestBaselineSuppressesReplayEffects(t *testing.T) {
	e := newEngine(t, Config{})
	h := http.Header{"Content-Type": {"application/json"}}
	// Production saw an existing user; the replay environment does not have
	// it, so baseline and candidate both answer 404: not the change's fault.
	x := recorded(200, `{"id":"u_1"}`, jsonHeaders)
	base := live(404, `{"error":"user not found"}`, h)
	r := e.Compare("run", x, live(404, `{"error":"user not found"}`, h), &base, nil)
	e.Finalize(r)
	if r.Class != ClassMatch || len(r.Differences) == 0 {
		t.Errorf("shared difference not suppressed: %s %+v", r.Class, r.Differences)
	}
	for _, d := range r.Differences {
		if d.Suppressed != SuppressedBaseline {
			t.Errorf("difference %+v not marked as explained by the baseline", d)
		}
	}
	// The candidate renames a field the baseline still has.
	x = recorded(200, `{"id":"u_1","phone":"tok_0123456789abcdef"}`, jsonHeaders)
	base = live(200, `{"id":"u_2","phone":"tok_0123456789abcdef"}`, h)
	r = e.Compare("run", x, live(200, `{"id":"u_2","phone_number":"tok_0123456789abcdef"}`, h), &base, nil)
	e.Finalize(r)
	if got := summary(r.Reported()); !slices.Equal(got, []string{"removed /phone", "added /phone_number"}) {
		t.Errorf("reported %v", got)
	}
	if r.Status.Baseline != 200 {
		t.Errorf("baseline status %d", r.Status.Baseline)
	}
	// Each difference carries the baseline's value: the baseline still has
	// phone and lacks phone_number; the id change is shared with it.
	for _, d := range r.Differences {
		var want string
		switch d.Pointer {
		case "/phone":
			want = `"tok_0123456789abcdef"`
		case "/phone_number":
			want = ""
		case "/id":
			want = d.Candidate
		}
		if d.Baseline != want {
			t.Errorf("%s: baseline value %q, want %q", d.Pointer, d.Baseline, want)
		}
	}
}

func TestThreeWayStatus(t *testing.T) {
	e := newEngine(t, Config{})
	h := http.Header{"Content-Type": {"application/json"}}
	// Production accepted the order; in the replay environment the baseline
	// rejects it with 422 and the candidate with 400: a real status change,
	// reported with all three values.
	x := recorded(201, `{"id":"o_1"}`, jsonHeaders)
	base := live(422, `{"error":"invalid"}`, h)
	r := e.Compare("run", x, live(400, `{"error":"invalid"}`, h), &base, nil)
	e.Finalize(r)
	if r.Class != ClassStatusMismatch {
		t.Fatalf("class %s: %+v", r.Class, r.Differences)
	}
	var status []Difference
	for _, d := range r.Differences {
		switch d.Where {
		case WhereStatus:
			status = append(status, d)
		case WhereBody:
			if d.Suppressed != SuppressedBaseline || d.Baseline != d.Candidate {
				t.Errorf("body difference shared with the baseline: %+v", d)
			}
		}
	}
	if len(status) != 1 || status[0].Prod != "201" || status[0].Baseline != "422" || status[0].Candidate != "400" || status[0].Suppressed != "" {
		t.Errorf("status differences %+v", status)
	}
}

func TestNoiseLearning(t *testing.T) {
	e := newEngine(t, Config{})
	h := http.Header{"Content-Type": {"application/json"}}
	var results []*Result
	for i := range 10 {
		x := recorded(200, `{"id":"u_1","generated_at":"2026-10-03T12:00:00Z"}`, jsonHeaders)
		at := func(ns int) string {
			return `{"id":"u_1","generated_at":"` + time.Date(2026, 10, 3, 13, 0, i, ns, time.UTC).Format(time.RFC3339Nano) + `"}`
		}
		base, probe := live(200, at(1), h), live(200, at(2), h)
		results = append(results, e.Compare("run", x, live(200, at(3), h), &base, &probe))
	}
	for _, r := range results {
		e.Finalize(r)
		if r.Class != ClassMatch {
			t.Fatalf("noise not suppressed: %s %+v", r.Class, r.Differences)
		}
		if r.Differences[0].Suppressed != SuppressedNoise {
			t.Errorf("suppression = %q, want noise", r.Differences[0].Suppressed)
		}
	}
	if got := e.NoiseReport()["GET /api/users/{id}"]; !slices.Equal(got, []string{"body:/generated_at"}) {
		t.Errorf("noise report = %v", got)
	}
}

func TestNormalizeScrubsAndDecodes(t *testing.T) {
	e := newEngine(t, Config{})
	var gz bytes.Buffer
	zw := gzip.NewWriter(&gz)
	zw.Write([]byte(`{"email":"alice@example.com","n":1}`))
	zw.Close()
	resp := &http.Response{
		StatusCode: 200,
		Header: http.Header{"Content-Type": {"application/json"}, "Content-Encoding": {"gzip"},
			"Set-Cookie": {"session=abc123secret"}},
		Body: io.NopCloser(&gz),
	}
	s, err := e.Normalize(resp)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(s.Body), "alice") || !strings.Contains(string(s.Body), "@redacted.invalid") {
		t.Errorf("body not scrubbed: %s", s.Body)
	}
	if strings.Contains(strings.Join(s.Headers["set-cookie"], ";"), "abc123secret") {
		t.Errorf("headers not scrubbed: %v", s.Headers)
	}
	// Scrubbing the recorded and the live response with one key gives the
	// same pseudonym, so an echoed value compares equal.
	sc, _ := scrub.New(scrub.Config{Key: testKey})
	rec := recorded(200, `{"email":"alice@example.com","n":1}`,
		capture.Headers{"content-type": {"application/json"}, "set-cookie": {"session=other"}})
	sc.Exchange(rec)
	r := e.Compare("run", rec, Live{Side: &s}, nil, nil)
	e.Finalize(r)
	if r.Class != ClassMatch {
		t.Errorf("same data scrubbed twice compares %s: %+v", r.Class, r.Differences)
	}
}

func TestBodiesNotComparable(t *testing.T) {
	e := newEngine(t, Config{})
	x := recorded(200, "", capture.Headers{"content-type": {"application/json"}})
	x.Response.Body = capture.Body{ContentType: "application/json", Size: 300000, Omitted: capture.OmittedTruncatedUnsafe, Truncated: true}
	r := e.Compare("run", x, live(200, `{"a":1}`, http.Header{"Content-Type": {"application/json"}}), nil, nil)
	e.Finalize(r)
	if r.Class != ClassBodyMismatch || r.Differences[0].Detail != "body not comparable" {
		t.Errorf("%s %+v", r.Class, r.Differences)
	}
	x.Response.Headers = nil
	x.Response.Body = capture.Body{Size: 10, Omitted: capture.OmittedBinary}
	cand := Live{Side: &Side{Status: 200, Size: 10, NotComparable: capture.OmittedBinary}}
	r = e.Compare("run", x, cand, nil, nil)
	e.Finalize(r)
	if r.Class != ClassMatch {
		t.Errorf("equal binary sizes: %s", r.Class)
	}
	cand.Side.Size = 11
	r = e.Compare("run", x, cand, nil, nil)
	e.Finalize(r)
	if r.Class != ClassBodyMismatch {
		t.Errorf("binary size change: %s", r.Class)
	}
}

func TestConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "diff.yaml")
	os.WriteFile(path, []byte("ignore_pointers: [/meta/**]\narray_keys: {/users: id}\nroutes:\n  - route: '^GET /api/users'\n    ignore_pointers: [/total]\n"), 0o600)
	c, err := LoadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	e := newEngine(t, c)
	x := recorded(200, `{"meta":{"t":1},"total":1,"users":[{"id":"a"},{"id":"b"}]}`, jsonHeaders)
	r := e.Compare("run", x, live(200, `{"meta":{"t":2},"total":2,"users":[{"id":"b"},{"id":"a"}]}`, http.Header{"Content-Type": {"application/json"}}), nil, nil)
	e.Finalize(r)
	if r.Class != ClassMatch {
		t.Errorf("configured ignores not applied: %+v", r.Differences)
	}
	os.WriteFile(path, []byte("bogus: 1\n"), 0o600)
	if _, err := LoadConfig(path); err == nil {
		t.Error("unknown field accepted")
	}
	for _, bad := range []Config{{IgnorePointers: []string{"meta"}}, {NoiseThreshold: 2}, {Routes: []RouteRule{{Route: "("}}}, {ArrayKeys: map[string]string{"/a": ""}}} {
		if _, err := compile(bad); err == nil {
			t.Errorf("%+v compiled", bad)
		}
	}
}

func TestSkipped(t *testing.T) {
	x := recorded(200, "", jsonHeaders)
	r := Skipped("run", x, "mutating_method")
	if r.Class != ClassSkipped || r.Reason != "mutating_method" {
		t.Errorf("%+v", r)
	}
}
