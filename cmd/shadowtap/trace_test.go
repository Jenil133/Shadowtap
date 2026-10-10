package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	traceID       = "4bf92f3577b34da6a3ce929d0e0e4736"
	replayTraceID = "7d3e8a1b2c4f5e6d7a8b9c0d1e2f3a4b"
	captureID     = "0199b3f0-1234-7abc-8def-0123456789ab"
	requestID     = "req-8c1d2e3f"
)

// traceDocs are the indexed documents the fake Elasticsearch serves, in the
// shape the indexer writes: one request with a sidecar span, an app span and
// a log, its capture, the diff of its replay and the replay's own span.
var traceDocs = map[string][]map[string]any{
	"traces-shadowtap-default": {
		{
			"@timestamp": "2026-10-03T09:00:00Z",
			"trace":      map[string]any{"id": traceID},
			"span":       map[string]any{"id": "0a1b2c3d4e5f6071", "name": "gateway.demo.svc.cluster.local:8080/*", "kind": "SERVER"},
			"event":      map[string]any{"duration": 24000000},
			"service":    map[string]any{"name": "gateway.demo"},
			"status":     map[string]any{"code": "UNSET"},
			"http":       map[string]any{"request": map[string]any{"method": "GET"}, "response": map[string]any{"status_code": 200}},
			"url":        map[string]any{"path": "/api/users/42/orders"},
		},
		{
			"@timestamp": "2026-10-03T09:00:00.0004Z",
			"trace":      map[string]any{"id": traceID},
			"span":       map[string]any{"id": "b2c3d4e5f6071829", "name": "GET /api/users/{id}/orders", "kind": "SERVER"},
			"parent":     map[string]any{"id": "0a1b2c3d4e5f6071"},
			"event":      map[string]any{"duration": 23200000},
			"service":    map[string]any{"name": "gateway"},
			"status":     map[string]any{"code": "OK"},
		},
		{
			"@timestamp": "2026-10-03T10:00:00Z",
			"trace":      map[string]any{"id": replayTraceID},
			"span":       map[string]any{"id": "5c6d7e8f90112233", "name": "shadowtap.replay GET /api/users/{id}/orders", "kind": "CLIENT"},
			"event":      map[string]any{"duration": 31000000},
			"service":    map[string]any{"name": "shadowtap-replay"},
			"status":     map[string]any{"code": "OK"},
			"links":      []any{map[string]any{"trace": map[string]any{"id": traceID}, "span": map[string]any{"id": "b2c3d4e5f6071829"}}},
		},
	},
	"logs-shadowtap-default": {
		{
			"@timestamp": "2026-10-03T09:00:00.0009Z",
			"trace":      map[string]any{"id": traceID},
			"span":       map[string]any{"id": "b2c3d4e5f6071829"},
			"service":    map[string]any{"name": "gateway"},
			"log":        map[string]any{"level": "INFO"},
			"message":    "proxying to users",
		},
	},
	"shadowtap-captures": {
		{
			"@timestamp": "2026-10-03T09:00:00.0001Z",
			"event":      map[string]any{"duration": 23600000},
			"capture":    map[string]any{"id": captureID},
			"trace":      map[string]any{"id": traceID},
			"service":    map[string]any{"name": "gateway"},
			"source":     map[string]any{"tap": "tap-0", "mode": "observe"},
			"http": map[string]any{
				"route":    "GET /api/users/{id}/orders",
				"request":  map[string]any{"method": "GET", "id": requestID, "body": map[string]any{"bytes": 0}},
				"response": map[string]any{"status_code": 200, "mime_type": "application/json", "body": map[string]any{"bytes": 15, "content": `{"secret":"body"}`}},
			},
			"url": map[string]any{"path": "/api/users/42/orders"},
		},
	},
	"shadowtap-diffs": {
		{
			"@timestamp": "2026-10-03T10:00:00Z",
			"event":      map[string]any{"start": "2026-10-03T09:00:00.0001Z"},
			"replay":     map[string]any{"run": map[string]any{"id": "run-1"}, "trace": map[string]any{"id": replayTraceID}, "sent_at": "2026-10-03T10:00:00Z", "status_code": 200},
			"capture":    map[string]any{"id": captureID},
			"trace":      map[string]any{"id": traceID},
			"service":    map[string]any{"name": "gateway"},
			"http":       map[string]any{"route": "GET /api/users/{id}/orders", "request": map[string]any{"method": "GET"}, "response": map[string]any{"status_code": 200}},
			"diff":       map[string]any{"class": "match", "reported_count": 0},
			"latency":    map[string]any{"prod_us": 23600, "candidate_us": 29000},
		},
	},
}

// fakeES answers _msearch over traceDocs, matching the term queries the
// command sends, and records the Authorization header of each request.
type fakeES struct {
	mu       sync.Mutex
	requests int
	auth     []string
}

func (f *fakeES) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	w.Header().Set("Content-Type", "application/json")
	f.mu.Lock()
	f.requests++
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	f.mu.Unlock()
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/_msearch") {
		http.Error(w, `{"error":{"type":"not_found","reason":"no such endpoint"},"status":404}`, http.StatusNotFound)
		return
	}
	var lines []map[string]any
	sc := bufio.NewScanner(r.Body)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			http.Error(w, `{"error":{"type":"parse_exception","reason":"bad ndjson"},"status":400}`, http.StatusBadRequest)
			return
		}
		lines = append(lines, m)
	}
	var responses []any
	for i := 0; i+1 < len(lines); i += 2 {
		index, _ := lines[i]["index"].(string)
		var hits []any
		for name, docs := range traceDocs {
			if !(index == name || strings.HasSuffix(index, "*") && strings.HasPrefix(name, strings.TrimSuffix(index, "*"))) {
				continue
			}
			for _, d := range docs {
				if matchesTerms(lines[i+1]["query"], d) {
					hits = append(hits, map[string]any{"_source": d})
				}
			}
		}
		responses = append(responses, map[string]any{"status": 200, "hits": map[string]any{"total": map[string]any{"value": len(hits)}, "hits": hits}})
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"responses": responses})
}

// matchesTerms reports whether any term query inside q matches the document;
// the command only sends single terms and a bool/should of terms.
func matchesTerms(q any, d map[string]any) bool {
	switch q := q.(type) {
	case map[string]any:
		if term, ok := q["term"].(map[string]any); ok {
			for field, want := range term {
				return lookup(d, field) == want
			}
		}
		for _, v := range q {
			if matchesTerms(v, d) {
				return true
			}
		}
	case []any:
		for _, v := range q {
			if matchesTerms(v, d) {
				return true
			}
		}
	}
	return false
}

// lookup resolves a dotted path through nested maps.
func lookup(d map[string]any, path string) any {
	var cur any = d
	for _, p := range strings.Split(path, ".") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[p]
	}
	return cur
}

func (f *fakeES) authHeaders() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.auth...)
}

func startFakeES(t *testing.T) (*fakeES, string) {
	t.Helper()
	f := &fakeES{}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv.URL
}

// traceCLI runs the trace command with the given environment.
func traceCLI(t *testing.T, env map[string]string, args ...string) (stdout, stderr string, code int) {
	t.Helper()
	var out, errOut bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	code = runTrace(ctx, args, func(k string) string { return env[k] }, &out, &errOut)
	return out.String(), errOut.String(), code
}

func TestTraceByTraceID(t *testing.T) {
	_, url := startFakeES(t)
	out, errOut, code := traceCLI(t, nil, "--es", url, strings.ToUpper(traceID))
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	for _, want := range []string{
		"trace " + traceID,
		"capture " + captureID,
		"diff run-1  class match",
		"gateway.demo  gateway.demo.svc.cluster.local:8080/*  [SERVER]",
		"gateway  GET /api/users/{id}/orders  [SERVER]  OK",
		"log   +0.900ms  INFO   gateway  proxying to users",
		"replay trace " + replayTraceID, // named by the diff
	} {
		if !strings.Contains(out, want) {
			t.Errorf("output lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, `"secret":"body"`) {
		t.Error("bodies printed without --show-bodies")
	}
	if strings.Contains(out, "shadowtap.replay GET") {
		t.Error("replay trace spans shown without --follow-replay")
	}
	if errOut != "" {
		t.Errorf("stderr: %q", errOut)
	}

	out, _, code = traceCLI(t, nil, "--es", url, "--show-bodies", "--follow-replay", traceID)
	if code != exitOK || !strings.Contains(out, `{"secret":"body"}`) || !strings.Contains(out, "shadowtap-replay  shadowtap.replay GET /api/users/{id}/orders  [CLIENT]") {
		t.Errorf("--show-bodies --follow-replay: exit %d\n%s", code, out)
	}
}

func TestTraceJSON(t *testing.T) {
	_, url := startFakeES(t)
	out, errOut, code := traceCLI(t, nil, "--es", url, "--json", "--request-id", requestID)
	if code != exitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	var doc struct {
		TraceID    string `json:"trace_id"`
		SpanCount  int    `json:"span_count"`
		LogCount   int    `json:"log_count"`
		Captures   []any  `json:"captures"`
		Diffs      []any  `json:"diffs"`
		ResolvedBy struct {
			RequestID string `json:"request_id"`
		} `json:"resolved_by"`
	}
	if err := json.Unmarshal([]byte(out), &doc); err != nil {
		t.Fatalf("stdout is not JSON: %v\n%s", err, out)
	}
	if doc.TraceID != traceID || doc.SpanCount != 2 || doc.LogCount != 1 || len(doc.Captures) != 1 || len(doc.Diffs) != 1 || doc.ResolvedBy.RequestID != requestID {
		t.Errorf("document = %+v", doc)
	}
	if strings.Contains(out, `secret`) {
		t.Error("bodies in JSON without --show-bodies")
	}

	out, _, code = traceCLI(t, nil, "--es", url, "--json", "--show-bodies", "--capture-id", captureID)
	if code != exitOK || !strings.Contains(out, `"content": "{\"secret\":\"body\"}"`) {
		t.Errorf("--capture-id --show-bodies: exit %d\n%s", code, out)
	}
}

func TestTraceExitCodes(t *testing.T) {
	_, url := startFakeES(t)
	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()
	denied := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Elastic-Product", "Elasticsearch")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = io.WriteString(w, `{"error":{"type":"security_exception","reason":"missing authentication credentials"},"status":401}`)
	}))
	t.Cleanup(denied.Close)
	stalled := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		<-r.Context().Done()
	}))
	t.Cleanup(stalled.Close)

	tests := []struct {
		name       string
		args       []string
		code       int
		wantStderr string
	}{
		{name: "unknown trace", args: []string{"--es", url, strings.Repeat("0", 32)}, code: exitNotFound, wantStderr: "not found"},
		{name: "unknown request id", args: []string{"--es", url, "--request-id", "nope"}, code: exitNotFound, wantStderr: "no capture indexed"},
		{name: "unknown capture id", args: []string{"--es", url, "--capture-id", "nope"}, code: exitNotFound, wantStderr: "no capture indexed"},
		{name: "unreachable", args: []string{"--es", closedURL, traceID}, code: exitError, wantStderr: "unreachable"},
		{name: "unauthorized", args: []string{"--es", denied.URL, traceID}, code: exitError, wantStderr: "credentials"},
		{name: "timeout", args: []string{"--es", stalled.URL, "--timeout", "100ms", traceID}, code: exitError, wantStderr: "no answer within 100ms"},
		{name: "no selector", args: []string{"--es", url}, code: exitUsage, wantStderr: "required"},
		{name: "two selectors", args: []string{"--es", url, "--request-id", requestID, traceID}, code: exitUsage, wantStderr: "one of"},
		{name: "two trace ids", args: []string{"--es", url, traceID, traceID}, code: exitUsage, wantStderr: "one trace id expected"},
		{name: "short trace id", args: []string{"--es", url, "abc"}, code: exitUsage, wantStderr: "hex"},
		{name: "unknown flag", args: []string{"--bogus", traceID}, code: exitUsage, wantStderr: "flag provided but not defined"},
		{name: "bad url", args: []string{"--es", "localhost:9200", traceID}, code: exitUsage, wantStderr: "http(s)://"},
		{name: "zero timeout", args: []string{"--es", url, "--timeout", "0", traceID}, code: exitUsage, wantStderr: "positive"},
		{name: "help", args: []string{"-h"}, code: exitOK, wantStderr: "Usage: shadowtap trace"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out, errOut, code := traceCLI(t, nil, tt.args...)
			if code != tt.code {
				t.Fatalf("exit %d, want %d (stdout %q, stderr %q)", code, tt.code, out, errOut)
			}
			if !strings.Contains(errOut, tt.wantStderr) {
				t.Errorf("stderr %q does not mention %q", errOut, tt.wantStderr)
			}
			if code != exitOK && out != "" {
				t.Errorf("stdout must stay empty on failure, got %q", out)
			}
		})
	}
}

func TestTraceEnvironment(t *testing.T) {
	fake, url := startFakeES(t)
	env := map[string]string{envESURL: url, envESUsername: "elastic", envESPassword: "secret"}
	if _, errOut, code := traceCLI(t, env, traceID); code != exitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if auth := fake.authHeaders(); len(auth) != 1 || auth[0] != "Basic ZWxhc3RpYzpzZWNyZXQ=" {
		t.Errorf("Authorization from the environment = %v", auth)
	}

	// An API key wins over basic auth, and flags win over the environment.
	env[envESAPIKey] = "ZnJvbS1lbnY="
	if _, errOut, code := traceCLI(t, env, "--es-api-key", "ZnJvbS1mbGFn", traceID); code != exitOK {
		t.Fatalf("exit %d: %s", code, errOut)
	}
	if auth := fake.authHeaders(); len(auth) != 2 || !strings.EqualFold(auth[1], "ApiKey ZnJvbS1mbGFn") {
		t.Errorf("Authorization with --es-api-key = %v", auth)
	}

	// Without --es and $SHADOWTAP_ES_URL the default is localhost:9200, which
	// is not what the fake listens on.
	_, errOut, code := traceCLI(t, map[string]string{}, "--timeout", "2s", traceID)
	if code != exitError || !strings.Contains(errOut, "localhost:9200") {
		t.Errorf("default URL: exit %d, stderr %q", code, errOut)
	}
}

func TestTraceCancelled(t *testing.T) {
	_, url := startFakeES(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out, errOut bytes.Buffer
	if code := runTrace(ctx, []string{"--es", url, traceID}, func(string) string { return "" }, &out, &errOut); code != exitError || !strings.Contains(errOut.String(), "interrupted") {
		t.Errorf("exit %d, stderr %q", code, errOut.String())
	}
}
