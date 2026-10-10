package tracequery

import (
	"bufio"
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
)

// fakeES serves the _msearch API over fixture documents. It evaluates the
// subset of the query DSL this package emits (term, bool, match_all), honours
// index patterns, @timestamp sorting and size, and records every request so
// tests can assert on what was sent.
type fakeES struct {
	t       *testing.T
	indices map[string][]doc // index name -> documents

	mu       sync.Mutex
	requests []msearchRequest
	// respond, when set, answers every request instead of the evaluator.
	respond func(w http.ResponseWriter, r *http.Request)
}

type msearchRequest struct {
	header http.Header
	path   string
	lines  []map[string]any
}

func loadFixtures(t *testing.T) map[string][]doc {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("testdata", "fixtures.json"))
	if err != nil {
		t.Fatal(err)
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var fixtures map[string][]doc
	if err := dec.Decode(&fixtures); err != nil {
		t.Fatalf("fixtures.json: %v", err)
	}
	return fixtures
}

func newFakeES(t *testing.T) (*fakeES, *httptest.Server) {
	t.Helper()
	f := &fakeES{t: t, indices: loadFixtures(t)}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	return f, srv
}

func (f *fakeES) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The client refuses to talk to anything that does not identify itself
	// as Elasticsearch, so every answer, canned or evaluated, carries the
	// product header; a responder that wants to look like something else
	// deletes it.
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	f.mu.Lock()
	respond := f.respond
	f.mu.Unlock()
	if respond != nil {
		respond(w, r)
		return
	}
	if r.Method != http.MethodPost || !strings.HasSuffix(r.URL.Path, "/_msearch") {
		http.Error(w, `{"error":{"type":"not_found","reason":"no such endpoint"},"status":404}`, http.StatusNotFound)
		return
	}
	var lines []map[string]any
	sc := bufio.NewScanner(r.Body)
	sc.Buffer(make([]byte, 1<<20), 64<<20)
	for sc.Scan() {
		if strings.TrimSpace(sc.Text()) == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(sc.Text()))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			http.Error(w, `{"error":{"type":"parse_exception","reason":"bad ndjson"},"status":400}`, http.StatusBadRequest)
			return
		}
		lines = append(lines, m)
	}
	if len(lines)%2 != 0 {
		http.Error(w, `{"error":{"type":"parse_exception","reason":"odd line count"},"status":400}`, http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	f.requests = append(f.requests, msearchRequest{header: r.Header.Clone(), path: r.URL.Path, lines: lines})
	f.mu.Unlock()

	var responses []any
	for i := 0; i < len(lines); i += 2 {
		responses = append(responses, f.search(lines[i], lines[i+1]))
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{"took": 3, "responses": responses})
}

func (f *fakeES) search(header, body map[string]any) map[string]any {
	var patterns []string
	switch idx := header["index"].(type) {
	case string:
		patterns = strings.Split(idx, ",")
	case []any:
		for _, p := range idx {
			patterns = append(patterns, p.(string))
		}
	}
	var hits []doc
	for name, docs := range f.indices {
		if !matchesAny(name, patterns) {
			continue
		}
		for _, d := range docs {
			if evalQuery(body["query"], d) {
				hits = append(hits, d)
			}
		}
	}
	sort.SliceStable(hits, func(i, j int) bool { return hits[i].when(FieldTimestamp).Before(hits[j].when(FieldTimestamp)) })
	size := len(hits)
	if n, ok := body["size"].(json.Number); ok {
		if v, err := n.Int64(); err == nil && int(v) < size {
			size = int(v)
		}
	}
	out := make([]any, 0, size)
	for _, d := range hits[:size] {
		out = append(out, map[string]any{"_index": "fixture", "_id": d.str(FieldSpanID) + d.str(FieldCaptureID), "_source": d})
	}
	return map[string]any{
		"status": 200,
		"hits":   map[string]any{"total": map[string]any{"value": len(hits), "relation": "eq"}, "hits": out},
	}
}

func matchesAny(name string, patterns []string) bool {
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if strings.HasSuffix(p, "*") && strings.HasPrefix(name, strings.TrimSuffix(p, "*")) || p == name {
			return true
		}
	}
	return false
}

// evalQuery evaluates term, bool and match_all queries; anything else fails
// the test, because this package must not emit what the fake cannot check.
func evalQuery(q any, d doc) bool {
	m, ok := q.(map[string]any)
	if !ok || len(m) == 0 {
		return true
	}
	if _, ok := m["match_all"]; ok {
		return true
	}
	if term, ok := m["term"].(map[string]any); ok {
		for field, want := range term {
			if v, ok := want.(map[string]any); ok {
				want = v["value"]
			}
			return d.str(field) == scalarString(want) && d.str(field) != ""
		}
	}
	if b, ok := m["bool"].(map[string]any); ok {
		for _, clause := range []string{"must", "filter"} {
			for _, sub := range asList(b[clause]) {
				if !evalQuery(sub, d) {
					return false
				}
			}
		}
		should := asList(b["should"])
		if len(should) > 0 {
			matched := 0
			for _, sub := range should {
				if evalQuery(sub, d) {
					matched++
				}
			}
			minimum := 1
			if n, ok := b["minimum_should_match"].(json.Number); ok {
				v, _ := n.Int64()
				minimum = int(v)
			}
			return matched >= minimum
		}
		return true
	}
	panic("fake elasticsearch: unsupported query " + scalarString(m))
}

func asList(v any) []any {
	switch v := v.(type) {
	case []any:
		return v
	case nil:
		return nil
	default:
		return []any{v}
	}
}

func (f *fakeES) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func (f *fakeES) lastRequest() msearchRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.requests) == 0 {
		return msearchRequest{}
	}
	return f.requests[len(f.requests)-1]
}

func (f *fakeES) setResponder(fn func(w http.ResponseWriter, r *http.Request)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.respond = fn
}
