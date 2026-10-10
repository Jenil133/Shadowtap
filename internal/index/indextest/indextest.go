// Package indextest provides an in-process fake of the Elasticsearch APIs
// the indexer uses: the bulk API with create semantics (201 for a new id,
// 409 for a known one), the ILM, component template, index template and
// data stream APIs the bootstrap calls, and the ping. Tests inspect what was
// stored and put, and can script item statuses (429, 500, ...), request
// failures, stalls and dropped connections.
package indextest

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
)

// Request is one non-bulk request the fake received.
type Request struct {
	Method string
	Path   string
	Body   []byte
}

// Server is a fake Elasticsearch.
type Server struct {
	srv     *httptest.Server
	arrived chan struct{}

	mu          sync.Mutex
	docs        map[string]map[string]json.RawMessage
	requests    []Request
	dataStreams map[string]bool
	attempts    map[string]int
	bulkCount   int
	itemStatus  func(index, id string, attempt int) int
	requestHook func(r *http.Request) (int, string)
	hold        chan struct{}
	abortNext   int
}

// NewServer starts a fake Elasticsearch. Call Close when done.
func NewServer() *Server {
	s := &Server{
		arrived:     make(chan struct{}, 1024),
		docs:        map[string]map[string]json.RawMessage{},
		dataStreams: map[string]bool{},
		attempts:    map[string]int{},
	}
	s.srv = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// URL is the base URL to configure as elasticsearch.urls.
func (s *Server) URL() string { return s.srv.URL }

// Close stops the server.
func (s *Server) Close() { s.srv.Close() }

// Docs returns a copy of the documents stored in index, by id.
func (s *Server) Docs(index string) map[string]json.RawMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]json.RawMessage, len(s.docs[index]))
	for id, doc := range s.docs[index] {
		out[id] = doc
	}
	return out
}

// Doc returns one stored document.
func (s *Server) Doc(index, id string) (json.RawMessage, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	doc, ok := s.docs[index][id]
	return doc, ok
}

// DocCount returns the number of documents stored across every index.
func (s *Server) DocCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, docs := range s.docs {
		n += len(docs)
	}
	return n
}

// Requests returns every non-bulk request received so far, in order.
func (s *Server) Requests() []Request {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Request(nil), s.requests...)
}

// DataStreams returns the names of the data streams created, sorted.
func (s *Server) DataStreams() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.dataStreams))
	for n := range s.dataStreams {
		names = append(names, n)
	}
	slices.Sort(names)
	return names
}

// BulkRequests returns how many bulk requests arrived, held or not.
func (s *Server) BulkRequests() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bulkCount
}

// Arrived receives one value for every bulk request as it arrives, before
// any hold or abort applies.
func (s *Server) Arrived() <-chan struct{} { return s.arrived }

// SetItemStatus scripts the status of bulk items: fn is called with the
// item's index, id and attempt number (1 for the first time this id is
// seen) and returns the status to answer, or 0 for the default behaviour
// (201, or 409 when the id exists). A 2xx status stores the document.
func (s *Server) SetItemStatus(fn func(index, id string, attempt int) int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.itemStatus = fn
}

// SetRequestHook scripts whole responses: when fn returns a non-zero
// status, every request is answered with it and the body instead of being
// handled.
func (s *Server) SetRequestHook(fn func(r *http.Request) (int, string)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requestHook = fn
}

// Hold makes bulk requests block until release is called (or the client
// gives up). Everything else keeps being served.
func (s *Server) Hold() (release func()) {
	ch := make(chan struct{})
	s.mu.Lock()
	s.hold = ch
	s.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			if s.hold == ch {
				s.hold = nil
			}
			s.mu.Unlock()
			close(ch)
		})
	}
}

// AbortConnections drops the connection of the next n bulk requests
// without a response, which the client sees as a transport error.
func (s *Server) AbortConnections(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.abortNext = n
}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Elastic-Product", "Elasticsearch")
	body, err := readBody(r)
	if err != nil {
		esError(w, http.StatusBadRequest, "illegal_argument_exception", err.Error())
		return
	}
	s.mu.Lock()
	hook := s.requestHook
	s.mu.Unlock()
	if hook != nil {
		if status, resp := hook(r); status != 0 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			_, _ = io.WriteString(w, resp)
			return
		}
	}
	path := r.URL.Path
	switch {
	case path == "/" && (r.Method == http.MethodGet || r.Method == http.MethodHead):
		writeJSON(w, http.StatusOK, map[string]any{
			"name": "fake", "cluster_name": "fake",
			"version": map[string]any{"number": "9.0.0"},
			"tagline": "You Know, for Search",
		})
	case path == "/_bulk" || strings.HasSuffix(path, "/_bulk"):
		s.bulk(w, r, body)
	case r.Method == http.MethodPut && (strings.HasPrefix(path, "/_ilm/policy/") ||
		strings.HasPrefix(path, "/_component_template/") || strings.HasPrefix(path, "/_index_template/")):
		s.record(r, body)
		writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
	case strings.HasPrefix(path, "/_data_stream/"):
		s.record(r, body)
		s.dataStream(w, r, strings.TrimPrefix(path, "/_data_stream/"))
	default:
		esError(w, http.StatusNotFound, "resource_not_found_exception", "no handler for "+r.Method+" "+path)
	}
}

func (s *Server) record(r *http.Request, body []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, Request{Method: r.Method, Path: r.URL.Path, Body: body})
}

func (s *Server) dataStream(w http.ResponseWriter, r *http.Request, name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	exists := s.dataStreams[name]
	switch r.Method {
	case http.MethodGet:
		if !exists {
			esError(w, http.StatusNotFound, "index_not_found_exception", "no such index ["+name+"]")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"data_streams": []any{map[string]any{"name": name}}})
	case http.MethodPut:
		if exists {
			esError(w, http.StatusBadRequest, "resource_already_exists_exception", "data_stream ["+name+"] already exists")
			return
		}
		s.dataStreams[name] = true
		writeJSON(w, http.StatusOK, map[string]any{"acknowledged": true})
	default:
		esError(w, http.StatusMethodNotAllowed, "illegal_argument_exception", "unsupported method "+r.Method)
	}
}

// bulkAction is the action line of a bulk item.
type bulkAction struct {
	Create *bulkTarget `json:"create"`
	Index  *bulkTarget `json:"index"`
}

type bulkTarget struct {
	Index string `json:"_index"`
	ID    string `json:"_id"`
}

func (s *Server) bulk(w http.ResponseWriter, r *http.Request, body []byte) {
	s.mu.Lock()
	s.bulkCount++
	hold := s.hold
	abort := s.abortNext > 0
	if abort {
		s.abortNext--
	}
	s.mu.Unlock()
	select {
	case s.arrived <- struct{}{}:
	default:
	}
	if abort {
		panic(http.ErrAbortHandler)
	}
	if hold != nil {
		select {
		case <-hold:
		case <-r.Context().Done():
			return
		}
	}

	lines := bytes.Split(body, []byte("\n"))
	var items []any
	anyErr := false
	for i := 0; i < len(lines); i++ {
		line := bytes.TrimSpace(lines[i])
		if len(line) == 0 {
			continue
		}
		var action bulkAction
		if err := json.Unmarshal(line, &action); err != nil {
			esError(w, http.StatusBadRequest, "illegal_argument_exception", "malformed action line: "+err.Error())
			return
		}
		target, name := action.Create, "create"
		if target == nil {
			target, name = action.Index, "index"
		}
		if target == nil {
			esError(w, http.StatusBadRequest, "illegal_argument_exception", "unknown action in "+string(line))
			return
		}
		i++
		if i >= len(lines) {
			esError(w, http.StatusBadRequest, "illegal_argument_exception", "action without source")
			return
		}
		source := bytes.TrimSpace(lines[i])
		if !json.Valid(source) {
			esError(w, http.StatusBadRequest, "illegal_argument_exception", "source is not JSON")
			return
		}
		status := s.store(target.Index, target.ID, source)
		item := map[string]any{"_index": target.Index, "_id": target.ID, "status": status}
		switch {
		case status >= 200 && status < 300:
			item["result"] = "created"
		default:
			anyErr = true
			item["error"] = itemError(status, target.ID)
		}
		items = append(items, map[string]any{name: item})
	}
	writeJSON(w, http.StatusOK, map[string]any{"took": 1, "errors": anyErr, "items": items})
}

// store applies the scripted or default status for one item and keeps the
// document when it is accepted.
func (s *Server) store(index, id string, source []byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := index + "/" + id
	s.attempts[key]++
	_, exists := s.docs[index][id]
	status := 0
	if s.itemStatus != nil {
		status = s.itemStatus(index, id, s.attempts[key])
	}
	if status == 0 {
		if exists {
			return http.StatusConflict
		}
		status = http.StatusCreated
	}
	if status >= 200 && status < 300 {
		if s.docs[index] == nil {
			s.docs[index] = map[string]json.RawMessage{}
		}
		s.docs[index][id] = append(json.RawMessage(nil), source...)
	}
	return status
}

// itemError renders the error object Elasticsearch would attach to an item
// with the given status.
func itemError(status int, id string) map[string]any {
	typ, reason := "exception", fmt.Sprintf("simulated failure with status %d", status)
	switch status {
	case http.StatusConflict:
		typ, reason = "version_conflict_engine_exception", "["+id+"]: version conflict, document already exists (current version [1])"
	case http.StatusTooManyRequests:
		typ, reason = "es_rejected_execution_exception", "rejected execution of coordinating operation"
	case http.StatusBadRequest:
		typ, reason = "document_parsing_exception", "[1:1] failed to parse field"
	case http.StatusServiceUnavailable:
		typ, reason = "unavailable_shards_exception", "primary shard is not active"
	case http.StatusInternalServerError:
		typ, reason = "internal_server_error", "simulated internal error"
	}
	return map[string]any{"type": typ, "reason": reason}
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, nil
	}
	var rd io.Reader = r.Body
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, fmt.Errorf("gzip body: %w", err)
		}
		defer gz.Close()
		rd = gz
	}
	return io.ReadAll(rd)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func esError(w http.ResponseWriter, status int, typ, reason string) {
	writeJSON(w, status, map[string]any{
		"error":  map[string]any{"type": typ, "reason": reason},
		"status": status,
	})
}
