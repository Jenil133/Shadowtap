// Package request rebuilds the HTTP request of a capture for a replay
// target: the recorded method, path and headers, minus what only made sense
// on the original hop, plus markers that identify the replay.
package request

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path"
	"strings"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// Replay marker headers.
const (
	HeaderRun             = "x-shadowtap-replay-run"
	HeaderCaptureID       = "x-shadowtap-capture-id"
	HeaderOriginalTraceID = "x-shadowtap-original-trace-id"
)

// ErrBodyUnavailable is returned for a capture whose request body was not
// kept in full: replaying it would send a different request.
var ErrBodyUnavailable = errors.New("request: the capture's request body was not kept in full")

// hopByHop are connection-level headers (RFC 9110 section 7.6.1) that must
// not be forwarded.
var hopByHop = map[string]bool{
	"connection": true, "keep-alive": true, "proxy-connection": true, "transfer-encoding": true,
	"te": true, "trailer": true, "upgrade": true, "proxy-authenticate": true,
}

// stripped are headers that belong to the original hop or the original
// trace: the target gets fresh ones.
var stripped = map[string]bool{
	"host": true, "content-length": true, "x-forwarded-client-cert": true, "x-request-id": true,
	"traceparent": true, "tracestate": true, "b3": true, "x-b3-traceid": true, "x-b3-spanid": true,
	"x-b3-parentspanid": true, "x-b3-sampled": true, "x-b3-flags": true, "x-ot-span-context": true,
	"x-cloud-trace-context": true, "grpc-trace-bin": true,
	HeaderRun: true, HeaderCaptureID: true, HeaderOriginalTraceID: true,
}

// Options adjusts the rebuilt requests.
type Options struct {
	// RunID goes into HeaderRun.
	RunID string
	// SetHeaders replace (or add) headers after the captured ones are
	// copied: typically a test credential for the candidate, since captured
	// credentials are tokenized.
	SetHeaders [][2]string
	// DropHeaders removes captured headers (case-insensitive).
	DropHeaders []string
	// NewTraceparent returns the traceparent of a replayed request;
	// defaults to a random sampled W3C traceparent.
	NewTraceparent func() string
}

// Built is a rebuilt request and what was set on it.
type Built struct {
	Request *http.Request
	// TraceID is the trace id of the replayed request's traceparent.
	TraceID string
}

// Build returns the request to send to target for x. target is a validated
// base URL (see ValidateTarget); the capture's path and query are appended
// to its path.
func Build(ctx context.Context, target *url.URL, x *capture.Exchange, opts Options) (Built, error) {
	b := x.Request.Body
	if b.Size > 0 && (b.Omitted != "" || b.Truncated) {
		return Built{}, ErrBodyUnavailable
	}
	body, err := b.Bytes()
	if err != nil {
		return Built{}, fmt.Errorf("request: capture %s: %w", x.ID, err)
	}
	u, err := joinURL(target, x.Request.Path)
	if err != nil {
		return Built{}, fmt.Errorf("request: capture %s: %w", x.ID, err)
	}
	req, err := http.NewRequestWithContext(ctx, x.Request.Method, u.String(), bytes.NewReader(body))
	if err != nil {
		return Built{}, fmt.Errorf("request: capture %s: %w", x.ID, err)
	}
	if len(body) == 0 {
		req.Body, req.ContentLength = http.NoBody, 0
	}

	drop := make(map[string]bool, len(opts.DropHeaders))
	for _, h := range opts.DropHeaders {
		drop[strings.ToLower(strings.TrimSpace(h))] = true
	}
	// Tokens listed in Connection are hop-by-hop too.
	connTokens := map[string]bool{}
	for _, v := range x.Request.Headers["connection"] {
		for _, tok := range strings.Split(v, ",") {
			connTokens[strings.ToLower(strings.TrimSpace(tok))] = true
		}
	}
	for name, values := range x.Request.Headers {
		switch {
		case hopByHop[name], stripped[name], connTokens[name], drop[name],
			strings.HasPrefix(name, "x-envoy-"), strings.HasPrefix(name, ":"):
			continue
		case name == "content-encoding" && b.DecodedFrom != "":
			// The stored body is decoded; send it as such.
			continue
		}
		for _, v := range values {
			req.Header.Add(name, v)
		}
	}
	for _, kv := range opts.SetHeaders {
		req.Header.Set(kv[0], kv[1])
	}

	tp := randomTraceparent
	if opts.NewTraceparent != nil {
		tp = opts.NewTraceparent
	}
	traceparent := tp()
	req.Header.Set("traceparent", traceparent)
	if opts.RunID != "" {
		req.Header.Set(HeaderRun, opts.RunID)
	}
	req.Header.Set(HeaderCaptureID, x.ID)
	if x.TraceID != "" {
		req.Header.Set(HeaderOriginalTraceID, x.TraceID)
	}
	built := Built{Request: req}
	if parts := strings.Split(traceparent, "-"); len(parts) == 4 {
		built.TraceID = parts[1]
	}
	return built, nil
}

// joinURL appends a captured path (already escaped, with its query) to the
// target's base path.
func joinURL(target *url.URL, captured string) (*url.URL, error) {
	if !strings.HasPrefix(captured, "/") {
		return nil, fmt.Errorf("path %q is not absolute", captured)
	}
	p, err := url.ParseRequestURI(captured)
	if err != nil {
		return nil, fmt.Errorf("path: %w", err)
	}
	u := *target
	basePath := strings.TrimSuffix(target.EscapedPath(), "/")
	u.Path = strings.TrimSuffix(target.Path, "/") + p.Path
	u.RawPath = ""
	if raw := basePath + p.EscapedPath(); raw != u.EscapedPath() {
		u.RawPath = raw // keep the captured escaping (e.g. %2F) byte for byte
	}
	u.RawQuery = p.RawQuery
	return &u, nil
}

// randomTraceparent returns a sampled W3C traceparent with random ids.
func randomTraceparent() string {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err) // crypto/rand never fails on supported platforms
	}
	b[0] |= 1  // never an all-zero trace id
	b[16] |= 1 // nor span id
	return "00-" + hex.EncodeToString(b[:16]) + "-" + hex.EncodeToString(b[16:]) + "-01"
}

// ValidateTarget parses a replay target and checks it is allowed. A target
// is an http(s) base URL without query, fragment or credentials. Without
// allow patterns only hosts that are clearly not production are accepted:
// loopback, localhost, single-label names (compose services) and Kubernetes
// service names (*.svc, *.svc.cluster.local). allow lists host patterns
// (path.Match syntax, matched against the host name and against host:port)
// that are accepted too. A host with a "prod" or "production" label is
// refused unless a pattern without wildcards names it exactly.
func ValidateTarget(raw string, allow []string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("request: target %q: %w", raw, err)
	}
	switch {
	case u.Scheme != "http" && u.Scheme != "https":
		return nil, fmt.Errorf("request: target %q must be an http or https URL", raw)
	case u.Host == "" || u.Hostname() == "":
		return nil, fmt.Errorf("request: target %q has no host", raw)
	case u.User != nil:
		return nil, fmt.Errorf("request: target must not carry credentials; use a header override")
	case u.RawQuery != "" || u.ForceQuery || u.Fragment != "":
		return nil, fmt.Errorf("request: target %q must not have a query or fragment", raw)
	}
	host := strings.ToLower(u.Hostname())
	for _, p := range allow {
		p = strings.ToLower(strings.TrimSpace(p))
		if p == host || p == strings.ToLower(u.Host) {
			return u, nil // named exactly: the operator means it
		}
	}
	if looksProduction(host) {
		return nil, fmt.Errorf("request: target host %s looks like production; name it exactly in the allowed targets to replay to it", host)
	}
	if safeByDefault(host) {
		return u, nil
	}
	for _, p := range allow {
		p = strings.ToLower(strings.TrimSpace(p))
		if ok, _ := path.Match(p, host); ok {
			return u, nil
		}
		if ok, _ := path.Match(p, strings.ToLower(u.Host)); ok {
			return u, nil
		}
	}
	return nil, fmt.Errorf("request: target host %s is not allowed; add it to the allowed targets", host)
}

func looksProduction(host string) bool {
	for _, label := range strings.FieldsFunc(host, func(r rune) bool { return r == '.' || r == '-' }) {
		if label == "prod" || label == "production" || label == "prd" {
			return true
		}
	}
	return false
}

func safeByDefault(host string) bool {
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return host == "localhost" || strings.HasSuffix(host, ".localhost") ||
		!strings.Contains(host, ".") ||
		strings.HasSuffix(host, ".svc") || strings.HasSuffix(host, ".svc.cluster.local")
}
