// Package tracequery answers "what happened to this request": it reads the
// spans, logs, captures and diffs the indexer wrote to Elasticsearch for one
// trace id, builds the span tree and renders it as a waterfall or JSON. It
// backs the shadowtap trace command.
package tracequery

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/elastic/elastic-transport-go/v8/elastictransport"
	elasticsearch "github.com/elastic/go-elasticsearch/v9"
)

// Defaults.
const (
	DefaultURL     = "http://localhost:9200"
	DefaultTimeout = 15 * time.Second

	// maxSpans and maxLogs bound one trace; Elasticsearch refuses larger
	// result windows without scrolling, and a trace this large is not
	// readable as a waterfall anyway.
	maxSpans    = 10_000
	maxLogs     = 10_000
	maxCaptures = 100
	maxDiffs    = 100
	// maxRelated bounds how many linked traces FollowReplay fetches.
	maxRelated = 20
)

// ErrNotFound is returned when nothing is indexed for the trace, request or
// capture id.
var ErrNotFound = errors.New("not found")

// UnreachableError reports that Elasticsearch could not be reached at all, as
// opposed to answering with an error.
type UnreachableError struct {
	URL string
	Err error
}

func (e *UnreachableError) Error() string {
	return fmt.Sprintf("elasticsearch at %s is unreachable: %v", e.URL, e.Err)
}

func (e *UnreachableError) Unwrap() error { return e.Err }

// Config connects a Client to Elasticsearch.
type Config struct {
	// URL is one or more comma-separated node URLs; empty means DefaultURL.
	URL string
	// Username and Password enable basic authentication.
	Username string
	Password string
	// APIKey is the base64 "id:api_key" token Elasticsearch issues; it wins
	// over Username and Password.
	APIKey string
	// CAFile is a PEM bundle to verify the server certificate with instead of
	// the system roots.
	CAFile string
	// Insecure skips server certificate verification (testing only).
	Insecure bool
	// Timeout bounds each query; 0 means DefaultTimeout.
	Timeout time.Duration
	// Transport replaces the HTTP transport (and so CAFile and Insecure);
	// tests use it.
	Transport http.RoundTripper
}

// Client queries Elasticsearch.
type Client struct {
	es      *elasticsearch.Client
	url     string
	timeout time.Duration
}

// New returns a Client. It does not contact Elasticsearch.
func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.URL) == "" {
		cfg.URL = DefaultURL
	}
	var addrs []string
	for _, a := range strings.Split(cfg.URL, ",") {
		a = strings.TrimSpace(a)
		if a == "" {
			continue
		}
		u, err := url.Parse(a)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			return nil, fmt.Errorf("tracequery: elasticsearch url %q must be http(s)://host[:port]", a)
		}
		addrs = append(addrs, a)
	}
	if len(addrs) == 0 {
		return nil, errors.New("tracequery: no elasticsearch url")
	}
	rt := cfg.Transport
	if rt == nil {
		var err error
		if rt, err = transport(cfg); err != nil {
			return nil, err
		}
	}
	opts := []elasticsearch.Option{
		elasticsearch.WithAddresses(addrs...),
		elasticsearch.WithTransportOptions(elastictransport.WithTransport(rt)),
		// A CLI should fail fast; the caller retries by running again.
		elasticsearch.WithRetry(0),
		elasticsearch.WithDisableMetaHeader(),
	}
	switch {
	case cfg.APIKey != "":
		opts = append(opts, elasticsearch.WithAPIKey(cfg.APIKey))
	case cfg.Username != "" || cfg.Password != "":
		opts = append(opts, elasticsearch.WithBasicAuth(cfg.Username, cfg.Password))
	}
	es, err := elasticsearch.New(opts...)
	if err != nil {
		return nil, fmt.Errorf("tracequery: elasticsearch client: %w", err)
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = DefaultTimeout
	}
	return &Client{es: es, url: addrs[0], timeout: timeout}, nil
}

func transport(cfg Config) (http.RoundTripper, error) {
	tr, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, errors.New("tracequery: default transport is not an *http.Transport")
	}
	tr = tr.Clone()
	tr.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.Insecure} //nolint:gosec // opt-in flag
	if cfg.CAFile != "" {
		pem, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("tracequery: read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("tracequery: %s holds no PEM certificates", cfg.CAFile)
		}
		tr.TLSClientConfig.RootCAs = pool
	}
	return tr, nil
}

// URL is the first configured node URL, for messages.
func (c *Client) URL() string { return c.url }

// search is one entry of a multi-search.
type search struct {
	index string
	body  map[string]any
}

// hits is the result of one search.
type hits struct {
	docs  []doc
	total int64
}

// msearch runs the searches as one _msearch request and returns their hits in
// order. An index that does not exist yet yields no hits rather than an
// error, so a fresh cluster answers "not found" instead of failing.
func (c *Client) msearch(ctx context.Context, searches []search) ([]hits, error) {
	if len(searches) == 0 {
		return nil, nil
	}
	var body bytes.Buffer
	enc := json.NewEncoder(&body)
	for _, s := range searches {
		header := map[string]any{"index": s.index, "ignore_unavailable": true, "allow_no_indices": true}
		if err := enc.Encode(header); err != nil {
			return nil, fmt.Errorf("tracequery: encode msearch: %w", err)
		}
		if err := enc.Encode(s.body); err != nil {
			return nil, fmt.Errorf("tracequery: encode msearch: %w", err)
		}
	}
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	res, err := c.es.Msearch(&body, c.es.Msearch.WithContext(ctx))
	if err != nil {
		return nil, c.transportError(ctx, err)
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(res.Body, 256<<20))
	if err != nil {
		return nil, fmt.Errorf("tracequery: read msearch response: %w", err)
	}
	if res.IsError() {
		return nil, responseError(res.StatusCode, raw)
	}

	var parsed struct {
		Responses []struct {
			Status int             `json:"status"`
			Error  json.RawMessage `json:"error"`
			Hits   struct {
				Total struct {
					Value int64 `json:"value"`
				} `json:"total"`
				Hits []struct {
					Source json.RawMessage `json:"_source"`
				} `json:"hits"`
			} `json:"hits"`
		} `json:"responses"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("tracequery: elasticsearch answered with something other than an msearch response: %w", err)
	}
	if len(parsed.Responses) != len(searches) {
		return nil, fmt.Errorf("tracequery: msearch returned %d responses for %d searches", len(parsed.Responses), len(searches))
	}
	out := make([]hits, len(searches))
	for i, r := range parsed.Responses {
		if len(r.Error) > 0 {
			if esErrorType(r.Error) == "index_not_found_exception" {
				continue
			}
			return nil, fmt.Errorf("tracequery: search %s: %s", searches[i].index, esErrorReason(r.Error, r.Status))
		}
		out[i].total = r.Hits.Total.Value
		for _, h := range r.Hits.Hits {
			d, err := decodeDoc(h.Source)
			if err != nil {
				return nil, fmt.Errorf("tracequery: search %s: %w", searches[i].index, err)
			}
			out[i].docs = append(out[i].docs, d)
		}
	}
	return out, nil
}

func decodeDoc(raw json.RawMessage) (doc, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var d doc
	if err := dec.Decode(&d); err != nil {
		return nil, fmt.Errorf("decode document: %w", err)
	}
	return d, nil
}

// notElasticsearch is the text of the error the client returns when the
// first response lacks the X-Elastic-Product header. The library exports no
// sentinel for it, so it is recognized by its message.
const notElasticsearch = "is not Elasticsearch"

// transportError explains a failure to get any response.
func (c *Client) transportError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		return &UnreachableError{URL: c.url, Err: fmt.Errorf("no answer within %s", c.timeout)}
	case errors.Is(err, context.Canceled):
		return err
	case strings.Contains(err.Error(), notElasticsearch):
		return fmt.Errorf("tracequery: %s answered without the X-Elastic-Product header, so it is not Elasticsearch (a proxy or the wrong port?): %w", c.url, err)
	}
	var netErr net.Error
	if errors.As(err, &netErr) || errors.Is(err, io.EOF) || isDialError(err) {
		return &UnreachableError{URL: c.url, Err: err}
	}
	return fmt.Errorf("tracequery: elasticsearch request: %w", err)
}

func isDialError(err error) bool {
	var opErr *net.OpError
	if errors.As(err, &opErr) {
		return true
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr)
}

// responseError explains a non-2xx answer to the whole request.
func responseError(status int, raw []byte) error {
	switch status {
	case http.StatusUnauthorized:
		return fmt.Errorf("tracequery: elasticsearch rejected the credentials (401); set --es-username/--es-password or --es-api-key")
	case http.StatusForbidden:
		return fmt.Errorf("tracequery: elasticsearch denied the search (403): the credentials lack read access to the shadowtap indices")
	}
	var body struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(raw, &body); err == nil && len(body.Error) > 0 {
		return fmt.Errorf("tracequery: elasticsearch: %s", esErrorReason(body.Error, status))
	}
	text := strings.TrimSpace(string(raw))
	if len(text) > 200 {
		text = text[:200] + "..."
	}
	return fmt.Errorf("tracequery: elasticsearch answered %d: %s", status, text)
}

func esErrorType(raw json.RawMessage) string {
	var e struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(raw, &e)
	return e.Type
}

func esErrorReason(raw json.RawMessage, status int) string {
	var e struct {
		Type   string `json:"type"`
		Reason string `json:"reason"`
	}
	if err := json.Unmarshal(raw, &e); err != nil || e.Type == "" && e.Reason == "" {
		var s string
		if json.Unmarshal(raw, &s) == nil && s != "" {
			return fmt.Sprintf("%s (status %d)", s, status)
		}
		return fmt.Sprintf("error status %d", status)
	}
	if e.Reason == "" {
		return fmt.Sprintf("%s (status %d)", e.Type, status)
	}
	return fmt.Sprintf("%s: %s (status %d)", e.Type, e.Reason, status)
}
