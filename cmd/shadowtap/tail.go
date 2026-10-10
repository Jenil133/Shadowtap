package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/scrub"
)

const maxSummaryPath = 120

type tailOptions struct {
	conn       connFlags
	topic      string
	from       string
	until      string
	timeout    time.Duration
	limit      int
	format     string
	service    string
	traceID    string
	method     string
	status     string
	pathPrefix string
	checkPII   bool
}

// filter selects which captures tail prints.
type filter struct {
	service, traceID, method, pathPrefix string
	status                               int // exact code, or 1-5 for a class like 5xx
	statusClass                          bool
	until                                time.Time
}

func runTail(ctx context.Context, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	var o tailOptions
	fs := flag.NewFlagSet("shadowtap tail", flag.ContinueOnError)
	fs.SetOutput(stderr)
	o.conn.register(fs, getenv)
	fs.StringVar(&o.topic, "topic", capture.DefaultTopic, "captures topic")
	fs.StringVar(&o.from, "from", "latest", "start position: earliest, latest, an RFC3339 time, or a negative duration such as -15m")
	fs.StringVar(&o.until, "until", "", "skip captures that started after this RFC3339 time")
	fs.DurationVar(&o.timeout, "timeout", 0, "exit after no records arrive for this long (0 waits forever)")
	fs.IntVar(&o.limit, "limit", 0, "exit after this many matching captures (0 = no limit)")
	fs.StringVar(&o.format, "format", "summary", "output format: summary, json (one capture per line) or pretty")
	fs.StringVar(&o.service, "service", "", "only captures of this service")
	fs.StringVar(&o.traceID, "trace-id", "", "only captures with this trace id")
	fs.StringVar(&o.method, "method", "", "only this HTTP method")
	fs.StringVar(&o.status, "status", "", "only this status code (201) or class (5xx)")
	fs.StringVar(&o.pathPrefix, "path-prefix", "", "only paths starting with this prefix")
	fs.BoolVar(&o.checkPII, "check-pii", false,
		"audit captures for unscrubbed PII instead of printing them; exit 3 if any is found or if a record cannot be decoded (and so was not audited)")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return exitOK
		}
		return exitUsage
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(stderr, "shadowtap tail: unexpected arguments %q\n", fs.Args())
		return exitUsage
	}
	f, err := o.filter()
	if err != nil {
		fmt.Fprintf(stderr, "shadowtap tail: %v\n", err)
		return exitUsage
	}
	emit, err := printer(o.format, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "shadowtap tail: %v\n", err)
		return exitUsage
	}

	cfg := kafka.DefaultConfig()
	if err := o.conn.apply(&cfg, getenv); err != nil {
		fmt.Fprintf(stderr, "shadowtap tail: %v\n", err)
		return exitUsage
	}
	cfg.Topic = o.topic
	cfg.ClientID = "shadowtap-cli"
	consumer, err := kafka.NewConsumer(cfg, kafka.ConsumeOptions{Start: o.from})
	if err != nil {
		fmt.Fprintf(stderr, "shadowtap tail: %v\n", err)
		return exitError
	}
	defer consumer.Close()

	var (
		records, matched, decodeErrors int
		findings                       = map[string]int{}
	)
	for o.limit == 0 || matched < o.limit {
		msg, err := next(ctx, consumer, o.timeout)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, errIdle) {
				break
			}
			fmt.Fprintf(stderr, "shadowtap tail: %v\n", err)
			return exitError
		}
		records++
		if msg.DecodeErr != nil {
			decodeErrors++
			fmt.Fprintf(stderr, "skipping partition %d offset %d: %v\n", msg.Partition, msg.Offset, msg.DecodeErr)
			continue
		}
		if !f.match(msg.Exchange) {
			continue
		}
		matched++
		if o.checkPII {
			for _, hit := range audit(msg.Exchange) {
				findings[hit.kind]++
				fmt.Fprintf(stdout, "PII capture=%s field=%s kind=%s\n", msg.Exchange.ID, hit.field, hit.kind)
			}
			continue
		}
		if err := emit(msg); err != nil {
			fmt.Fprintf(stderr, "shadowtap tail: %v\n", err)
			return exitError
		}
	}

	fmt.Fprintf(stderr, "%d records read, %d matched, %d undecodable\n", records, matched, decodeErrors)
	if o.checkPII {
		total := 0
		for _, n := range findings {
			total += n
		}
		if total > 0 {
			fmt.Fprintf(stderr, "PII audit FAILED: %d findings (%s) in %d captures\n", total, formatCounts(findings), matched)
			return exitPII
		}
		if decodeErrors > 0 {
			// A record that cannot be decoded cannot be audited, so a clean
			// verdict would be unfounded.
			fmt.Fprintf(stderr, "PII audit FAILED: %d records could not be decoded and were not audited\n", decodeErrors)
			return exitPII
		}
		fmt.Fprintf(stderr, "PII audit passed: %d captures, no unscrubbed personal data found\n", matched)
	}
	return exitOK
}

var errIdle = errors.New("idle timeout")

// next waits for the next record, giving up with errIdle after idle when idle
// is positive.
func next(ctx context.Context, c *kafka.Consumer, idle time.Duration) (kafka.Message, error) {
	if idle <= 0 {
		return c.Next(ctx)
	}
	nctx, cancel := context.WithTimeout(ctx, idle)
	defer cancel()
	msg, err := c.Next(nctx)
	if err != nil && ctx.Err() == nil && nctx.Err() != nil {
		return msg, errIdle
	}
	return msg, err
}

func (o tailOptions) filter() (filter, error) {
	f := filter{service: o.service, traceID: strings.ToLower(o.traceID), method: strings.ToUpper(o.method), pathPrefix: o.pathPrefix}
	if o.until != "" {
		t, err := time.Parse(time.RFC3339Nano, o.until)
		if err != nil {
			return f, fmt.Errorf("--until: %w", err)
		}
		f.until = t
	}
	switch s := strings.ToLower(o.status); {
	case s == "":
	case len(s) == 3 && s[1:] == "xx" && s[0] >= '1' && s[0] <= '5':
		f.status, f.statusClass = int(s[0]-'0'), true
	default:
		code, err := strconv.Atoi(s)
		if err != nil || code < 100 || code > 999 {
			return f, fmt.Errorf("--status %q must be a code like 201 or a class like 5xx", o.status)
		}
		f.status = code
	}
	return f, nil
}

func (f filter) match(x *capture.Exchange) bool {
	switch {
	case f.service != "" && x.Service != f.service:
		return false
	case f.traceID != "" && x.TraceID != f.traceID:
		return false
	case f.method != "" && x.Request.Method != f.method:
		return false
	case f.pathPrefix != "" && !strings.HasPrefix(x.Request.Path, f.pathPrefix):
		return false
	case !f.until.IsZero() && x.Timing.StartedAt.After(f.until):
		return false
	case f.status != 0 && f.statusClass && x.Response.Status/100 != f.status:
		return false
	case f.status != 0 && !f.statusClass && x.Response.Status != f.status:
		return false
	}
	return true
}

func printer(format string, w io.Writer) (func(kafka.Message) error, error) {
	switch format {
	case "summary":
		return func(m kafka.Message) error {
			_, err := fmt.Fprintln(w, summary(m.Exchange))
			return err
		}, nil
	case "json":
		// The record value is already compact JSON: print it untouched.
		return func(m kafka.Message) error {
			_, err := fmt.Fprintf(w, "%s\n", bytes.TrimSpace(m.Raw))
			return err
		}, nil
	case "pretty":
		return func(m kafka.Message) error {
			enc := json.NewEncoder(w)
			enc.SetEscapeHTML(false)
			enc.SetIndent("", "  ")
			return enc.Encode(m.Exchange)
		}, nil
	}
	return nil, fmt.Errorf("--format %q must be summary, json or pretty", format)
}

func summary(x *capture.Exchange) string {
	path := x.Request.Path
	if len(path) > maxSummaryPath {
		path = path[:maxSummaryPath] + "…"
	}
	var b strings.Builder
	// The first duration is time to response headers (duration_us); the
	// total, including the response body, follows when it is known.
	fmt.Fprintf(&b, "%s %s %s %s %s -> %d %.1fms",
		x.Timing.StartedAt.UTC().Format("2006-01-02T15:04:05.000Z"),
		x.Service, x.Source.Mode, x.Request.Method, path, x.Response.Status,
		float64(x.Timing.DurationMicros)/1000)
	if total := x.Timing.TotalMicros(); total > 0 {
		fmt.Fprintf(&b, " total=%.1fms", float64(total)/1000)
	}
	if x.TraceID != "" {
		fmt.Fprintf(&b, " trace=%s", x.TraceID)
	}
	if len(x.Redactions) > 0 {
		fmt.Fprintf(&b, " redactions=%s", formatCounts(x.Redactions))
	}
	for _, body := range []struct {
		name string
		b    capture.Body
	}{{"req", x.Request.Body}, {"resp", x.Response.Body}} {
		if body.b.Omitted != "" {
			fmt.Fprintf(&b, " %s_body=omitted:%s", body.name, body.b.Omitted)
		} else if body.b.Truncated {
			fmt.Fprintf(&b, " %s_body=truncated", body.name)
		}
	}
	return b.String()
}

type hit struct{ field, kind string }

// audit reports detectable PII left anywhere in a capture, including PII
// hidden behind URL or JSON escaping. It returns field names and kinds only,
// never the values.
func audit(x *capture.Exchange) []hit {
	var hits []hit
	report := func(field string, matches []scrub.Match) {
		for _, m := range matches {
			hits = append(hits, hit{field: field, kind: m.Kind})
		}
	}
	checkHeaders := func(prefix string, h capture.Headers) {
		for _, name := range slices.Sorted(maps.Keys(h)) {
			for _, v := range h[name] {
				report(prefix+"."+name, detect(v))
			}
		}
	}
	checkBody := func(field string, b capture.Body) {
		data, err := b.Bytes()
		if err != nil {
			hits = append(hits, hit{field: field, kind: "undecodable"})
			return
		}
		best := detect(string(data))
		var inTokens []scrub.Match
		for _, tok := range jsonStrings(data) {
			inTokens = append(inTokens, detect(tok)...)
		}
		if len(inTokens) > len(best) {
			best = inTokens
		}
		report(field, best)
	}
	report("request.path", detect(x.Request.Path))
	report("request.authority", detect(x.Request.Authority))
	checkHeaders("request.headers", x.Request.Headers)
	checkHeaders("request.trailers", x.Request.Trailers)
	checkBody("request.body", x.Request.Body)
	checkHeaders("response.headers", x.Response.Headers)
	checkHeaders("response.trailers", x.Response.Trailers)
	checkBody("response.body", x.Response.Body)
	return hits
}

// detect runs the detectors over s and its URL-unescaped forms and returns the
// matches of whichever view exposes the most, so escaping cannot hide a value
// and a value is not counted twice.
func detect(s string) []scrub.Match {
	best := scrub.Detect(s)
	for _, v := range decodedViews(s)[1:] {
		if m := scrub.Detect(v); len(m) > len(best) {
			best = m
		}
	}
	return best
}

// decodedViews returns s followed by its URL-unescaped forms when they differ.
func decodedViews(s string) []string {
	views := []string{s}
	for _, unescape := range []func(string) (string, error){url.PathUnescape, url.QueryUnescape} {
		if u, err := unescape(s); err == nil && !slices.Contains(views, u) {
			views = append(views, u)
		}
	}
	return views
}

// jsonStrings returns the decoded string tokens of JSON or NDJSON data, so
// escapes such as \u0040 cannot hide a value from the detectors.
func jsonStrings(data []byte) []string {
	var out []string
	dec := json.NewDecoder(bytes.NewReader(data))
	for {
		tok, err := dec.Token()
		if err != nil {
			return out
		}
		if s, ok := tok.(string); ok {
			out = append(out, s)
		}
	}
}

func formatCounts(m map[string]int) string {
	parts := make([]string, 0, len(m))
	for _, k := range slices.Sorted(maps.Keys(m)) {
		parts = append(parts, fmt.Sprintf("%s:%d", k, m[k]))
	}
	return strings.Join(parts, ",")
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
