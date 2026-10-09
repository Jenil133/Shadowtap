// Package diff compares a replayed response with the recorded production
// response: status, selected headers, and the body (structurally for JSON,
// by lines for text). Both sides are normalized first: decoded, scrubbed with
// the tap's scrubber and key (so recorded pseudonyms and re-scrubbed
// candidate values agree), and stripped of volatile headers.
//
// With a baseline (a deployment of the current production build that receives
// the same replayed requests), a candidate difference is only reported when
// the baseline disagrees with the candidate at the same place: a difference
// the baseline shares is caused by the replay environment (pseudonymized
// inputs, state), not by the code change.
//
// Noise (timestamps, random ids) is learned the way Diffy does it, from two
// responses of the same code to the same request: safe requests are sent to
// the baseline a second time (a probe), and places where the two baseline
// responses differ in more than the noise threshold of a route's probes are
// suppressed in that route's candidate differences. Differences between the
// baseline and the recording cannot serve: they mix nondeterminism with the
// replay environment, and suppressing them would hide real regressions.
package diff

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	"sigs.k8s.io/yaml"
)

// Defaults.
const (
	DefaultNoiseThreshold = 0.2
	DefaultMaxDifferences = 100
	DefaultMaxValueBytes  = 200
	// MinNoiseSamples is how many noise probes a route needs before its
	// noise statistics are trusted.
	MinNoiseSamples = 3
)

// Config is the diff.yaml file.
type Config struct {
	// IgnoreHeaders are not compared (in addition to the volatile defaults).
	IgnoreHeaders []string `json:"ignore_headers,omitempty"`
	// CompareHeaders are compared exactly (in addition to content-type,
	// location, cache-control and set-cookie names).
	CompareHeaders []string `json:"compare_headers,omitempty"`
	// IgnorePointers are JSON Pointer patterns into the body that are never
	// compared. "*" matches one segment, a trailing "**" any remainder.
	IgnorePointers []string `json:"ignore_pointers,omitempty"`
	// ArrayKeys matches array elements by a key field rather than by index:
	// pointer pattern of the array -> field name.
	ArrayKeys map[string]string `json:"array_keys,omitempty"`
	// UnorderedArrays are compared as multisets.
	UnorderedArrays []string `json:"unordered_arrays,omitempty"`
	// FloatTolerance is the largest relative difference between two numbers
	// that still counts as equal.
	FloatTolerance float64 `json:"float_tolerance,omitempty"`
	// NoiseThreshold is the share of a route's noise probes in which a place
	// must differ to count as noise; 0 means DefaultNoiseThreshold.
	NoiseThreshold float64 `json:"noise_threshold,omitempty"`
	// MaxDifferences caps the differences kept per exchange.
	MaxDifferences int `json:"max_differences,omitempty"`
	// Routes holds per-route ignore rules.
	Routes []RouteRule `json:"routes,omitempty"`
}

// RouteRule applies extra ignores to routes matching Route, a regular
// expression over the route template ("GET /api/users/{id}").
type RouteRule struct {
	Route          string   `json:"route"`
	IgnoreHeaders  []string `json:"ignore_headers,omitempty"`
	IgnorePointers []string `json:"ignore_pointers,omitempty"`
}

// LoadConfig reads a diff.yaml file; unknown fields are rejected.
func LoadConfig(path string) (Config, error) {
	var c Config
	data, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("diff: %w", err)
	}
	if err := yaml.UnmarshalStrict(data, &c); err != nil {
		return c, fmt.Errorf("diff: %s: %w", path, err)
	}
	return c, nil
}

// volatileHeaders change on every response and are never compared.
var volatileHeaders = []string{
	"date", "server", "x-envoy-upstream-service-time", "x-request-id", "content-length", "etag", "age",
	"expires", "last-modified", "x-envoy-decorator-operation", "traceparent", "tracestate",
	"content-encoding", "vary", "transfer-encoding", "connection", "keep-alive", "x-shadowtap-replay-run",
}

// compiled is a validated Config.
type compiled struct {
	ignoreHeaders  map[string]bool
	compareHeaders []string
	ignore         []pattern
	arrayKeys      []arrayKey
	unordered      []pattern
	tolerance      float64
	threshold      float64
	maxDiffs       int
	routes         []compiledRoute
}

type arrayKey struct {
	p     pattern
	field string
}

type compiledRoute struct {
	re            *regexp.Regexp
	ignoreHeaders map[string]bool
	ignore        []pattern
}

func compile(c Config) (*compiled, error) {
	var errs []error
	out := &compiled{
		ignoreHeaders: map[string]bool{},
		tolerance:     c.FloatTolerance,
		threshold:     c.NoiseThreshold,
		maxDiffs:      c.MaxDifferences,
	}
	if out.threshold == 0 {
		out.threshold = DefaultNoiseThreshold
	}
	if out.threshold < 0 || out.threshold > 1 {
		errs = append(errs, fmt.Errorf("noise_threshold %v not in (0, 1]", c.NoiseThreshold))
	}
	if c.FloatTolerance < 0 {
		errs = append(errs, fmt.Errorf("float_tolerance %v is negative", c.FloatTolerance))
	}
	if out.maxDiffs == 0 {
		out.maxDiffs = DefaultMaxDifferences
	}
	if out.maxDiffs < 0 {
		errs = append(errs, fmt.Errorf("max_differences %d is negative", c.MaxDifferences))
	}
	for _, h := range append(volatileHeaders, c.IgnoreHeaders...) {
		out.ignoreHeaders[normHeader(h)] = true
	}
	for _, h := range c.CompareHeaders {
		out.compareHeaders = append(out.compareHeaders, normHeader(h))
	}
	parse := func(field string, list []string) []pattern {
		var ps []pattern
		for _, s := range list {
			p, err := parsePattern(s)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", field, err))
				continue
			}
			ps = append(ps, p)
		}
		return ps
	}
	out.ignore = parse("ignore_pointers", c.IgnorePointers)
	out.unordered = parse("unordered_arrays", c.UnorderedArrays)
	for ptr, field := range c.ArrayKeys {
		p, err := parsePattern(ptr)
		if err != nil {
			errs = append(errs, fmt.Errorf("array_keys: %w", err))
			continue
		}
		if field == "" {
			errs = append(errs, fmt.Errorf("array_keys: %s: empty key field", ptr))
		}
		out.arrayKeys = append(out.arrayKeys, arrayKey{p, field})
	}
	for i, r := range c.Routes {
		re, err := regexp.Compile(r.Route)
		if err != nil {
			errs = append(errs, fmt.Errorf("routes[%d]: %w", i, err))
			continue
		}
		cr := compiledRoute{re: re, ignoreHeaders: map[string]bool{}, ignore: parse(fmt.Sprintf("routes[%d].ignore_pointers", i), r.IgnorePointers)}
		for _, h := range r.IgnoreHeaders {
			cr.ignoreHeaders[normHeader(h)] = true
		}
		out.routes = append(out.routes, cr)
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("diff: invalid config: %w", err)
	}
	return out, nil
}

func normHeader(h string) string { return strings.ToLower(strings.TrimSpace(h)) }

// pattern is a JSON Pointer pattern split into unescaped segments.
type pattern struct {
	segs []string
	rest bool // ends in "**"
}

func parsePattern(s string) (pattern, error) {
	if s == "" {
		return pattern{}, nil // the whole document
	}
	if !strings.HasPrefix(s, "/") {
		return pattern{}, fmt.Errorf("pointer pattern %q must start with /", s)
	}
	segs := splitPointer(s)
	p := pattern{segs: segs}
	if n := len(segs); n > 0 && segs[n-1] == "**" {
		p.segs, p.rest = segs[:n-1], true
	}
	return p, nil
}

func (p pattern) match(segs []string) bool {
	if len(segs) < len(p.segs) || (!p.rest && len(segs) != len(p.segs)) {
		return false
	}
	for i, s := range p.segs {
		if s != "*" && s != segs[i] {
			return false
		}
	}
	return true
}

// splitPointer splits a JSON Pointer into unescaped segments.
func splitPointer(ptr string) []string {
	if ptr == "" {
		return nil
	}
	parts := strings.Split(ptr[1:], "/")
	for i, s := range parts {
		parts[i] = strings.ReplaceAll(strings.ReplaceAll(s, "~1", "/"), "~0", "~")
	}
	return parts
}

// escapeSegment escapes one JSON Pointer segment.
func escapeSegment(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}
