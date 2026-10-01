// Package sampling decides which requests Shadowtap captures.
//
// Decisions hash the request's trace id, so they are deterministic and nested:
// every sidecar that samples at a rate of at least r makes the same choice for a
// given trace, and a sampled request is captured at every hop without any
// coordination between taps.
package sampling

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// Decision reasons.
const (
	ReasonSampled     = "sampled"
	ReasonUnsampled   = "unsampled"
	ReasonExcluded    = "excluded"
	ReasonRateLimited = "rate_limited"
)

// Rule names reported for decisions not made by a configured rule.
const (
	RuleDefault     = "default"
	RuleUpgrade     = "upgrade"
	RuleExcludePath = "exclude_path"
)

// Key sources: which value the sampling hash was computed from.
const (
	KeySourceTraceparent = "traceparent"
	KeySourceB3          = "b3"
	KeySourceB3Multi     = "x-b3"
	KeySourceRequestID   = "x-request-id"
	KeySourceRandom      = "random"
)

// Config controls sampling.
type Config struct {
	// DefaultRate applies when no rule matches, in [0, 1].
	DefaultRate float64 `json:"default_rate"`
	// MaxPerSecond caps sampled requests per tap instance; 0 means unlimited.
	MaxPerSecond float64 `json:"max_per_second"`
	// Burst is the token bucket size; defaults to ceil(MaxPerSecond).
	Burst int `json:"burst"`
	// ExcludePaths are paths (without query) that are never captured.
	ExcludePaths []string `json:"exclude_paths"`
	// Rules are evaluated in order; the first match sets the rate.
	Rules []Rule `json:"rules"`
	// Salt is mixed into the hash. Every tap must use the same salt for
	// decisions to stay consistent across services.
	Salt string `json:"salt"`
}

// Rule sets the sampling rate for matching requests. Every non-empty condition
// must match; a rule without conditions matches everything.
type Rule struct {
	Name       string   `json:"name"`
	Services   []string `json:"services,omitempty"`
	Methods    []string `json:"methods,omitempty"`
	PathPrefix string   `json:"path_prefix,omitempty"`
	PathRegex  string   `json:"path_regex,omitempty"`
	Rate       float64  `json:"rate"`
}

// Request is what the sampler looks at.
type Request struct {
	Service string
	Method  string
	Path    string // HTTP :path; a query string is ignored
	// Header returns the first value of a lowercase header name, or "".
	Header func(name string) string
}

// Decision is the sampling outcome for one request.
type Decision struct {
	Sampled   bool
	Reason    string  // Reason* constant
	Rule      string  // matched rule name or a Rule* constant
	Rate      float64 // rate that applied
	KeySource string  // KeySource* constant; empty for exclusions
}

// DefaultConfig samples 1% of traffic and skips health and metrics endpoints.
func DefaultConfig() Config {
	return Config{
		DefaultRate:  0.01,
		ExcludePaths: []string{"/healthz", "/readyz", "/livez", "/metrics"},
	}
}

// Validate checks rates, rule names and regular expressions.
func (c Config) Validate() error {
	var errs []error
	if !validRate(c.DefaultRate) {
		errs = append(errs, fmt.Errorf("default_rate %v not in [0, 1]", c.DefaultRate))
	}
	if c.MaxPerSecond < 0 {
		errs = append(errs, fmt.Errorf("max_per_second %v is negative", c.MaxPerSecond))
	}
	if c.Burst < 0 {
		errs = append(errs, fmt.Errorf("burst %d is negative", c.Burst))
	}
	for _, p := range c.ExcludePaths {
		if !strings.HasPrefix(p, "/") {
			errs = append(errs, fmt.Errorf("exclude path %q must start with /", p))
		}
	}
	seen := map[string]bool{}
	for i, r := range c.Rules {
		switch {
		case r.Name == "":
			errs = append(errs, fmt.Errorf("rule %d has no name", i))
		case seen[r.Name]:
			errs = append(errs, fmt.Errorf("duplicate rule name %q", r.Name))
		}
		seen[r.Name] = true
		if !validRate(r.Rate) {
			errs = append(errs, fmt.Errorf("rule %q: rate %v not in [0, 1]", r.Name, r.Rate))
		}
		if r.PathRegex != "" {
			if _, err := regexp.Compile(r.PathRegex); err != nil {
				errs = append(errs, fmt.Errorf("rule %q: path_regex: %w", r.Name, err))
			}
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("sampling: %w", err)
	}
	return nil
}

func validRate(r float64) bool { return r >= 0 && r <= 1 }
