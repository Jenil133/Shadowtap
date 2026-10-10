package source

import (
	"fmt"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// Reasons a filter skips a capture.
const (
	SkipService         = "service"
	SkipMethod          = "method"
	SkipMutating        = "mutating_method"
	SkipPath            = "path"
	SkipStatus          = "status"
	SkipBodyUnavailable = "body_unavailable"
)

// SafeMethods are replayed by default: they should not change state on the
// target.
var SafeMethods = []string{"GET", "HEAD"}

// Filter selects the captures a replay sends.
type Filter struct {
	// Services keeps only these services; empty keeps all.
	Services []string
	// Methods keeps only these methods (case-insensitive); empty means
	// SafeMethods, or every method when AllowMutating is set.
	Methods []string
	// AllowMutating permits methods other than GET, HEAD and OPTIONS. Without
	// it a Methods list naming POST is rejected by Validate.
	AllowMutating bool
	// PathRegex keeps only paths (without the query) it matches.
	PathRegex *regexp.Regexp
	// Status keeps only these recorded statuses: codes ("200") or classes
	// ("2xx"); empty keeps all.
	Status []string
}

// Validate reports a Filter that would replay mutating requests without
// AllowMutating, or that holds malformed statuses.
func (f Filter) Validate() error {
	for _, m := range f.methods() {
		if !safe(m) && !f.AllowMutating {
			return fmt.Errorf("source: method %s changes state on the target; allow mutating methods explicitly to replay it", m)
		}
	}
	for _, s := range f.Status {
		if _, _, err := parseStatus(s); err != nil {
			return err
		}
	}
	return nil
}

// methods returns the methods to replay, or nil for every method.
func (f Filter) methods() []string {
	if len(f.Methods) == 0 {
		if f.AllowMutating {
			return nil // asked for mutating methods, named none: all of them
		}
		return SafeMethods
	}
	out := make([]string, len(f.Methods))
	for i, m := range f.Methods {
		out[i] = strings.ToUpper(strings.TrimSpace(m))
	}
	return out
}

func safe(method string) bool {
	return method == "GET" || method == "HEAD" || method == "OPTIONS"
}

// Skip returns why x is not replayed, or "" to replay it.
func (f Filter) Skip(x *capture.Exchange) string {
	if len(f.Services) > 0 && !slices.Contains(f.Services, x.Service) {
		return SkipService
	}
	method := strings.ToUpper(x.Request.Method)
	if ms := f.methods(); ms != nil && !slices.Contains(ms, method) {
		if !safe(method) && !f.AllowMutating {
			return SkipMutating
		}
		return SkipMethod
	}
	if f.PathRegex != nil {
		path, _, _ := strings.Cut(x.Request.Path, "?")
		if !f.PathRegex.MatchString(path) {
			return SkipPath
		}
	}
	if len(f.Status) > 0 && !f.statusMatches(x.Response.Status) {
		return SkipStatus
	}
	if b := x.Request.Body; b.Size > 0 && (b.Omitted != "" || b.Truncated) {
		// The tap did not keep the whole request body, so the candidate
		// would get a different request than production did.
		return SkipBodyUnavailable
	}
	return ""
}

func (f Filter) statusMatches(status int) bool {
	for _, s := range f.Status {
		code, class, err := parseStatus(s)
		if err != nil {
			continue
		}
		if class && status/100 == code || !class && status == code {
			return true
		}
	}
	return false
}

// parseStatus parses "201" (code) or "5xx" (class 5).
func parseStatus(s string) (n int, class bool, err error) {
	s = strings.ToLower(strings.TrimSpace(s))
	if len(s) == 3 && s[1:] == "xx" && s[0] >= '1' && s[0] <= '5' {
		return int(s[0] - '0'), true, nil
	}
	n, err = strconv.Atoi(s)
	if err != nil || n < 100 || n > 999 || s[0] == '+' {
		return 0, false, fmt.Errorf("source: status %q must be a code like 201 or a class like 5xx", s)
	}
	return n, false, nil
}
