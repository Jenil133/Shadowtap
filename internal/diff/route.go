package diff

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/Jenil133/Shadowtap/internal/scrub"
)

var (
	numericRe  = regexp.MustCompile(`^[0-9]+$`)
	uuidRe     = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)
	ulidRe     = regexp.MustCompile(`^[0-9A-HJKMNP-TV-Z]{26}$`)
	hexRe      = regexp.MustCompile(`^[0-9a-fA-F]{8,}$`)
	prefixIDRe = regexp.MustCompile(`^[a-zA-Z]{1,8}[_-][0-9a-zA-Z]{6,}$`)
)

// RouteTemplate collapses the identifiers in a request path so that requests
// for different resources of one endpoint group together:
// "GET /api/users/u_3f2a1b9c0d4e?x=1" becomes "GET /api/users/{id}". Numeric,
// UUID, ULID, long hex and prefixed ids (u_..., ord-...) and pseudonyms
// collapse; the query is dropped and the method is uppercased. The path part
// of a template is always absolute: a path without a leading slash, an empty
// one or a bare query string becomes "/..." (a capture's :path is absolute,
// so this only matters for malformed input), and a template is its own
// template.
func RouteTemplate(method, rawPath string) string {
	p, _, _ := strings.Cut(rawPath, "?")
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	segs := strings.Split(p, "/")
	for i, s := range segs {
		if s == "" {
			continue
		}
		if isIDSegment(s) {
			segs[i] = "{id}"
		}
	}
	return strings.ToUpper(method) + " " + strings.Join(segs, "/")
}

func isIDSegment(s string) bool {
	if u, err := url.PathUnescape(s); err == nil {
		s = u
	}
	if numericRe.MatchString(s) || uuidRe.MatchString(s) || ulidRe.MatchString(s) || scrub.IsPseudonym(s) {
		return true
	}
	if hexRe.MatchString(s) && strings.ContainsAny(s, "0123456789") {
		return true
	}
	// Prefixed ids carry a digit; words such as "lookup-users" do not.
	return prefixIDRe.MatchString(s) && strings.ContainsAny(s, "0123456789")
}
