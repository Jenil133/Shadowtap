package tap

import (
	"strings"
	"sync"
	"sync/atomic"
)

const (
	// unknownService names exchanges whose service cannot be determined.
	unknownService = "unknown"
	// otherService is the metrics label for services beyond the label cap.
	otherService = "other"
	// maxServiceLabels bounds the distinct service label values in metrics.
	maxServiceLabels = 100
	// maxServiceNameLen is the longest service name accepted from :authority
	// (one DNS label).
	maxServiceNameLen = 63
)

// serviceName picks the service a request belongs to: the name the
// EnvoyFilter passes in gRPC metadata, else one derived from the :authority
// host, else "unknown".
func serviceName(fromMetadata, authority string) string {
	if fromMetadata != "" {
		return fromMetadata
	}
	if s := serviceFromAuthority(authority); s != "" {
		return s
	}
	return unknownService
}

// serviceFromAuthority derives a service name from an :authority value: the
// host without port, with in-cluster names (name.ns.svc[.cluster.local])
// reduced to their first label. The authority comes from the client, so the
// result is restricted to a short lowercase hostname; anything else yields "".
func serviceFromAuthority(authority string) string {
	host := authority
	if i := strings.LastIndexByte(host, '@'); i >= 0 {
		host = host[i+1:] // userinfo is not valid in :authority but cheap to tolerate
	}
	switch {
	case strings.HasPrefix(host, "["):
		end := strings.IndexByte(host, ']')
		if end < 0 {
			return ""
		}
		host = host[1:end] // an IPv6 literal, rejected below
	case strings.Count(host, ":") == 1:
		host = host[:strings.IndexByte(host, ':')]
	}
	host = strings.TrimSuffix(strings.ToLower(host), ".")
	if strings.HasSuffix(host, ".svc") || strings.HasSuffix(host, ".svc.cluster.local") {
		if i := strings.IndexByte(host, '.'); i > 0 {
			host = host[:i]
		}
	}
	if !validServiceName(host) {
		return ""
	}
	return host
}

// validServiceName reports whether s is non-empty, at most maxServiceNameLen
// bytes and only uses [a-z0-9.-].
func validServiceName(s string) bool {
	if s == "" || len(s) > maxServiceNameLen {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !('a' <= c && c <= 'z' || '0' <= c && c <= '9' || c == '.' || c == '-') {
			return false
		}
	}
	return true
}

// serviceLabels caps the number of distinct service label values: the first
// limit names are used as they are and later ones become "other", so random
// Host headers cannot explode metric cardinality.
//
// Lookups happen for every stream, so the set is copy-on-write: readers load
// an immutable map without locking; writers (at most limit of them) copy it.
type serviceLabels struct {
	limit int
	mu    sync.Mutex // serializes writers
	known atomic.Pointer[map[string]struct{}]
}

func newServiceLabels(limit int) *serviceLabels {
	l := &serviceLabels{limit: limit}
	l.known.Store(&map[string]struct{}{})
	return l
}

// label returns the metrics label for service.
func (l *serviceLabels) label(service string) string {
	if service == unknownService {
		return service
	}
	if lbl, ok := l.lookup(*l.known.Load(), service); ok {
		return lbl
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	cur := *l.known.Load()
	if lbl, ok := l.lookup(cur, service); ok {
		return lbl
	}
	next := make(map[string]struct{}, len(cur)+1)
	for k := range cur {
		next[k] = struct{}{}
	}
	next[service] = struct{}{}
	l.known.Store(&next)
	return service
}

// lookup answers from the current set when it can: a known name, or "other"
// once the set is full.
func (l *serviceLabels) lookup(known map[string]struct{}, service string) (string, bool) {
	if _, ok := known[service]; ok {
		return service, true
	}
	if len(known) >= l.limit {
		return otherService, true
	}
	return "", false
}
