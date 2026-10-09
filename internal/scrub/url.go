package scrub

import (
	"net/url"
	"strings"
)

// maxComponentPasses bounds how often a URL component is re-scrubbed while
// its decoded and encoded forms disagree about what is clean.
const maxComponentPasses = 6

// urlCodec is how a URL component is escaped.
type urlCodec int

const (
	pathCodec  urlCodec = iota // path segments: %XX, '+' is literal
	queryCodec                 // query and form pairs: %XX, '+' is a space
)

func (c urlCodec) unescape(s string) (string, error) {
	if c == queryCodec {
		return url.QueryUnescape(s)
	}
	return url.PathUnescape(s)
}

func (c urlCodec) escape(s string) string {
	if c == queryCodec {
		return url.QueryEscape(s)
	}
	return url.PathEscape(s)
}

// other returns the codec that reads '+' the other way.
func (c urlCodec) other() urlCodec {
	if c == queryCodec {
		return pathCodec
	}
	return queryCodec
}

// cleanViews reports whether no detector matches v, nor v read as an
// escaped path or query component. Audits decode stored values both ways,
// so a value is only clean when every reading is.
func cleanViews(v string, kinds kindSet) bool {
	return len(detect(v, kinds)) == 0 && cleanUnescaped(v, kinds)
}

// cleanUnescaped is cleanViews without the check of v itself.
func cleanUnescaped(v string, kinds kindSet) bool {
	if strings.IndexByte(v, '%') < 0 && strings.IndexByte(v, '+') < 0 {
		return true
	}
	for _, c := range []urlCodec{pathCodec, queryCodec} {
		if d, err := c.unescape(v); err == nil && d != v && len(detect(d, kinds)) > 0 {
			return false
		}
	}
	return true
}

// Path scrubs a request :path. Each path segment is unescaped and run
// through the detectors; each query pair is unescaped and its value redacted
// when the key is sensitive or run through the detectors otherwise. Only
// changed segments and pairs are re-encoded; malformed escapes are scrubbed
// as raw text. Path never fails.
func (s *Scrubber) Path(p string, rep Report) string {
	path, query, hasQuery := strings.Cut(p, "?")
	np := s.segments(path, rep)
	if !hasQuery {
		return np
	}
	nq := s.query(query, rep)
	if np == path && nq == query {
		return p
	}
	return np + "?" + nq
}

func (s *Scrubber) segments(p string, rep Report) string {
	if strings.IndexByte(p, '%') < 0 && !mayContainPII(p) {
		return p
	}
	segs := strings.Split(p, "/")
	changed := false
	for i, seg := range segs {
		if ns := s.component(seg, pathCodec, false, rep); ns != seg {
			segs[i], changed = ns, true
		}
	}
	if !changed {
		return p
	}
	return strings.Join(segs, "/")
}

// query scrubs an &-separated list of key=value pairs: a query string, a
// fragment or a form body.
func (s *Scrubber) query(q string, rep Report) string {
	if q == "" {
		return q
	}
	pairs := strings.Split(q, "&")
	changed := false
	for i, pr := range pairs {
		if np := s.pair(pr, rep); np != pr {
			pairs[i], changed = np, true
		}
	}
	if !changed {
		return q
	}
	return strings.Join(pairs, "&")
}

func (s *Scrubber) pair(pr string, rep Report) string {
	k, v, hasValue := strings.Cut(pr, "=")
	nk := s.component(k, queryCodec, false, rep)
	if !hasValue {
		return nk
	}
	sensitive := s.keyRules() && (s.sensitiveName(decodeName(k)) || nk != k && s.sensitiveName(decodeName(nk)))
	nv := s.component(v, queryCodec, sensitive, rep)
	if nk == k && nv == v {
		return pr
	}
	return nk + "=" + nv
}

func decodeName(k string) string {
	if d, err := url.QueryUnescape(k); err == nil {
		return d
	}
	return k
}

// component scrubs one escaped URL component: a path segment, query key or
// query value. The decoded text is scrubbed and re-escaped only if it
// changed. Escaping can itself put detectable text side by side ("+" for a
// space before a phone number), and servers disagree on whether '+' in a
// path is a space, so the encoded form and the other codec's reading are
// checked as well; the component is processed until all three are clean.
func (s *Scrubber) component(raw string, c urlCodec, sensitive bool, rep Report) string {
	if !sensitive && strings.IndexByte(raw, '%') < 0 && !mayContainPII(raw) {
		return raw
	}
	kinds := s.kinds & textDetectors
	alt := c.other()
	out := raw
	if c == queryCodec && strings.IndexByte(out, '+') >= 0 {
		// '+' is a space in a query, but also the plus of alice+tag@example.com.
		// Reading it as a space would find only "tag@example.com" and leave
		// "alice", so the literal reading is scrubbed first.
		if dec, err := alt.unescape(out); err == nil {
			if v := s.scrubValue(dec, sensitive, rep); v != dec {
				out = alt.escape(v)
			}
		}
	}
	for range maxComponentPasses {
		next := out
		if dec, err := c.unescape(out); err != nil {
			next = s.scrubValue(out, sensitive, rep)
		} else if v := s.scrubValue(dec, sensitive, rep); v != dec {
			next = c.escape(v)
		}
		if dec, err := alt.unescape(next); err == nil && dec != next && len(detect(dec, kinds)) > 0 {
			next = alt.escape(s.text(dec, rep))
		}
		if len(detect(next, kinds)) > 0 {
			next = s.text(next, rep)
		}
		if next == out {
			return out
		}
		out = next
	}
	// The two forms keep disagreeing; drop the component's content.
	rep.Add(KindSensitiveKey, 1)
	return s.token(raw)
}

// urlValue scrubs a URL header (Referer, Location): an absolute URL keeps
// its scheme and host, user info is tokenized, and the path, query and
// fragment are scrubbed like a request path.
func (s *Scrubber) urlValue(v string, rep Report) string {
	n := schemeLen(v)
	if n == 0 || !strings.HasPrefix(v[n:], "//") {
		return s.reference(v, rep)
	}
	start := n + 2
	end := len(v)
	if i := strings.IndexAny(v[start:], "/?#"); i >= 0 {
		end = start + i
	}
	auth, rest := v[start:end], v[end:]
	na, nr := s.authority(auth, rep), s.reference(rest, rep)
	if na == auth && nr == rest {
		return v
	}
	return v[:start] + na + nr
}

// schemeLen returns the length of a leading "scheme:", or 0.
func schemeLen(v string) int {
	for i := 0; i < len(v); i++ {
		c := v[i]
		switch {
		case isAlpha(c):
		case i > 0 && (isDigit(c) || c == '+' || c == '-' || c == '.'):
		case i > 0 && c == ':':
			return i + 1
		default:
			return 0
		}
	}
	return 0
}

func (s *Scrubber) authority(a string, rep Report) string {
	user, host, hasUser := "", a, false
	if at := strings.LastIndexByte(a, '@'); at >= 0 {
		user, host, hasUser = a[:at], a[at+1:], true
	}
	nh := s.ipOrText(host, rep)
	if !hasUser {
		return nh
	}
	nu := user
	switch {
	case user == "" || strongPseudonym(user):
	case s.kinds&setCredential != 0:
		rep.Add(KindCredential, 1)
		nu = s.token(user)
	default:
		nu = s.text(user, rep)
	}
	return nu + "@" + nh
}

// reference scrubs a URL without scheme and authority: path, query and a
// fragment, which is scrubbed like a query (OAuth implicit grants put
// access_token there).
func (s *Scrubber) reference(r string, rep Report) string {
	before, frag, hasFrag := strings.Cut(r, "#")
	np := s.Path(before, rep)
	if !hasFrag {
		return np
	}
	nf := s.query(frag, rep)
	if np == before && nf == frag {
		return r
	}
	return np + "#" + nf
}
