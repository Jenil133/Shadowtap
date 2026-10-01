package scrub

import (
	"net/url"
	"strings"
)

// maxAssignName bounds how far back a name is looked for before an '=' or ':'.
const maxAssignName = 64

// assignments redacts the values of sensitive names written as assignments
// inside free text: password=hunter2, "token": "abc", pin: 1234. Structured
// bodies are handled by their parsers; this covers what they cannot see: text
// bodies, JSON or form data sent under the wrong content type, GraphQL
// documents, JSON embedded in a string, and URLs in text.
//
// A value is quoted ("..." or '...') or runs to the next delimiter; when a
// detector match starts the value (a phone number with spaces) the match is
// the value. Values that are already pseudonyms are kept, so the pass is
// idempotent.
func (s *Scrubber) assignments(t string, rep Report) string {
	if !s.keyRules() || (strings.IndexByte(t, '=') < 0 && strings.IndexByte(t, ':') < 0) {
		return t
	}
	var b strings.Builder
	done := 0
	for i := 0; i < len(t); i++ {
		if t[i] != '=' && t[i] != ':' {
			continue
		}
		if i < done {
			continue
		}
		name := assignName(t, i)
		if name == "" || !s.sensitiveName(name) {
			continue
		}
		vs, ve := assignValue(t, i+1)
		if vs >= ve {
			continue
		}
		nv := s.assignedValue(t[vs:ve], rep)
		if nv == t[vs:ve] {
			i = ve - 1
			continue
		}
		if b.Len() == 0 {
			b.Grow(len(t) + 24)
		}
		b.WriteString(t[done:vs])
		b.WriteString(nv)
		done = ve
		i = ve - 1
	}
	if done == 0 {
		return t
	}
	b.WriteString(t[done:])
	return b.String()
}

// assignedValue redacts one assigned value. A percent-encoded value (a URL
// query inside text) is decoded first, so alice%40example.com is an email
// address, and the result is encoded again.
func (s *Scrubber) assignedValue(v string, rep Report) string {
	if strings.IndexByte(v, '%') >= 0 {
		if dec, err := url.QueryUnescape(v); err == nil && dec != v {
			if nv := s.sensitiveValue(dec, rep); nv != dec {
				return url.QueryEscape(nv)
			}
			return v
		}
	}
	return s.sensitiveValue(v, rep)
}

// assignName returns the name left of the '=' or ':' at t[i]: the run of name
// characters before it, ignoring spaces and one closing quote.
func assignName(t string, i int) string {
	j := i
	for j > 0 && (t[j-1] == ' ' || t[j-1] == '\t') {
		j--
	}
	if j > 0 && (t[j-1] == '"' || t[j-1] == '\'') {
		j--
	}
	k := j
	for k > 0 && j-k < maxAssignName && isAssignNameChar(t[k-1]) {
		k--
	}
	name := t[k:j]
	// A pseudonymized address just before the name is not part of it; without
	// this "redacted.invalid_token" would read as a name ending in "idtoken".
	if i := strings.LastIndex(name, pseudoEmailDomain); i >= 0 {
		name = name[i+len(pseudoEmailDomain):]
	}
	return name
}

func isAssignNameChar(c byte) bool {
	return isAlnum(c) || c == '_' || c == '-' || c == '.' || c == '[' || c == ']'
}

// assignValue finds the value after the separator at t[from-1]: its start and
// end offsets, quotes excluded, or an empty range.
func assignValue(t string, from int) (int, int) {
	i := from
	// A '+' here is how a query writes a space (phone=+tok), and a plus in
	// front of a number is no part of what needs hiding.
	for i < len(t) && (t[i] == ' ' || t[i] == '\t' || t[i] == '+') {
		i++
	}
	if i >= len(t) {
		return i, i
	}
	if q := t[i]; q == '"' || q == '\'' {
		j := i + 1
		for j < len(t) && t[j] != q && t[j] != '\n' {
			if t[j] == '\\' && j+1 < len(t) {
				j++
			}
			j++
		}
		return i + 1, min(j, len(t))
	}
	end := i
	for end < len(t) && !isAssignEnd(t[end]) {
		end++
	}
	// A value with spaces inside (a phone number) is whatever a detector finds
	// at its start.
	lineEnd := end
	for lineEnd < len(t) && t[lineEnd] != '\n' && t[lineEnd] != '\r' && lineEnd-i < 64 {
		lineEnd++
	}
	if ms := detect(t[i:lineEnd], textDetectors); len(ms) > 0 && ms[0].Start == 0 && ms[0].End > end-i {
		return i, i + ms[0].End
	}
	// "tok_..+more" is a pseudonym followed by an escaped space and more text.
	if p := strings.IndexByte(t[i:end], '+'); p > 0 && strongPseudonym(t[i:i+p]) {
		end = i + p
	}
	// Trailing '+' is an escaped space after the value, not part of it.
	for end > i && t[end-1] == '+' {
		end--
	}
	return i, end
}

func isAssignEnd(c byte) bool {
	switch c {
	case ' ', '\t', '\r', '\n', ',', ';', '&', '"', '\'', '<', '>', ')', '}', ']', '\\':
		return true
	}
	return false
}

// encodedEmails pseudonymizes email addresses whose '@' is percent-encoded
// (alice%40example.com, or %2540 when encoded twice) in text that is not a
// URL component the URL scrubbers would decode, such as a link inside a JSON
// string or an HTML attribute. The pseudonym keeps the encoding.
func (s *Scrubber) encodedEmails(t string, rep Report) string {
	if s.kinds&setEmail == 0 || (!strings.Contains(t, "%40") && !strings.Contains(t, "%2540")) {
		return t
	}
	var b strings.Builder
	done := 0
	for i := 0; i < len(t); i++ {
		at := 0
		switch {
		case strings.HasPrefix(t[i:], "%2540"):
			at = 5
		case strings.HasPrefix(t[i:], "%40"):
			at = 3
		default:
			continue
		}
		ls := i
		for ls > done && isEmailLocalByte(t[ls-1]) {
			ls--
		}
		if ls > done && t[ls-1] == '%' {
			// The first two characters belong to an escape (%21), not to the
			// address.
			ls = min(ls+2, i)
		}
		de := i + at
		for de < len(t) && isEmailDomainByte(t[de]) {
			de++
		}
		local, domain := t[ls:i], strings.TrimRight(t[i+at:de], ".-")
		if local == "" || !validEncodedDomain(domain) {
			i += at - 1
			continue
		}
		email := local + "@" + domain
		if len(detect(email, setEmail)) != 1 {
			i += at - 1
			continue
		}
		b.WriteString(t[done:ls])
		b.WriteString(strings.Replace(s.pseudonym(KindEmail, email), "@", t[i:i+at], 1))
		rep.Add(KindEmail, 1)
		done = i + at + len(domain)
		i = done - 1
	}
	if done == 0 {
		return t
	}
	b.WriteString(t[done:])
	return b.String()
}

func isEmailLocalByte(c byte) bool {
	return isAlnum(c) || c == '.' || c == '_' || c == '+' || c == '-'
}

func isEmailDomainByte(c byte) bool { return isAlnum(c) || c == '.' || c == '-' }

func validEncodedDomain(d string) bool {
	dot := strings.LastIndexByte(d, '.')
	return dot > 0 && len(d)-dot-1 >= 2 && !strings.EqualFold(d, pseudoEmailDomain)
}
