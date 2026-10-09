package scrub

import (
	"net/netip"
	"strings"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// Headers returns a scrubbed copy of h; h itself is not modified. Drop
// headers are removed; credential headers, cookies and set-cookie values
// are tokenized; addresses in IP headers are pseudonymized one by one; URL
// headers are scrubbed like a request path; every other value goes through
// the text detectors, and like a URL when it carries percent-encoded
// personal data. Multiple values keep their order. Every value is clean as
// stored and when read as an escaped path or query; one that cannot be made
// so is replaced by a token.
func (s *Scrubber) Headers(h capture.Headers, rep Report) capture.Headers {
	if h == nil {
		return nil
	}
	out := make(capture.Headers, len(h))
	for name, vals := range h {
		lname := strings.ToLower(name)
		if _, drop := s.drop[lname]; drop {
			continue
		}
		if vals == nil {
			out[name] = nil
			continue
		}
		nv := make([]string, len(vals))
		for i, v := range vals {
			nv[i] = s.headerValue(lname, v, rep)
		}
		out[name] = nv
	}
	return out
}

// headerValue scrubs one value of the header with the lowercase name. The
// result is clean as stored and when read as an escaped path or query.
func (s *Scrubber) headerValue(name, v string, rep Report) string {
	kinds := s.kinds & textDetectors
	creds := s.kinds&setCredential != 0
	var out string
	switch {
	case creds && has(s.credential, name):
		out = s.credentialValue(name, v, rep)
	case creds && name == "cookie":
		out = s.cookieValue(v, rep)
	case creds && name == "set-cookie":
		out = s.setCookieValue(v, rep)
	case has(s.ipHeaders, name):
		out = s.ipHeaderValue(v, name == "forwarded", rep)
	case has(s.urlHeaders, name) || name == ":path":
		out = s.urlValue(v, rep)
	case s.keyRules() && name != "cookie" && name != "set-cookie" && !has(s.credential, name) && s.sensitiveName(name):
		// A header named like a sensitive field (x-user-email, x-auth-token)
		// is redacted whole, the way the same name would be in a body.
		out = s.sensitiveValue(v, rep)
	default:
		out = s.text(v, rep)
		if cleanUnescaped(out, kinds) { // Text left out itself clean
			return out
		}
		// Escaped data in other headers (x-envoy-original-path carries the
		// pre-rewrite path and query) is scrubbed like a URL.
		out = s.urlValue(out, rep)
	}
	// Each part of a structured value is clean, but parts kept verbatim (an
	// auth scheme, a URL scheme) can still form a match with their
	// neighbours: "A+1 000-00-0000" is a phone number. Keep nothing of such
	// a value; every header kind leaves a token alone, so this is final.
	if !cleanViews(out, kinds) {
		rep.Add(KindSensitiveKey, 1)
		out = s.token(v)
	}
	return out
}

func has(set map[string]struct{}, k string) bool {
	_, ok := set[k]
	return ok
}

// credentialValue tokenizes a credential header. "Scheme credentials"
// becomes "Scheme tok_<16 hex>" for well-known schemes only, so the first
// word of an opaque credential is never kept.
func (s *Scrubber) credentialValue(name, v string, rep Report) string {
	if v == "" {
		return v
	}
	scheme, cred := "", v
	if n, ok := authScheme(name, v); ok {
		scheme, cred = v[:n], v[n:]
	}
	if strongPseudonym(cred) {
		return v
	}
	rep.Add(KindCredential, 1)
	return scheme + s.token(cred)
}

// authScheme finds "scheme SP+ credentials" in v and returns where the
// credentials start. Schemes start with a letter.
func authScheme(header, v string) (int, bool) {
	if v == "" || !isAlpha(v[0]) {
		return 0, false
	}
	i := 0
	for i < len(v) && isTokenChar(v[i]) {
		i++
	}
	if i == len(v) || v[i] != ' ' {
		return 0, false
	}
	j := i
	for j < len(v) && v[j] == ' ' {
		j++
	}
	if j == len(v) {
		return 0, false
	}
	// Only well-known schemes are kept: a credential written without one
	// ("hunter2 extra") must not leave its first word behind as a scheme.
	if !knownAuthScheme(v[:i]) {
		return 0, false
	}
	return j, true
}

func knownAuthScheme(s string) bool {
	switch strings.ToLower(s) {
	case "basic", "bearer", "digest", "negotiate", "ntlm", "token", "apikey", "api-key",
		"dpop", "hoba", "mutual", "vapid", "oauth", "mac", "hawk", "ssws", "bot", "jwt",
		"aws4-hmac-sha256", "scram-sha-1", "scram-sha-256":
		return true
	}
	return false
}

// isTokenChar reports whether c is an RFC 9110 tchar.
func isTokenChar(c byte) bool {
	return isAlnum(c) || strings.IndexByte("!#$%&'*+-.^_`|~", c) >= 0
}

// cookieValue tokenizes every value of a Cookie header and keeps the names.
func (s *Scrubber) cookieValue(v string, rep Report) string {
	parts := strings.Split(v, ";")
	changed := false
	for i, p := range parts {
		if np := s.cookiePair(p, rep); np != p {
			parts[i], changed = np, true
		}
	}
	if !changed {
		return v
	}
	return strings.Join(parts, ";")
}

// setCookieValue tokenizes the cookie value of a Set-Cookie header and keeps
// its attributes.
func (s *Scrubber) setCookieValue(v string, rep Report) string {
	first, attrs, hasAttrs := strings.Cut(v, ";")
	nf := s.cookiePair(first, rep)
	if !hasAttrs {
		return nf
	}
	na := s.text(attrs, rep)
	if nf == first && na == attrs {
		return v
	}
	return nf + ";" + na
}

// cookiePair tokenizes the value of one name=value pair, keeping the
// surrounding whitespace. A pair without '=' is all value.
func (s *Scrubber) cookiePair(p string, rep Report) string {
	lead, body, trail := splitSpace(p)
	if body == "" {
		return p
	}
	name, val, hasEq := strings.Cut(body, "=")
	if !hasEq {
		return lead + s.cookieToken(body, rep) + trail
	}
	nn, nv := s.text(name, rep), s.cookieToken(val, rep)
	if nn == name && nv == val {
		return p
	}
	return lead + nn + "=" + nv + trail
}

func (s *Scrubber) cookieToken(v string, rep Report) string {
	inner, quoted := v, false
	if len(v) >= 2 && v[0] == '"' && v[len(v)-1] == '"' {
		inner, quoted = v[1:len(v)-1], true
	}
	if inner == "" || strongPseudonym(inner) {
		return v
	}
	rep.Add(KindCredential, 1)
	if quoted {
		return `"` + s.token(inner) + `"`
	}
	return s.token(inner)
}

// splitSpace splits v into leading spaces and tabs, the rest, and trailing
// spaces and tabs.
func splitSpace(v string) (lead, body, trail string) {
	body = strings.TrimLeft(v, " \t")
	lead = v[:len(v)-len(body)]
	trimmed := strings.TrimRight(body, " \t")
	return lead, trimmed, body[len(trimmed):]
}

// ipHeaderValue pseudonymizes each address in a comma-separated list of
// nodes, or in the for= and by= parameters of a Forwarded header (RFC 7239).
// Tokens that are not addresses ("unknown", "_hidden") go through the text
// detectors and are normally left alone.
func (s *Scrubber) ipHeaderValue(v string, forwarded bool, rep Report) string {
	elems := splitQuoted(v, ',')
	changed := false
	for i, e := range elems {
		var ne string
		if forwarded {
			ne = s.forwardedElement(e, rep)
		} else {
			ne = s.ipNode(e, rep)
		}
		if ne != e {
			elems[i], changed = ne, true
		}
	}
	if !changed {
		return v
	}
	return strings.Join(elems, ",")
}

func (s *Scrubber) forwardedElement(e string, rep Report) string {
	pairs := splitQuoted(e, ';')
	changed := false
	for i, p := range pairs {
		var np string
		if k, val, ok := strings.Cut(p, "="); ok && isNodeParam(k) {
			np = k + "=" + s.ipNode(val, rep)
		} else {
			np = s.text(p, rep)
		}
		if np != p {
			pairs[i], changed = np, true
		}
	}
	if !changed {
		return e
	}
	return strings.Join(pairs, ";")
}

func isNodeParam(k string) bool {
	k = strings.TrimSpace(k)
	return strings.EqualFold(k, "for") || strings.EqualFold(k, "by")
}

// ipNode pseudonymizes one node: an address, optionally with a port,
// brackets, quotes and surrounding whitespace, all of which are kept.
func (s *Scrubber) ipNode(raw string, rep Report) string {
	lead, body, trail := splitSpace(raw)
	inner, quoted := body, false
	if len(body) >= 2 && body[0] == '"' && body[len(body)-1] == '"' {
		inner, quoted = body[1:len(body)-1], true
	}
	ni := s.ipOrText(inner, rep)
	if ni == inner {
		return raw
	}
	if quoted {
		ni = `"` + ni + `"`
	}
	return lead + ni + trail
}

// ipOrText pseudonymizes v as an address when it is one and runs the text
// detectors over it otherwise. The text detectors can turn a near-address
// into an address (::1.0.00.0 becomes ::243.1.2.3, a valid IPv6 literal),
// so this repeats until v is stable.
func (s *Scrubber) ipOrText(v string, rep Report) string {
	for range maxComponentPasses {
		next, ok := s.ipLiteral(v, rep)
		if !ok {
			next = s.text(v, rep)
		}
		if next == v {
			return v
		}
		v = next
	}
	rep.Add(KindSensitiveKey, 1)
	return s.token(v)
}

// ipLiteral pseudonymizes an address written as a.b.c.d[:port], as an IPv6
// address, or as [v6][:port]. ok is false when v is not an address.
func (s *Scrubber) ipLiteral(v string, rep Report) (string, bool) {
	if strings.HasPrefix(v, "[") {
		end := strings.IndexByte(v, ']')
		if end < 0 {
			return "", false
		}
		addr, err := netip.ParseAddr(v[1:end])
		rest := v[end+1:]
		if err != nil || rest != "" && !isPort(rest) {
			return "", false
		}
		return "[" + s.ipPseudonym(addr, v[1:end], rep) + "]" + rest, true
	}
	if addr, err := netip.ParseAddr(v); err == nil {
		return s.ipPseudonym(addr, v, rep), true
	}
	if i := strings.LastIndexByte(v, ':'); i > 0 && isPort(v[i:]) {
		if addr, err := netip.ParseAddr(v[:i]); err == nil && addr.Is4() {
			return s.ipPseudonym(addr, v[:i], rep) + v[i:], true
		}
	}
	return "", false
}

// isPort reports whether p is ":" followed by 1-5 digits.
func isPort(p string) bool {
	return len(p) >= 2 && len(p) <= 6 && p[0] == ':' && allDigits(p[1:])
}

// ipPseudonym returns the pseudonym for addr, or orig, its original text,
// when the address identifies no one or its detector is disabled.
func (s *Scrubber) ipPseudonym(addr netip.Addr, orig string, rep Report) string {
	switch {
	case addr.Is4() || addr.Is4In6():
		v4 := addr.Unmap()
		if s.kinds&setIPv4 == 0 || skipIPv4(v4.As4()[0]) {
			return orig
		}
		rep.Add(KindIPv4, 1)
		p := s.pseudonym(KindIPv4, v4.String())
		if addr.Is4() {
			return p
		}
		// Keep the IPv4-mapped form, ::ffff:240.x.y.z.
		return "::ffff:" + p
	default:
		a := addr.WithZone("")
		if s.kinds&setIPv6 == 0 || a.IsLoopback() || a.IsUnspecified() || inDiscardPrefix(a) {
			return orig
		}
		rep.Add(KindIPv6, 1)
		return s.pseudonym(KindIPv6, a.String())
	}
}

// splitQuoted splits s at every sep outside a quoted string. Joining the
// parts with sep gives s back.
func splitQuoted(s string, sep byte) []string {
	var parts []string
	start, quoted := 0, false
	for i := 0; i < len(s); i++ {
		switch c := s[i]; {
		case c == '\\' && quoted:
			i++
		case c == '"':
			quoted = !quoted
		case c == sep && !quoted:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	return append(parts, s[start:])
}
