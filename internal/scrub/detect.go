package scrub

import "strings"

// Detector limits.
const (
	minCardDigits  = 13
	maxCardDigits  = 19
	minPhoneDigits = 8
	maxPhoneDigits = 15
	// An email match extends at most this far from its '@', so a long run of
	// address characters cannot make detection quadratic.
	maxEmailLocal  = 256
	maxEmailDomain = 512
)

// kindSet is a bit set of redaction kinds.
type kindSet uint16

const (
	setSecret kindSet = 1 << iota
	setJWT
	setEmail
	setCard
	setSSN
	setPhone
	setIPv4
	setIPv6
	setSensitiveKey
	setCredential

	// textDetectors are the detectors that run on free text.
	textDetectors = setSecret | setJWT | setEmail | setCard | setSSN | setPhone | setIPv4
	allKinds      = textDetectors | setIPv6 | setSensitiveKey | setCredential
)

// kindBit returns the set bit for a Kind* constant, or 0 for an unknown kind.
func kindBit(kind string) kindSet {
	switch kind {
	case KindSecret:
		return setSecret
	case KindJWT:
		return setJWT
	case KindEmail:
		return setEmail
	case KindCard:
		return setCard
	case KindSSN:
		return setSSN
	case KindPhone:
		return setPhone
	case KindIPv4:
		return setIPv4
	case KindIPv6:
		return setIPv6
	case KindSensitiveKey:
		return setSensitiveKey
	case KindCredential:
		return setCredential
	}
	return 0
}

// Detect returns the personal data that the text detectors find in t, sorted
// by Start and free of overlaps. Detectors take precedence in the order
// secret, jwt, email, card, ssn, phone, ipv4. Pseudonyms produced by this
// package never match: every marker fails a detector's validation. IPv6
// addresses are only recognized inside IP headers, so Detect ignores them.
//
// Detect needs no key; it backs leak audits such as `shadowtap tail
// --check-pii`.
func Detect(t string) []Match {
	return detect(t, textDetectors)
}

// detect runs the detectors in kinds on t.
func detect(t string, kinds kindSet) []Match {
	if t == "" || kinds&textDetectors == 0 || !mayContainPII(t) {
		return nil
	}
	var acc, buf []Match
	if kinds&setSecret != 0 && maySecret(t) {
		buf = findSecrets(t, buf[:0])
		acc = accept(acc, buf)
	}
	if kinds&setJWT != 0 && strings.Contains(t, "eyJ") {
		buf = findJWTs(t, buf[:0])
		acc = accept(acc, buf)
	}
	if kinds&setEmail != 0 && strings.IndexByte(t, '@') >= 0 {
		buf = findEmails(t, buf[:0])
		acc = accept(acc, buf)
	}
	if kinds&(setCard|setSSN|setPhone|setIPv4) == 0 {
		return acc
	}
	digits := countDigits(t, minCardDigits)
	if kinds&setCard != 0 && digits >= minCardDigits {
		buf = findCards(t, buf[:0])
		acc = accept(acc, buf)
	}
	if kinds&setSSN != 0 && digits >= 9 && strings.IndexByte(t, '-') >= 0 {
		buf = findSSNs(t, buf[:0])
		acc = accept(acc, buf)
	}
	if kinds&setPhone != 0 && digits >= minPhoneDigits {
		buf = findPhones(t, buf[:0])
		acc = accept(acc, buf)
	}
	if kinds&setIPv4 != 0 && digits >= 4 && strings.IndexByte(t, '.') >= 0 {
		buf = findIPv4s(t, buf[:0])
		acc = accept(acc, buf)
	}
	return acc
}

// accept merges the candidates that do not overlap an accepted match into
// acc. Both inputs are sorted by Start and free of overlaps, and so is the
// result; cands may be reused by the caller afterwards.
func accept(acc, cands []Match) []Match {
	if len(cands) == 0 {
		return acc
	}
	out := make([]Match, 0, len(acc)+len(cands))
	i := 0
	for _, c := range cands {
		for i < len(acc) && acc[i].End <= c.Start {
			out = append(out, acc[i])
			i++
		}
		if i < len(acc) && acc[i].Start < c.End {
			continue
		}
		out = append(out, c)
	}
	return append(out, acc[i:]...)
}

// mayContainPII is a cheap prefilter: it is false only when no detector can
// match t. Every detector needs a digit, an '@', an underscore (gh*_, sk_,
// rk_) or one of the literal prefixes eyJ, AKIA, ASIA, AIza and xox.
func mayContainPII[T string | []byte](t T) bool {
	for i := 0; i < len(t); i++ {
		switch c := t[i]; {
		case isDigit(c), c == '@', c == '_':
			return true
		case c == 'e':
			if i+2 < len(t) && t[i+1] == 'y' && t[i+2] == 'J' {
				return true
			}
		case c == 'A':
			if i+3 < len(t) && (t[i+1] == 'K' && t[i+2] == 'I' && t[i+3] == 'A' ||
				t[i+1] == 'S' && t[i+2] == 'I' && t[i+3] == 'A' ||
				t[i+1] == 'I' && t[i+2] == 'z' && t[i+3] == 'a') {
				return true
			}
		case c == 'x':
			if i+2 < len(t) && t[i+1] == 'o' && t[i+2] == 'x' {
				return true
			}
		}
	}
	return false
}

func maySecret(t string) bool {
	return strings.IndexByte(t, '_') >= 0 || strings.Contains(t, "AKIA") ||
		strings.Contains(t, "ASIA") || strings.Contains(t, "AIza") || strings.Contains(t, "xox")
}

// countDigits counts the ASCII digits in t, stopping at limit.
func countDigits(t string, limit int) int {
	n := 0
	for i := 0; i < len(t) && n < limit; i++ {
		if isDigit(t[i]) {
			n++
		}
	}
	return n
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }

func isAlpha(c byte) bool { c |= 0x20; return 'a' <= c && c <= 'z' }

func isAlnum(c byte) bool { return isDigit(c) || isAlpha(c) }

func isWord(c byte) bool { return isAlnum(c) || c == '_' }

func isLowerHex(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !isDigit(c) && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

// runLen counts the leading bytes of s accepted by ok, up to limit when
// limit >= 0.
func runLen(s string, ok func(byte) bool, limit int) int {
	n := 0
	for n < len(s) && (limit < 0 || n < limit) && ok(s[n]) {
		n++
	}
	return n
}

// startOK reports whether a numeric match may begin at t[i]: the byte before
// must not belong to a word. A letter preceded by a backslash is an escape
// such as \n, so the digits after it still start a match.
func startOK(t string, i int) bool {
	if i == 0 || !isWord(t[i-1]) {
		return true
	}
	return i >= 2 && t[i-2] == '\\' && strings.IndexByte("bfnrt", t[i-1]) >= 0
}

// endOK reports whether a numeric match may end before t[j].
func endOK(t string, j int) bool { return j >= len(t) || !isWord(t[j]) }

// Secrets: (AKIA|ASIA)[0-9A-Z]{16}, gh[pousr]_[A-Za-z0-9]{36,},
// (sk|rk)_(live|test)_[A-Za-z0-9]{16,}, xox[abprs]-[A-Za-z0-9-]{10,} and
// AIza[0-9A-Za-z_-]{35}.

func findSecrets(t string, dst []Match) []Match {
	for i := 0; i+4 <= len(t); i++ {
		if n := secretAt(t[i:]); n > 0 {
			dst = append(dst, Match{Kind: KindSecret, Start: i, End: i + n})
			i += n - 1
		}
	}
	return dst
}

// secretAt returns the length of the secret at the start of s, or 0.
func secretAt(s string) int {
	switch s[0] {
	case 'A':
		// AKIA: long-lived access key id; ASIA: temporary (STS) one.
		if (strings.HasPrefix(s, "AKIA") || strings.HasPrefix(s, "ASIA")) && runLen(s[4:], isUpperAlnum, 16) == 16 {
			return 20
		}
		if strings.HasPrefix(s, "AIza") && runLen(s[4:], isKeyChar, 35) == 35 {
			return 39
		}
	case 'g':
		if len(s) > 4 && s[1] == 'h' && strings.IndexByte("pousr", s[2]) >= 0 && s[3] == '_' {
			if n := runLen(s[4:], isAlnum, -1); n >= 36 {
				return 4 + n
			}
		}
	case 's', 'r':
		if strings.HasPrefix(s[1:], "k_live_") || strings.HasPrefix(s[1:], "k_test_") {
			if n := runLen(s[8:], isAlnum, -1); n >= 16 {
				return 8 + n
			}
		}
	case 'x':
		if len(s) > 5 && strings.HasPrefix(s, "xox") && strings.IndexByte("abprs", s[3]) >= 0 && s[4] == '-' {
			if n := runLen(s[5:], isAlnumDash, -1); n >= 10 {
				return 5 + n
			}
		}
	}
	return 0
}

func isUpperAlnum(c byte) bool { return isDigit(c) || 'A' <= c && c <= 'Z' }

func isKeyChar(c byte) bool { return isAlnum(c) || c == '_' || c == '-' }

func isAlnumDash(c byte) bool { return isAlnum(c) || c == '-' }

// JWTs: eyJ<base64url>{5,}.eyJ<base64url>{5,}.<base64url>*

func findJWTs(t string, dst []Match) []Match {
	for i := 0; ; {
		k := strings.Index(t[i:], "eyJ")
		if k < 0 {
			return dst
		}
		i += k
		if n := jwtAt(t[i:]); n > 0 {
			dst = append(dst, Match{Kind: KindJWT, Start: i, End: i + n})
			i += n
		} else {
			i++
		}
	}
}

func jwtAt(s string) int {
	n := jwtSegment(s)
	if n == 0 || n >= len(s) || s[n] != '.' {
		return 0
	}
	m := jwtSegment(s[n+1:])
	j := n + 1 + m
	if m == 0 || j >= len(s) || s[j] != '.' {
		return 0
	}
	j++
	return j + runLen(s[j:], isKeyChar, -1)
}

func jwtSegment(s string) int {
	if !strings.HasPrefix(s, "eyJ") {
		return 0
	}
	if n := runLen(s[3:], isKeyChar, -1); n >= 5 {
		return 3 + n
	}
	return 0
}

// Emails: [A-Za-z0-9._%+-]+@(label.)+[A-Za-z]{2,63}, case-insensitive.
// Addresses at redacted.invalid are pseudonyms, and a tokenized local part
// (tok_<hex>@host, from a URL's user info) carries nothing personal.

func findEmails(t string, dst []Match) []Match {
	last := 0
	for i := 0; i < len(t); i++ {
		k := strings.IndexByte(t[i:], '@')
		if k < 0 {
			break
		}
		at := i + k
		i = at
		l := at
		for l > last && at-l < maxEmailLocal && isLocalChar(t[l-1]) {
			l--
		}
		if l == at {
			continue
		}
		end := domainEnd(t, at+1)
		if end < 0 {
			continue
		}
		last, i = end, end-1
		if strings.EqualFold(t[at+1:end], pseudoEmailDomain) || isTokenPseudonym(t[l:at]) {
			continue
		}
		dst = append(dst, Match{Kind: KindEmail, Start: l, End: end})
	}
	return dst
}

// domainEnd returns the end of the domain that starts at t[j], or -1. Like
// the regular expression (label\.)+[A-Za-z]{2,63} it takes as many labels as
// still leave a top-level domain of at least two letters.
func domainEnd(t string, j int) int {
	end := -1
	limit := min(len(t), j+maxEmailDomain)
	pos := j
	for k := 0; ; k++ {
		start := pos
		for pos < limit && isLabelChar(t[pos]) {
			pos++
		}
		label := t[start:pos]
		if k > 0 {
			n := runLen(label, isAlpha, 63)
			if n >= 2 {
				end = start + n
			}
		}
		if !validLabel(label) || pos >= limit || t[pos] != '.' {
			return end
		}
		pos++
	}
}

func validLabel(l string) bool {
	return len(l) >= 1 && len(l) <= 63 && isAlnum(l[0]) && isAlnum(l[len(l)-1])
}

func isLocalChar(c byte) bool {
	return isAlnum(c) || c == '.' || c == '_' || c == '%' || c == '+' || c == '-'
}

func isLabelChar(c byte) bool { return isAlnum(c) || c == '-' }

// Cards: 13-19 digits, optionally separated by single spaces or dashes. A
// candidate must pass the Luhn check and match a brand's prefix and length.

type span struct{ start, end int }

func findCards(t string, dst []Match) []Match {
	var stack [8]span
	for i := 0; i < len(t); {
		if !isDigit(t[i]) {
			i++
			continue
		}
		// A chain is a maximal run of digit groups joined by single
		// separators; cards are looked for on group boundaries inside it.
		groups := stack[:0]
		j := i
		for {
			k := j
			for k < len(t) && isDigit(t[k]) {
				k++
			}
			groups = append(groups, span{j, k})
			if k+1 < len(t) && (t[k] == ' ' || t[k] == '-') && isDigit(t[k+1]) {
				j = k + 1
				continue
			}
			j = k
			break
		}
		dst = cardsInChain(t, groups, dst)
		i = j
	}
	return dst
}

// cardsInChain reports, from left to right, the longest valid card starting
// at each group boundary.
func cardsInChain(t string, groups []span, dst []Match) []Match {
	for a := 0; a < len(groups); {
		if a == 0 && !cardStartOK(t, groups[0].start) {
			a++
			continue
		}
		best, digits := -1, 0
		for b := a; b < len(groups); b++ {
			digits += groups[b].end - groups[b].start
			if digits > maxCardDigits {
				break
			}
			if digits < minCardDigits || b == len(groups)-1 && !cardEndOK(t, groups[b].end) {
				continue
			}
			if validCard(t[groups[a].start:groups[b].end]) {
				best = b
			}
		}
		if best < 0 {
			a++
			continue
		}
		dst = append(dst, Match{Kind: KindCard, Start: groups[a].start, End: groups[best].end})
		a = best + 1
	}
	return dst
}

// cardStartOK and cardEndOK also reject digits that are part of a decimal
// number such as 0.4111111111111111.
func cardStartOK(t string, i int) bool {
	return startOK(t, i) && !(i >= 2 && t[i-1] == '.' && isDigit(t[i-2]))
}

func cardEndOK(t string, j int) bool {
	return endOK(t, j) && !(j+1 < len(t) && t[j] == '.' && isDigit(t[j+1]))
}

// validCard reports whether the digits of s form a card number.
func validCard(s string) bool {
	var buf [maxCardDigits]byte
	d := buf[:0]
	for i := 0; i < len(s); i++ {
		if isDigit(s[i]) {
			if len(d) == maxCardDigits {
				return false
			}
			d = append(d, s[i])
		}
	}
	return len(d) >= minCardDigits && cardBrand(d) && luhnValid(d)
}

// cardBrand reports whether d (13-19 digits) has a known brand's prefix and
// length.
func cardBrand(d []byte) bool {
	n := len(d)
	prefix := func(k int) int {
		v := 0
		for _, c := range d[:k] {
			v = v*10 + int(c-'0')
		}
		return v
	}
	p2, p3, p4 := prefix(2), prefix(3), prefix(4)
	switch {
	case d[0] == '4': // Visa
		return n == 13 || n == 16 || n == 19
	case p2 >= 51 && p2 <= 55 || p4 >= 2221 && p4 <= 2720: // Mastercard
		return n == 16
	case p2 == 34 || p2 == 37: // American Express
		return n == 15
	case p4 == 6011 || p3 >= 644 && p3 <= 649 || p2 == 65: // Discover
		return n >= 16
	case p4 >= 3528 && p4 <= 3589: // JCB
		return n >= 16
	case p3 >= 300 && p3 <= 305 || p2 == 36 || p2 == 38 || p2 == 39: // Diners Club
		return n >= 14
	case p2 == 62: // UnionPay
		return n >= 16
	}
	return false
}

func luhnValid(d []byte) bool {
	return luhnSum(d)%10 == 0
}

// luhnSum returns the Luhn sum of d, doubling every second digit from the
// right starting with the second to last.
func luhnSum(d []byte) int {
	sum := 0
	for i := len(d) - 1; i >= 0; i-- {
		v := int(d[i] - '0')
		if (len(d)-1-i)%2 == 1 {
			v *= 2
			if v > 9 {
				v -= 9
			}
		}
		sum += v
	}
	return sum
}

// SSNs: ddd-dd-dddd with area not 000 or 666, group not 00 and serial not
// 0000. Area numbers 9xx are ITINs, which are just as personal.

func findSSNs(t string, dst []Match) []Match {
	for i := 0; i+11 <= len(t); i++ {
		if !ssnLayoutAt(t, i) || !startOK(t, i) || !endOK(t, i+11) {
			continue
		}
		if validSSN(t[i:i+3], t[i+4:i+6], t[i+7:i+11]) {
			dst = append(dst, Match{Kind: KindSSN, Start: i, End: i + 11})
			i += 10
		}
	}
	return dst
}

func ssnLayoutAt(t string, i int) bool {
	return t[i+3] == '-' && t[i+6] == '-' &&
		allDigits(t[i:i+3]) && allDigits(t[i+4:i+6]) && allDigits(t[i+7:i+11])
}

func validSSN(area, group, serial string) bool {
	return area != "000" && area != "666" && group != "00" && serial != "0000"
}

// isBareSSN reports whether v is nine digits forming a valid SSN. Bare
// numbers are only treated as SSNs under sensitive keys.
func isBareSSN(v string) bool {
	return len(v) == 9 && allDigits(v) && validSSN(v[:3], v[3:5], v[5:])
}

// Phones: "+" followed by 8-15 digits (the first 1-9) in groups joined by a
// space, dot, dash or parentheses, and North American numbers written with
// separators: (415) 555-2671, 415-555-2671, 415.555.2671, 415 555 2671.

func findPhones(t string, dst []Match) []Match {
	for i := 0; i < len(t); {
		c := t[i]
		switch {
		case c == '+':
			if end, ok := intlPhoneAt(t, i); ok {
				dst = append(dst, Match{Kind: KindPhone, Start: i, End: end})
				i = end
				continue
			}
		case c == '(' || isDigit(c):
			if end, ok := nanpAt(t, i, true); ok {
				dst = append(dst, Match{Kind: KindPhone, Start: i, End: end})
				i = end
				continue
			}
			if isDigit(c) {
				// No match can start inside a run of digits.
				for i < len(t) && isDigit(t[i]) {
					i++
				}
				continue
			}
		}
		i++
	}
	return dst
}

// intlPhoneAt matches an international number at t[i] == '+' and returns the
// end of the longest one with at most 15 digits.
func intlPhoneAt(t string, i int) (int, bool) {
	j := i + 1
	if j >= len(t) || t[j] < '1' || t[j] > '9' {
		return 0, false
	}
	best, digits := -1, 0
	for {
		k := j
		for k < len(t) && isDigit(t[k]) {
			k++
		}
		digits += k - j
		if digits > maxPhoneDigits {
			break
		}
		if digits >= minPhoneDigits && endOK(t, k) {
			best = k
		}
		n := joinerLen(t, k)
		if n == 0 || k+n >= len(t) || !isDigit(t[k+n]) {
			break
		}
		j = k + n
	}
	return best, best >= 0
}

// joinerLen returns the length of the separator `\)?[ .-]?\(?` at t[k].
func joinerLen(t string, k int) int {
	n := 0
	if k+n < len(t) && t[k+n] == ')' {
		n++
	}
	if k+n < len(t) && isPhoneSep(t[k+n]) {
		n++
	}
	if k+n < len(t) && t[k+n] == '(' {
		n++
	}
	return n
}

func isPhoneSep(c byte) bool { return c == ' ' || c == '.' || c == '-' }

// nanpAt matches a North American number at t[i]. With strict set the area
// code and exchange must start with 2-9 and the match must be word bounded;
// otherwise only the layout is checked (used to recognize pseudonyms).
func nanpAt(t string, i int, strict bool) (int, bool) {
	lead := func(c byte) bool { return isDigit(c) && (!strict || c >= '2') }
	three := func(j int) bool {
		return j+3 <= len(t) && lead(t[j]) && isDigit(t[j+1]) && isDigit(t[j+2])
	}
	j := i
	if t[j] == '(' {
		if !three(j+1) || j+4 >= len(t) || t[j+4] != ')' {
			return 0, false
		}
		j += 5
		if j < len(t) && (t[j] == ' ' || t[j] == '-') {
			j++
		}
	} else {
		if strict && !startOK(t, i) || !three(j) || j+3 >= len(t) || !isPhoneSep(t[j+3]) {
			return 0, false
		}
		j += 4
	}
	if !three(j) || j+3 >= len(t) || !isPhoneSep(t[j+3]) {
		return 0, false
	}
	j += 4
	if j+4 > len(t) || !allDigits(t[j:j+4]) {
		return 0, false
	}
	j += 4
	if strict && !endOK(t, j) {
		return 0, false
	}
	return j, true
}

// IPv4: a dotted quad with octets 0-255 that is not part of a longer dotted
// number. 0.0.0.0/8, 127.0.0.0/8 and 240.0.0.0/4 identify no one.

func findIPv4s(t string, dst []Match) []Match {
	for i := 0; i < len(t); {
		if !isDigit(t[i]) {
			i++
			continue
		}
		if end, ok := ipv4At(t, i); ok {
			dst = append(dst, Match{Kind: KindIPv4, Start: i, End: end})
			i = end
			continue
		}
		for i < len(t) && isDigit(t[i]) {
			i++
		}
	}
	return dst
}

func ipv4At(t string, i int) (int, bool) {
	if !startOK(t, i) || i >= 2 && t[i-1] == '.' && isDigit(t[i-2]) {
		return 0, false
	}
	octets, j, ok := parseIPv4(t, i)
	if !ok || !endOK(t, j) || j+1 < len(t) && t[j] == '.' && isDigit(t[j+1]) {
		return 0, false
	}
	if skipIPv4(octets[0]) {
		return 0, false
	}
	return j, true
}

// parseIPv4 parses four dot-separated decimal octets at t[i], allowing
// leading zeros, and returns them with the end offset.
func parseIPv4(t string, i int) ([4]byte, int, bool) {
	var octets [4]byte
	j := i
	for k := range octets {
		if k > 0 {
			if j >= len(t) || t[j] != '.' {
				return octets, 0, false
			}
			j++
		}
		n, v := 0, 0
		for j < len(t) && isDigit(t[j]) {
			if n == 3 {
				return octets, 0, false
			}
			v = v*10 + int(t[j]-'0')
			n++
			j++
		}
		if n == 0 || v > 255 {
			return octets, 0, false
		}
		octets[k] = byte(v)
	}
	return octets, j, true
}

// skipIPv4 reports whether an address with this first octet is left alone:
// "this network", loopback and the reserved range pseudonyms are drawn from.
func skipIPv4(first byte) bool { return first == 0 || first == 127 || first >= 240 }
