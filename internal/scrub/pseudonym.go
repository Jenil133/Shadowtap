package scrub

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"hash"
	"net/netip"
	"strconv"
	"strings"
)

// Pseudonym markers. Real data never looks like these, which is what makes
// scrubbing idempotent.
const (
	tokenPrefix       = "tok_"
	emailLocalPrefix  = "user-"
	pseudoEmailDomain = "redacted.invalid"
	jwtPrefix         = "redacted.jwt."
	secretPrefix      = "redacted.secret."

	// maxRegenerations bounds the attempts at a pseudonym no detector
	// matches before falling back to the all-zero layout.
	maxRegenerations = 16
)

// macState is a reusable HMAC with scratch space for its input and output.
type macState struct {
	h   hash.Hash
	msg []byte
	sum []byte
}

// maxPooledMsg keeps an unusually large value from pinning its buffer in
// the pool.
const maxPooledMsg = 4 << 10

// mac returns HMAC-SHA256(key, kind ‖ 0x00 ‖ value), with ‖ 0x00 ‖ ctr
// appended when ctr > 0 to derive alternative pseudonyms.
func (s *Scrubber) mac(kind, value string, ctr int) [sha256.Size]byte {
	st, ok := s.macs.Get().(*macState)
	if !ok {
		st = &macState{h: hmac.New(sha256.New, s.key)}
	}
	st.h.Reset()
	msg := append(st.msg[:0], kind...)
	msg = append(msg, 0)
	msg = append(msg, value...)
	if ctr > 0 {
		msg = append(msg, 0, byte(ctr))
	}
	st.h.Write(msg)
	st.sum = st.h.Sum(st.sum[:0])
	var sum [sha256.Size]byte
	copy(sum[:], st.sum)
	if cap(msg) <= maxPooledMsg {
		st.msg = msg
	}
	s.macs.Put(st)
	return sum
}

// pseudonym returns the deterministic replacement for v, a value of the
// given kind. Each candidate is checked with Detect; one a detector would
// still match is regenerated with a counter, and the all-zero layout, which
// no detector matches, is the last resort.
func (s *Scrubber) pseudonym(kind, v string) string {
	norm := normalizeValue(kind, v)
	for ctr := range maxRegenerations {
		sum := s.mac(kind, norm, ctr)
		if p := render(kind, v, &sum); len(Detect(p)) == 0 {
			return p
		}
	}
	return render(kind, v, nil)
}

// token returns tok_<16 hex> for v. Credentials, cookies and values under
// sensitive keys share this derivation, so one secret maps to the same token
// wherever it appears.
func (s *Scrubber) token(v string) string {
	return s.pseudonym(KindSensitiveKey, v)
}

// normalizeValue returns the HMAC input for v, so that formatting variants
// of one value share a pseudonym.
func normalizeValue(kind, v string) string {
	switch kind {
	case KindEmail:
		return strings.ToLower(v)
	case KindCard, KindSSN, KindPhone:
		return digitsOf(v)
	case KindIPv4:
		if octets, end, ok := parseIPv4(v, 0); ok && end == len(v) {
			return formatIPv4(octets)
		}
	}
	return v
}

func digitsOf(v string) string {
	b := make([]byte, 0, len(v))
	for i := 0; i < len(v); i++ {
		if isDigit(v[i]) {
			b = append(b, v[i])
		}
	}
	return string(b)
}

// render builds the pseudonym of kind for v from sum, or the all-zero
// layout when sum is nil.
func render(kind, v string, sum *[sha256.Size]byte) string {
	switch kind {
	case KindEmail:
		return hexPseudonym(emailLocalPrefix, sum, 5, "@"+pseudoEmailDomain)
	case KindJWT:
		return hexPseudonym(jwtPrefix, sum, 8, "")
	case KindSecret:
		return hexPseudonym(secretPrefix, sum, 8, "")
	case KindCard:
		return renderCard(v, newDigitStream(sum))
	case KindSSN:
		// Always 000-dd-dddd, also for a bare nine-digit SSN under a
		// sensitive key: a bare 000dddddd marker could be a real zero-padded
		// number, which IsPseudonym would then keep.
		return renderDigits("000-00-0000", newDigitStream(sum), func(k int, _ bool) bool { return k < 3 })
	case KindPhone:
		return renderDigits(v, newDigitStream(sum), func(_ int, runStart bool) bool { return runStart })
	case KindIPv4:
		return formatIPv4(ipv4Pseudonym(sum))
	case KindIPv6:
		return ipv6Pseudonym(sum).String()
	}
	return hexPseudonym(tokenPrefix, sum, 8, "")
}

// hexPseudonym returns prefix, n bytes of sum in hex (zeros when sum is
// nil), and suffix.
func hexPseudonym(prefix string, sum *[sha256.Size]byte, n int, suffix string) string {
	var buf [64]byte
	b := append(buf[:0], prefix...)
	if sum == nil {
		for range 2 * n {
			b = append(b, '0')
		}
	} else {
		b = hex.AppendEncode(b, sum[:n])
	}
	return string(append(b, suffix...))
}

// digitStream yields uniform decimal digits from an HMAC. A nil stream
// yields zeros.
type digitStream struct {
	buf  [sha256.Size]byte
	i    int
	zero bool
}

func newDigitStream(sum *[sha256.Size]byte) *digitStream {
	if sum == nil {
		return &digitStream{zero: true}
	}
	return &digitStream{buf: *sum}
}

func (d *digitStream) next() byte {
	if d.zero {
		return '0'
	}
	for {
		if d.i == len(d.buf) {
			d.buf = sha256.Sum256(d.buf[:])
			d.i = 0
		}
		b := d.buf[d.i]
		d.i++
		if b < 250 { // 250 is a multiple of 10, so every digit is equally likely
			return '0' + b%10
		}
	}
}

// renderDigits keeps the layout of v and replaces each digit with one from
// ds, or with 0 where zero(k, runStart) says so (k counts digits, runStart
// marks the first digit of a run).
func renderDigits(v string, ds *digitStream, zero func(k int, runStart bool) bool) string {
	b := []byte(v)
	k := 0
	for i, c := range b {
		if !isDigit(c) {
			continue
		}
		if zero(k, i == 0 || !isDigit(b[i-1])) {
			b[i] = '0'
		} else {
			b[i] = ds.next()
		}
		k++
	}
	return string(b)
}

// renderCard keeps the layout of v, sets the first digit to 0 (no card brand
// starts with 0) and the last so that the Luhn check passes.
func renderCard(v string, ds *digitStream) string {
	b := []byte(v)
	var pos []int
	for i, c := range b {
		if isDigit(c) {
			pos = append(pos, i)
		}
	}
	if len(pos) == 0 {
		return v
	}
	digits := make([]byte, len(pos))
	for k, p := range pos {
		switch {
		case k == 0 || k == len(pos)-1:
			b[p] = '0'
		default:
			b[p] = ds.next()
		}
		digits[k] = b[p]
	}
	// With a check digit of 0 in place, the Luhn sum mod 10 tells how far the
	// check digit must move.
	b[pos[len(pos)-1]] = byte('0' + (10-luhnSum(digits)%10)%10)
	return string(b)
}

// ipv4Pseudonym draws an address from 240.0.0.0/4 (reserved, never routed),
// avoiding 255 so the result is never the broadcast address.
func ipv4Pseudonym(sum *[sha256.Size]byte) [4]byte {
	if sum == nil {
		return [4]byte{240, 0, 0, 0}
	}
	return [4]byte{240 + sum[0]%15, sum[1], sum[2], sum[3]}
}

// ipv6Pseudonym draws an address from the 100::/64 discard prefix (RFC 6666).
func ipv6Pseudonym(sum *[sha256.Size]byte) netip.Addr {
	b := [16]byte{0: 0x01}
	if sum != nil {
		copy(b[8:], sum[:8])
	}
	return netip.AddrFrom16(b)
}

func formatIPv4(o [4]byte) string {
	b := make([]byte, 0, 15)
	for i, v := range o {
		if i > 0 {
			b = append(b, '.')
		}
		b = strconv.AppendUint(b, uint64(v), 10)
	}
	return string(b)
}

// IsPseudonym reports whether the whole of v is a pseudonym this package
// produces: tok_<16 hex>, user-<10 hex>@redacted.invalid,
// redacted.jwt.<16 hex>, redacted.secret.<16 hex>, a Luhn-valid card number
// starting with 0, 000-dd-dddd, a phone number whose digit runs all start
// with 0, an IPv4 address in 240.0.0.0-254.255.255.255 (also in its ::ffff:
// mapped form) or an IPv6 address in 100::/64. A value of that shape that
// still contains something Detect matches is not a pseudonym.
func IsPseudonym(v string) bool {
	return pseudonymShape(v) && len(Detect(v)) == 0
}

func pseudonymShape(v string) bool {
	switch {
	case strings.HasPrefix(v, tokenPrefix):
		return isTokenPseudonym(v)
	case strings.HasPrefix(v, emailLocalPrefix):
		local, domain, ok := strings.Cut(v, "@")
		return ok && domain == pseudoEmailDomain &&
			len(local) == len(emailLocalPrefix)+10 && isLowerHex(local[len(emailLocalPrefix):])
	case strings.HasPrefix(v, jwtPrefix):
		return isHex16(v[len(jwtPrefix):])
	case strings.HasPrefix(v, secretPrefix):
		return isHex16(v[len(secretPrefix):])
	}
	return isCardShape(v) || isSSNShape(v) || isPhoneShape(v) || isIPv4Shape(v) || isIPv6Shape(v)
}

func isHex16(s string) bool { return len(s) == 16 && isLowerHex(s) }

func isTokenPseudonym(v string) bool {
	return strings.HasPrefix(v, tokenPrefix) && isHex16(v[len(tokenPrefix):])
}

func isCardShape(v string) bool {
	if v == "" || v[0] != '0' {
		return false
	}
	var buf [maxCardDigits]byte
	d := buf[:0]
	for i := 0; i < len(v); i++ {
		c := v[i]
		if isDigit(c) {
			if len(d) == maxCardDigits {
				return false
			}
			d = append(d, c)
			continue
		}
		if c != ' ' && c != '-' || i+1 >= len(v) || !isDigit(v[i-1]) || !isDigit(v[i+1]) {
			return false
		}
	}
	return len(d) >= minCardDigits && luhnValid(d)
}

func isSSNShape(v string) bool {
	return len(v) == 11 && strings.HasPrefix(v, "000-") && v[6] == '-' && allDigits(v[4:6]) && allDigits(v[7:])
}

func isPhoneShape(v string) bool {
	switch {
	case v == "":
		return false
	case v[0] == '+':
		return isIntlPhoneShape(v)
	}
	end, ok := nanpAt(v, 0, false)
	return ok && end == len(v) && runsStartWithZero(v)
}

// isIntlPhoneShape reports whether v is "+" and 8-15 digits in runs that all
// start with 0, joined like an international number.
func isIntlPhoneShape(v string) bool {
	digits := 0
	for j := 1; ; {
		if j >= len(v) || v[j] != '0' {
			return false
		}
		k := j
		for k < len(v) && isDigit(v[k]) {
			k++
		}
		digits += k - j
		if k == len(v) {
			return digits >= minPhoneDigits && digits <= maxPhoneDigits
		}
		n := joinerLen(v, k)
		if n == 0 {
			return false
		}
		j = k + n
	}
}

func runsStartWithZero(v string) bool {
	for i := 0; i < len(v); i++ {
		if isDigit(v[i]) && (i == 0 || !isDigit(v[i-1])) && v[i] != '0' {
			return false
		}
	}
	return true
}

func isIPv4Shape(v string) bool {
	octets, end, ok := parseIPv4(v, 0)
	return ok && end == len(v) && formatIPv4(octets) == v && octets[0] >= 240 && octets[0] <= 254
}

func isIPv6Shape(v string) bool {
	// Canonical 100::/64 addresses start with "100:" and mapped IPv4 ones
	// with "::ffff:"; checking first spares ParseAddr's error allocation.
	if !strings.HasPrefix(v, "100:") && !strings.HasPrefix(v, "::ffff:") {
		return false
	}
	a, err := netip.ParseAddr(v)
	if err != nil || !a.Is6() || a.Zone() != "" || a.String() != v {
		return false
	}
	if a.Is4In6() {
		first := a.Unmap().As4()[0]
		return first >= 240 && first <= 254
	}
	return inDiscardPrefix(a)
}

// inDiscardPrefix reports whether a is in 100::/64.
func inDiscardPrefix(a netip.Addr) bool {
	b := a.As16()
	return b[0] == 0x01 && b[1] == 0 && b[2] == 0 && b[3] == 0 && b[4] == 0 && b[5] == 0 && b[6] == 0 && b[7] == 0
}
