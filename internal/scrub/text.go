package scrub

import "strings"

const (
	// maxTextPasses bounds the detect-and-replace passes Text makes. A
	// replacement can complete a match with neighbouring text (a card
	// pseudonym next to more digits), so Text repeats until nothing matches.
	maxTextPasses = 8
	// maxFailClosedPasses bounds the passes that replace leftovers with
	// redactedMarker before the whole value is dropped.
	maxFailClosedPasses = 64
	// redactedMarker replaces matches when pseudonyms do not converge. No
	// detector matches it and its brackets end every detector's match.
	redactedMarker = "[redacted]"
)

// Text replaces the personal data that the enabled detectors find in t with
// pseudonyms and counts each replacement in rep. It repeats until the result
// is clean, so Text(Text(t)) == Text(t).
func (s *Scrubber) Text(t string, rep Report) string {
	return s.text(t, rep)
}

func (s *Scrubber) text(t string, rep Report) string {
	kinds := s.kinds & textDetectors
	for range maxTextPasses {
		prev := t
		t = s.encodedEmails(s.assignments(t, rep), rep)
		if ms := detect(t, kinds); len(ms) > 0 {
			t = s.replace(t, ms, rep)
		}
		if t == prev {
			return t
		}
	}
	return redactRemaining(t, kinds, rep)
}

// replace substitutes a pseudonym for every match.
func (s *Scrubber) replace(t string, ms []Match, rep Report) string {
	var b strings.Builder
	b.Grow(len(t) + 24*len(ms))
	last := 0
	for _, m := range ms {
		b.WriteString(t[last:m.Start])
		b.WriteString(s.pseudonym(m.Kind, t[m.Start:m.End]))
		rep.Add(m.Kind, 1)
		last = m.End
	}
	b.WriteString(t[last:])
	return b.String()
}

// redactRemaining is the fail-closed end of Text: whatever the detectors
// still find is replaced with redactedMarker, and if even that does not
// settle, nothing of t is kept.
func redactRemaining(t string, kinds kindSet, rep Report) string {
	for range maxFailClosedPasses {
		ms := detect(t, kinds)
		if len(ms) == 0 {
			return t
		}
		var b strings.Builder
		last := 0
		for _, m := range ms {
			b.WriteString(t[last:m.Start])
			b.WriteString(redactedMarker)
			rep.Add(m.Kind, 1)
			last = m.End
		}
		b.WriteString(t[last:])
		t = b.String()
	}
	return redactedMarker
}

// keyRules reports whether values under sensitive names are redacted.
func (s *Scrubber) keyRules() bool { return s.kinds&setSensitiveKey != 0 }

// sensitiveValue redacts a string found under a sensitive name. Empty
// strings and existing pseudonyms are kept, a value that is exactly one
// detector match gets that detector's format-preserving pseudonym (bare
// nine-digit SSNs included; cards and phones get a token, see below), and
// anything else becomes tok_<16 hex>.
func (s *Scrubber) sensitiveValue(v string, rep Report) string {
	if v == "" || strongPseudonym(v) {
		return v
	}
	if kind := s.wholeMatch(v); kind != "" {
		rep.Add(kind, 1)
		if kind == KindCard || kind == KindPhone {
			// Their pseudonyms are only a shape (a leading 0 and a valid
			// check digit), which a real account or national id number can
			// have too, so under a sensitive name they could not be told
			// apart from data and would be kept. A token can.
			return s.token(v)
		}
		return s.pseudonym(kind, v)
	}
	rep.Add(KindSensitiveKey, 1)
	return s.token(v)
}

// wholeMatch returns the kind of the single detector match spanning all of
// v, or "".
func (s *Scrubber) wholeMatch(v string) string {
	ms := detect(v, s.kinds&textDetectors)
	if len(ms) == 1 && ms[0].Start == 0 && ms[0].End == len(v) {
		return ms[0].Kind
	}
	if len(ms) == 0 && s.kinds&setSSN != 0 && isBareSSN(v) {
		return KindSSN
	}
	return ""
}

// scrubValue applies the sensitive-name rules when sensitive is set and the
// text detectors otherwise.
func (s *Scrubber) scrubValue(v string, sensitive bool, rep Report) string {
	if sensitive && s.keyRules() {
		return s.sensitiveValue(v, rep)
	}
	return s.text(v, rep)
}

// strongPseudonym reports whether v is a pseudonym that real data cannot
// look like: IsPseudonym without the card and phone shapes, which any number
// starting with 0 and passing the Luhn check (or written in zero-led digit
// runs, like an 00-prefixed international number) also has.
func strongPseudonym(v string) bool {
	return pseudonymShape(v) && !isCardShape(v) && !isPhoneShape(v) && len(Detect(v)) == 0
}
