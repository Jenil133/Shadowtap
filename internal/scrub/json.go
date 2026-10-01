package scrub

import (
	"bytes"
	"errors"
	"unicode"
	"unicode/utf16"
	"unicode/utf8"
)

// maxJSONDepth is the deepest nesting of objects and arrays that is
// scrubbed; deeper documents are dropped.
const maxJSONDepth = 128

var (
	errInvalidJSON = errors.New("scrub: invalid JSON")
	errJSONDepth   = errors.New("scrub: JSON nested too deeply")
)

// scrubJSON scrubs one JSON value by walking its tokens in order. Only the
// tokens that change are rewritten: keys keep their order, duplicates
// survive, numbers keep their exact text, nothing is HTML-escaped, and the
// input is returned as is (whitespace included) when nothing changed. The
// error never contains input.
func (s *Scrubber) scrubJSON(in []byte, rep Report) (out []byte, changed bool, err error) {
	w := jsonWalker{s: s, rep: rep, in: in, kinds: s.kinds & textDetectors}
	w.skipSpace()
	if err := w.value(false); err != nil {
		return nil, false, err
	}
	w.skipSpace()
	if w.pos != len(in) {
		return nil, false, errInvalidJSON
	}
	if w.out == nil {
		return in, false, nil
	}
	return append(w.out, in[w.done:]...), true, nil
}

type jsonWalker struct {
	s     *Scrubber
	rep   Report
	kinds kindSet
	in    []byte
	pos   int
	depth int
	out   []byte // rewritten output; nil while nothing has changed
	done  int    // in[:done] is accounted for in out
	buf   []byte // scratch space for unescaped strings
	enc   []byte // scratch space for re-encoded strings
}

// value walks the value at pos. Under a sensitive name every leaf is
// redacted.
func (w *jsonWalker) value(sensitive bool) error {
	switch c := w.peek(); {
	case c == '{':
		return w.object(sensitive)
	case c == '[':
		return w.array(sensitive)
	case c == '"':
		start := w.pos
		escaped, err := w.str()
		if err != nil {
			return err
		}
		w.scrubString(start, escaped, sensitive)
		return nil
	case c == '-' || isDigit(c):
		start := w.pos
		if err := w.num(); err != nil {
			return err
		}
		w.scrubNumber(start, sensitive)
		return nil
	case c == 't':
		return w.literal("true")
	case c == 'f':
		return w.literal("false")
	case c == 'n':
		return w.literal("null")
	}
	return errInvalidJSON
}

func (w *jsonWalker) object(sensitive bool) error {
	if w.depth++; w.depth > maxJSONDepth {
		return errJSONDepth
	}
	w.pos++
	w.skipSpace()
	if w.peek() == '}' {
		w.pos++
		w.depth--
		return nil
	}
	for {
		if w.peek() != '"' {
			return errInvalidJSON
		}
		start := w.pos
		escaped, err := w.str()
		if err != nil {
			return err
		}
		memberSensitive := w.scrubKey(start, escaped) || sensitive
		w.skipSpace()
		if w.peek() != ':' {
			return errInvalidJSON
		}
		w.pos++
		w.skipSpace()
		if err := w.value(memberSensitive); err != nil {
			return err
		}
		w.skipSpace()
		switch w.peek() {
		case ',':
			w.pos++
			w.skipSpace()
		case '}':
			w.pos++
			w.depth--
			return nil
		default:
			return errInvalidJSON
		}
	}
}

func (w *jsonWalker) array(sensitive bool) error {
	if w.depth++; w.depth > maxJSONDepth {
		return errJSONDepth
	}
	w.pos++
	w.skipSpace()
	if w.peek() == ']' {
		w.pos++
		w.depth--
		return nil
	}
	for {
		if err := w.value(sensitive); err != nil {
			return err
		}
		w.skipSpace()
		switch w.peek() {
		case ',':
			w.pos++
			w.skipSpace()
		case ']':
			w.pos++
			w.depth--
			return nil
		default:
			return errInvalidJSON
		}
	}
}

// peek returns the byte at pos, or 0 at the end of input.
func (w *jsonWalker) peek() byte {
	if w.pos < len(w.in) {
		return w.in[w.pos]
	}
	return 0
}

func (w *jsonWalker) skipSpace() {
	for w.pos < len(w.in) {
		switch w.in[w.pos] {
		case ' ', '\t', '\n', '\r':
			w.pos++
		default:
			return
		}
	}
}

func (w *jsonWalker) literal(lit string) error {
	end := w.pos + len(lit)
	if end > len(w.in) || string(w.in[w.pos:end]) != lit {
		return errInvalidJSON
	}
	w.pos = end
	return nil
}

// str validates the string at pos, moves past it and reports whether it
// contains escapes.
func (w *jsonWalker) str() (escaped bool, err error) {
	in := w.in
	i := w.pos + 1
	for i < len(in) {
		switch c := in[i]; {
		case c == '"':
			w.pos = i + 1
			return escaped, nil
		case c == '\\':
			escaped = true
			if i+1 >= len(in) {
				return false, errInvalidJSON
			}
			switch in[i+1] {
			case '"', '\\', '/', 'b', 'f', 'n', 'r', 't':
				i += 2
			case 'u':
				if i+6 > len(in) || !isHex4(in[i+2:i+6]) {
					return false, errInvalidJSON
				}
				i += 6
			default:
				return false, errInvalidJSON
			}
		case c < 0x20:
			return false, errInvalidJSON
		default:
			i++
		}
	}
	return false, errInvalidJSON
}

// num validates the number at pos (RFC 8259) and moves past it.
func (w *jsonWalker) num() error {
	in, i := w.in, w.pos
	if in[i] == '-' {
		i++
	}
	switch {
	case i < len(in) && in[i] == '0':
		i++
	case i < len(in) && in[i] >= '1' && in[i] <= '9':
		for i < len(in) && isDigit(in[i]) {
			i++
		}
	default:
		return errInvalidJSON
	}
	if i < len(in) && in[i] == '.' {
		i++
		if i >= len(in) || !isDigit(in[i]) {
			return errInvalidJSON
		}
		for i < len(in) && isDigit(in[i]) {
			i++
		}
	}
	if i < len(in) && (in[i] == 'e' || in[i] == 'E') {
		i++
		if i < len(in) && (in[i] == '+' || in[i] == '-') {
			i++
		}
		if i >= len(in) || !isDigit(in[i]) {
			return errInvalidJSON
		}
		for i < len(in) && isDigit(in[i]) {
			i++
		}
	}
	w.pos = i
	return nil
}

// unescaped returns the content of the string token raw (without quotes)
// with escapes resolved. The result may alias w.buf.
func (w *jsonWalker) unescaped(raw []byte, escaped bool) []byte {
	if !escaped {
		return raw
	}
	w.buf = appendUnquoted(w.buf[:0], raw)
	return w.buf
}

// scrubKey runs the detectors over the object key that ended at pos and
// reports whether its value is under a sensitive name.
func (w *jsonWalker) scrubKey(start int, escaped bool) bool {
	raw := w.in[start+1 : w.pos-1]
	key := w.unescaped(raw, escaped)
	if !mayContainPII(raw) && !mayContainPII(key) {
		return w.s.keyRules() && w.s.sensitiveNameBytes(key)
	}
	k := string(key)
	nk := w.s.text(k, w.rep)
	w.emitString(start, escaped, k, nk)
	return w.s.keyRules() && (w.s.sensitiveName(k) || nk != k && w.s.sensitiveName(nk))
}

// scrubString redacts the string value that ended at pos.
func (w *jsonWalker) scrubString(start int, escaped, sensitive bool) {
	raw := w.in[start+1 : w.pos-1]
	val := w.unescaped(raw, escaped)
	if sensitive && w.s.keyRules() {
		v := string(val)
		w.emitString(start, escaped, v, w.s.sensitiveValue(v, w.rep))
		return
	}
	if !mayContainPII(raw) && !mayContainPII(val) {
		return
	}
	v := string(val)
	w.emitString(start, escaped, v, w.s.text(v, w.rep))
}

// emitString writes the string token from start to pos, whose unescaped
// value v scrubbed to nv. The written token must be clean as encoded, not
// just as decoded: escapes such as \u0020 put letters and digits next to
// the text around them.
func (w *jsonWalker) emitString(start int, escaped bool, v, nv string) {
	if nv == v && (!escaped || len(detect(string(w.in[start+1:w.pos-1]), w.kinds)) == 0) {
		return
	}
	w.enc = appendJSONString(w.enc[:0], nv)
	if bytes.IndexByte(w.enc, '\\') >= 0 && len(detect(string(w.enc[1:len(w.enc)-1]), w.kinds)) > 0 {
		// Even minimal escaping leaves a match (a control character next to
		// an '@'); keep nothing of the value.
		w.rep.Add(KindSensitiveKey, 1)
		w.enc = appendJSONString(w.enc[:0], w.s.token(nv))
	}
	w.replace(start, w.pos, w.enc)
}

// scrubNumber redacts the number that ended at pos. Under a sensitive name
// any number becomes 0; elsewhere numbers are kept unless they hold
// something a detector matches, in practice a card number.
func (w *jsonWalker) scrubNumber(start int, sensitive bool) {
	tok := w.in[start:w.pos]
	if sensitive && w.s.keyRules() {
		if len(tok) != 1 || tok[0] != '0' {
			w.rep.Add(KindSensitiveKey, 1)
			w.replace(start, w.pos, []byte{'0'})
		}
		return
	}
	if len(tok) < minPhoneDigits { // too short for a card or a phone number
		return
	}
	if ms := detect(string(tok), w.kinds); len(ms) > 0 {
		w.rep.Add(ms[0].Kind, 1)
		w.replace(start, w.pos, []byte{'0'})
	}
}

// replace substitutes repl for in[start:end] in the output.
func (w *jsonWalker) replace(start, end int, repl []byte) {
	if w.out == nil {
		w.out = make([]byte, 0, len(w.in)+len(repl))
	}
	w.out = append(w.out, w.in[w.done:start]...)
	w.out = append(w.out, repl...)
	w.done = end
}

func isHex4(b []byte) bool {
	for _, c := range b {
		if !isDigit(c) && (c|0x20 < 'a' || c|0x20 > 'f') {
			return false
		}
	}
	return len(b) == 4
}

func hex4(b []byte) rune {
	var r rune
	for _, c := range b {
		switch {
		case isDigit(c):
			c -= '0'
		case c >= 'a':
			c -= 'a' - 10
		default:
			c -= 'A' - 10
		}
		r = r<<4 | rune(c)
	}
	return r
}

// appendUnquoted appends the unescaped content of a validated JSON string
// to dst. Like encoding/json, an unpaired surrogate becomes U+FFFD.
func appendUnquoted(dst, raw []byte) []byte {
	for i := 0; i < len(raw); {
		c := raw[i]
		if c != '\\' {
			dst = append(dst, c)
			i++
			continue
		}
		e := raw[i+1]
		i += 2
		switch e {
		case 'b':
			dst = append(dst, '\b')
		case 'f':
			dst = append(dst, '\f')
		case 'n':
			dst = append(dst, '\n')
		case 'r':
			dst = append(dst, '\r')
		case 't':
			dst = append(dst, '\t')
		case 'u':
			r := hex4(raw[i : i+4])
			i += 4
			if utf16.IsSurrogate(r) {
				dec := unicode.ReplacementChar
				if i+6 <= len(raw) && raw[i] == '\\' && raw[i+1] == 'u' {
					if d := utf16.DecodeRune(r, hex4(raw[i+2:i+6])); d != unicode.ReplacementChar {
						dec = d
						i += 6
					}
				}
				r = dec
			}
			dst = utf8.AppendRune(dst, r)
		default: // '"', '\\', '/'
			dst = append(dst, e)
		}
	}
	return dst
}

// appendJSONString appends s as a JSON string with minimal escaping: no HTML
// escaping, and invalid UTF-8 becomes U+FFFD.
func appendJSONString(dst []byte, s string) []byte {
	const hexDigits = "0123456789abcdef"
	dst = append(dst, '"')
	for i := 0; i < len(s); {
		c := s[i]
		if c < utf8.RuneSelf {
			switch {
			case c == '"' || c == '\\':
				dst = append(dst, '\\', c)
			case c == '\n':
				dst = append(dst, '\\', 'n')
			case c == '\r':
				dst = append(dst, '\\', 'r')
			case c == '\t':
				dst = append(dst, '\\', 't')
			case c == '\b':
				dst = append(dst, '\\', 'b')
			case c == '\f':
				dst = append(dst, '\\', 'f')
			case c < 0x20:
				dst = append(dst, '\\', 'u', '0', '0', hexDigits[c>>4], hexDigits[c&0xf])
			default:
				dst = append(dst, c)
			}
			i++
			continue
		}
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			dst = utf8.AppendRune(dst, unicode.ReplacementChar)
		} else {
			dst = append(dst, s[i:i+size]...)
		}
		i += size
	}
	return append(dst, '"')
}
