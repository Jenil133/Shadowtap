package scrub

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// Scrubber removes personal data from captured traffic. Create one with New;
// it is immutable afterwards and safe for concurrent use.
type Scrubber struct {
	key        []byte
	kinds      kindSet             // enabled detectors and rules
	exact      map[string]struct{} // sensitive names, normalized
	suffixes   []string            // sensitive name suffixes, normalized
	credential map[string]struct{} // header names, lowercase
	ipHeaders  map[string]struct{}
	urlHeaders map[string]struct{}
	drop       map[string]struct{}
	textTypes  map[string]struct{} // media types, lowercase
	maxDecoded int

	// macs holds HMAC states for reuse: a hash.Hash is not safe for
	// concurrent use, and building one per pseudonym is wasteful.
	macs sync.Pool
}

// New validates cfg and returns a Scrubber. The key must be at least
// MinKeyBytes long. The lists in cfg are added to Defaults(); AllowKeys then
// removes names from both sensitive lists. Field names are normalized to
// lowercase a-z0-9, header names and media types to lowercase.
// DisabledDetectors must name Kind* constants.
func New(cfg Config) (*Scrubber, error) {
	var errs []error
	if len(cfg.Key) < MinKeyBytes {
		errs = append(errs, fmt.Errorf("key has %d bytes, need at least %d", len(cfg.Key), MinKeyBytes))
	}
	if cfg.MaxDecodedBytes < 0 {
		errs = append(errs, fmt.Errorf("max_decoded_bytes %d is negative", cfg.MaxDecodedBytes))
	}
	def := Defaults()
	sets := func(field string, norm func(string) string, lists ...[]string) map[string]struct{} {
		set := make(map[string]struct{})
		for _, list := range lists {
			for _, name := range list {
				n := norm(name)
				if n == "" {
					errs = append(errs, fmt.Errorf("%s: %q is empty after normalization", field, name))
					continue
				}
				set[n] = struct{}{}
			}
		}
		return set
	}
	s := &Scrubber{
		key:        bytes.Clone(cfg.Key),
		kinds:      allKinds,
		exact:      sets("sensitive_keys", normalizeKey, def.SensitiveKeys, cfg.SensitiveKeys),
		credential: sets("credential_headers", normalizeHeader, def.CredentialHeaders, cfg.CredentialHeaders),
		ipHeaders:  sets("ip_headers", normalizeHeader, def.IPHeaders, cfg.IPHeaders),
		urlHeaders: sets("url_headers", normalizeHeader, def.URLHeaders, cfg.URLHeaders),
		drop:       sets("drop_headers", normalizeHeader, def.DropHeaders, cfg.DropHeaders),
		textTypes:  sets("text_content_types", mediaType, def.TextContentTypes, cfg.TextContentTypes),
		maxDecoded: cfg.MaxDecodedBytes,
	}
	if s.maxDecoded == 0 {
		s.maxDecoded = DefaultMaxDecodedBytes
	}
	suffixes := sets("sensitive_key_suffixes", normalizeKey, def.SensitiveKeySuffixes, cfg.SensitiveKeySuffixes)
	for k := range sets("allow_keys", normalizeKey, cfg.AllowKeys) {
		delete(s.exact, k)
		delete(suffixes, k)
	}
	for k := range suffixes {
		s.suffixes = append(s.suffixes, k)
	}
	slices.Sort(s.suffixes)
	for _, d := range cfg.DisabledDetectors {
		bit := kindBit(strings.ToLower(strings.TrimSpace(d)))
		if bit == 0 {
			errs = append(errs, fmt.Errorf("disabled_detectors: unknown kind %q", d))
			continue
		}
		s.kinds &^= bit
	}
	if err := errors.Join(errs...); err != nil {
		return nil, fmt.Errorf("scrub: %w", err)
	}
	key := s.key
	s.macs.New = func() any { return &macState{h: hmac.New(sha256.New, key)} }
	return s, nil
}

func normalizeKey(name string) string { return string(appendNormalizedKey(nil, name)) }

func normalizeHeader(name string) string { return strings.ToLower(strings.TrimSpace(name)) }

// appendNormalizedKey appends name lowercased and stripped of everything but
// a-z and 0-9: "billing_Email" becomes "billingemail".
func appendNormalizedKey[T string | []byte](dst []byte, name T) []byte {
	for i := 0; i < len(name); i++ {
		switch c := name[i]; {
		case 'a' <= c && c <= 'z', '0' <= c && c <= '9':
			dst = append(dst, c)
		case 'A' <= c && c <= 'Z':
			dst = append(dst, c+'a'-'A')
		}
	}
	return dst
}

// sensitiveName reports whether values under a field, query or form name
// are always redacted.
func (s *Scrubber) sensitiveName(name string) bool {
	var buf [64]byte
	return s.sensitiveNormalized(appendNormalizedKey(buf[:0], trimIndexes(name)))
}

// trimIndexes drops trailing array subscripts from a form or query name:
// phone[0] and user[phones][] are named phone and user[phones].
func trimIndexes(name string) string {
	for strings.HasSuffix(name, "]") {
		i := strings.LastIndexByte(name, '[')
		if i < 0 || strings.Trim(name[i+1:len(name)-1], "0123456789") != "" {
			break
		}
		name = name[:i]
	}
	return name
}

func (s *Scrubber) sensitiveNameBytes(name []byte) bool {
	var buf [64]byte
	return s.sensitiveNormalized(appendNormalizedKey(buf[:0], name))
}

func (s *Scrubber) sensitiveNormalized(n []byte) bool {
	if len(n) == 0 {
		return false
	}
	if _, ok := s.exact[string(n)]; ok {
		return true
	}
	for _, suf := range s.suffixes {
		if len(n) >= len(suf) && string(n[len(n)-len(suf):]) == suf {
			return true
		}
	}
	return false
}

// Exchange scrubs x in place: the request path, the headers and trailers of
// both directions and both bodies, and adds the redaction counts to
// x.Redactions (left nil when nothing was redacted). It is idempotent, so a
// scrubbed exchange can be scrubbed again. Each part fails closed: should
// scrubbing a part panic, that part is dropped rather than kept raw.
func (s *Scrubber) Exchange(x *capture.Exchange) {
	if x == nil {
		return
	}
	rep := Report{}
	req, resp := &x.Request, &x.Response
	// Bodies first: they need the original Content-Type and
	// Content-Encoding headers.
	s.exchangeBody(&req.Body, req.Headers, rep)
	s.exchangeBody(&resp.Body, resp.Headers, rep)
	guard(rep, func(r Report) { req.Path = s.Path(req.Path, r) }, func() { req.Path = "/" })
	for _, h := range []*capture.Headers{&req.Headers, &req.Trailers, &resp.Headers, &resp.Trailers} {
		guard(rep, func(r Report) { *h = s.Headers(*h, r) }, func() { *h = nil })
	}
	for k, n := range rep {
		if n <= 0 {
			continue
		}
		if x.Redactions == nil {
			x.Redactions = make(map[string]int, len(rep))
		}
		x.Redactions[k] += n
	}
}

// guard runs f with a fresh report and merges the counts into rep. If f
// panics, failClosed runs instead and the counts are discarded: a scrubber
// bug must lose data rather than leak it.
func guard(rep Report, f func(Report), failClosed func()) {
	local := Report{}
	defer func() {
		if recover() != nil {
			failClosed()
			return
		}
		for k, n := range local {
			rep.Add(k, n)
		}
	}()
	f(local)
}

// exchangeBody scrubs one body of an exchange in place. headers are the
// direction's original headers.
func (s *Scrubber) exchangeBody(b *capture.Body, headers capture.Headers, rep Report) {
	guard(rep, func(r Report) {
		ct := b.ContentType
		b.ContentType = s.text(ct, r)
		if b.Omitted != "" {
			b.Omit(b.Omitted) // already dropped; make sure no data is left
			return
		}
		raw, err := b.Bytes()
		if err != nil {
			b.Omit(capture.OmittedUnparseable)
			return
		}
		in := BodyInput{Data: raw, ContentType: ct, Truncated: b.Truncated}
		if in.ContentType == "" {
			in.ContentType = headers.Get("content-type")
		}
		// A body scrubbed before holds decoded data already.
		if b.DecodedFrom == "" {
			in.ContentEncoding = strings.Join(headers["content-encoding"], ",")
		}
		res := s.Body(in, r)
		if res.Omitted != "" {
			b.Omit(res.Omitted)
		} else {
			b.SetData(res.Data)
		}
		if res.DecodedFrom != "" {
			b.DecodedFrom = res.DecodedFrom
		}
		b.Truncated = res.Truncated
	}, func() { b.Omit(capture.OmittedUnparseable) })
}
