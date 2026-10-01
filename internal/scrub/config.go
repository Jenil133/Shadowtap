// Package scrub removes personal data from captured HTTP exchanges before they
// are stored.
//
// Values are replaced with keyed pseudonyms (HMAC-SHA256): the same input
// always maps to the same pseudonym, so replays, diffs and joins keep working,
// but the original cannot be recovered without the key. Pseudonyms keep the
// shape of what they replace where replay needs it (an email stays an email),
// and carry a marker real data never has, so scrubbing is idempotent. Anything
// that cannot be parsed or decoded safely is dropped rather than stored.
package scrub

// Redaction kinds. They are used as metric labels and as keys of
// capture.Exchange.Redactions.
const (
	KindSecret       = "secret"
	KindJWT          = "jwt"
	KindEmail        = "email"
	KindCard         = "card"
	KindSSN          = "ssn"
	KindPhone        = "phone"
	KindIPv4         = "ipv4"
	KindIPv6         = "ipv6"
	KindSensitiveKey = "sensitive_key"
	KindCredential   = "credential"
)

// MinKeyBytes is the shortest accepted HMAC key.
const MinKeyBytes = 16

// DefaultMaxDecodedBytes caps decompressed body size.
const DefaultMaxDecodedBytes = 1 << 20

// Config controls the scrubber. List fields are added to the built-in
// defaults (see Defaults); AllowKeys and DisabledDetectors are the only ways
// to switch a built-in protection off.
type Config struct {
	// Key is the HMAC key for pseudonyms, at least MinKeyBytes long. It is
	// loaded from a secret, never from the config file.
	Key []byte `json:"-"`

	// SensitiveKeys are field, query and form names whose values are always
	// redacted, matched exactly after normalization (lowercase, only a-z0-9).
	SensitiveKeys []string `json:"sensitive_keys,omitempty"`
	// SensitiveKeySuffixes match normalized names by suffix ("billingEmail").
	SensitiveKeySuffixes []string `json:"sensitive_key_suffixes,omitempty"`
	// AllowKeys removes normalized names from both sensitive lists.
	AllowKeys []string `json:"allow_keys,omitempty"`

	// CredentialHeaders have their whole value tokenized (an auth scheme
	// prefix such as "Bearer" is kept).
	CredentialHeaders []string `json:"credential_headers,omitempty"`
	// IPHeaders hold client addresses; each address is pseudonymized.
	IPHeaders []string `json:"ip_headers,omitempty"`
	// URLHeaders hold URLs and are scrubbed like a request path.
	URLHeaders []string `json:"url_headers,omitempty"`
	// DropHeaders are removed entirely.
	DropHeaders []string `json:"drop_headers,omitempty"`

	// TextContentTypes are media types scrubbed as free text.
	TextContentTypes []string `json:"text_content_types,omitempty"`
	// DisabledDetectors turns detectors off by kind (Kind* constants).
	DisabledDetectors []string `json:"disabled_detectors,omitempty"`
	// MaxDecodedBytes caps decompressed bodies; 0 means DefaultMaxDecodedBytes.
	MaxDecodedBytes int `json:"max_decoded_bytes,omitempty"`
}

// Report counts redactions by kind. It never holds values.
type Report map[string]int

// Add records n redactions of kind. It is a no-op on a nil Report.
func (r Report) Add(kind string, n int) {
	if r != nil && n > 0 {
		r[kind] += n
	}
}

// BodyInput is a raw body as seen by the tap.
type BodyInput struct {
	Data            []byte
	ContentType     string // Content-Type header value
	ContentEncoding string // Content-Encoding header value
	Truncated       bool   // the tap kept only a prefix of the body
}

// BodyResult is a scrubbed body.
type BodyResult struct {
	Data        []byte // scrubbed data; nil when Omitted is set
	Omitted     string // capture.Omitted* reason when the data is not kept
	DecodedFrom string // content-encoding that was removed, e.g. "gzip"
	Truncated   bool   // the kept data is a prefix of the real body
}

// Match is one detected PII value in a string.
type Match struct {
	Kind       string
	Start, End int // byte offsets
}
