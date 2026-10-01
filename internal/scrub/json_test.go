package scrub

import (
	"encoding/json"
	"maps"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// scrubJSONBody runs a JSON body through Body and fails unless it is kept.
func scrubJSONBody(t *testing.T, s *Scrubber, in string, rep Report) string {
	t.Helper()
	res := s.Body(BodyInput{Data: []byte(in), ContentType: "application/json"}, rep)
	if res.Omitted != "" {
		t.Fatalf("Body(%s) omitted: %s", in, res.Omitted)
	}
	out := string(res.Data)
	if !json.Valid(res.Data) {
		t.Fatalf("Body(%s) = %s, not valid JSON", in, out)
	}
	assertClean(t, "JSON body", out)
	again := s.Body(BodyInput{Data: res.Data, ContentType: "application/json"}, nil)
	if string(again.Data) != out {
		t.Errorf("JSON scrubbing is not idempotent:\n%s\n%s", out, again.Data)
	}
	return out
}

func TestJSONFidelity(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		name, in string
	}{
		{"key order", `{"z":1,"a":2,"m":{"y":true,"b":null}}`},
		{"duplicate keys", `{"a":1,"a":2,"a":"three"}`},
		{"big integer", `{"id":12345678901234567890,"n":-0,"e":1E400}`},
		{"floats", `[1e-7,0.1,-2.50,3.0e+2,0.30000000000000004]`},
		{"unicode escapes", `{"s":"caf\u00e9 \ud83d\ude00 \u2028 \/ \"q\" \\"}`},
		{"html characters", `{"html":"<b>a & b</b> > c"}`},
		{"whitespace", "\n  {\n\t\"a\" : [ 1 ,\r\n 2 ] ,  \"b\":\"x\"  }  \n"},
		{"scalar string", `"hello"`},
		{"scalar number", `42`},
		{"empty containers", `{"a":{},"b":[],"c":""}`},
		{"invalid utf-8 kept", "{\"a\":\"\xff\xfe\"}"},
		{"lone surrogate kept", `{"a":"\ud800"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := Report{}
			if out := scrubJSONBody(t, s, tt.in, rep); out != tt.in || len(rep) != 0 {
				t.Errorf("unchanged document was rewritten:\n in: %q\nout: %q\nreport %v", tt.in, out, rep)
			}
		})
	}
}

func TestJSONMinimalChange(t *testing.T) {
	s := newTestScrubber(t)
	in := "{\n  \"z\": \"<keep> & \\u0041\",\n  \"email\":   \"alice@example.com\",\n  \"n\": 12345678901234567890,\n  \"z\": [1, 2]\n}"
	out := scrubJSONBody(t, s, in, nil)
	if !matches(`^\{\n  "z": "<keep> & \\u0041",\n  "email":   "user-[0-9a-f]{10}@redacted\.invalid",\n  "n": 12345678901234567890,\n  "z": \[1, 2\]\n\}$`, out) {
		t.Errorf("output changed more than the email:\n%s", out)
	}
}

func TestJSONEscapedPII(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		name, in, pattern string
	}{
		{"escaped at sign", `{"note":"mail alice\u0040example.com"}`, `^\{"note":"mail user-[0-9a-f]{10}@redacted\.invalid"\}$`},
		{"escaped digits", `{"note":"card \u0034111111111111111"}`, `^\{"note":"card 0\d{15}"\}$`},
		{"newline escape before digits", `{"note":"card:\n4111111111111111"}`, `^\{"note":"card:\\n0\d{15}"\}$`},
		{"html and pii", `{"note":"<a href=\"mailto:alice@example.com\">x</a>"}`,
			`^\{"note":"<a href=\\"mailto:user-[0-9a-f]{10}@redacted\.invalid\\">x</a>"\}$`},
		// Decoded the value has no address, but the escape's hex digits
		// would form one; minimal re-encoding drops the escape.
		{"escape forming a local part", `{"a":"\u0020@example.com"}`, `^\{"a":" @example\.com"\}$`},
		// A control character must stay escaped, so the value is dropped.
		{"control character before at sign", `{"a":"\u001f@example.com"}`, `^\{"a":"tok_[0-9a-f]{16}"\}$`},
		{"pii in key", `{"alice@example.com":{"role":"admin"}}`, `^\{"user-[0-9a-f]{10}@redacted\.invalid":\{"role":"admin"\}\}$`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if out := scrubJSONBody(t, s, tt.in, nil); !matches(tt.pattern, out) {
				t.Errorf("got %s, want %s", out, tt.pattern)
			}
		})
	}
}

func TestJSONNumbers(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		in, out string
		report  Report
	}{
		{`{"total":4111111111111111}`, `{"total":0}`, Report{KindCard: 1}},
		{`{"total":-4111111111111111}`, `{"total":0}`, Report{KindCard: 1}},
		{`{"total":4111111111111112}`, `{"total":4111111111111112}`, Report{}},
		{`{"ts":1759449600000,"id":1580661436132757504}`, `{"ts":1759449600000,"id":1580661436132757504}`, Report{}},
		{`{"x":0.4111111111111111,"y":4111111111111111.5}`, `{"x":0.4111111111111111,"y":4111111111111111.5}`, Report{}},
		{`{"exp":1e+14155552671}`, `{"exp":0}`, Report{KindPhone: 1}},
		{`{"pin":1234,"zip":0,"lat":37.7749,"lng":-122.4194,"ok":true,"none":null}`,
			`{"pin":0,"zip":0,"lat":0,"lng":0,"ok":true,"none":null}`, Report{KindSensitiveKey: 3}}, // 0 stays 0
	}
	for _, tt := range tests {
		rep := Report{}
		if out := scrubJSONBody(t, s, tt.in, rep); out != tt.out {
			t.Errorf("%s -> %s, want %s", tt.in, out, tt.out)
		}
		if !maps.Equal(rep, tt.report) {
			t.Errorf("%s: report %v, want %v", tt.in, rep, tt.report)
		}
	}
}

func TestJSONSensitiveKeys(t *testing.T) {
	s := newTestScrubber(t)
	const tok = `"tok_[0-9a-f]{16}"`
	tests := []struct {
		name, in, pattern string
	}{
		{"exact", `{"password":"hunter2","token":"abc","ssn":"123456789"}`,
			`^\{"password":` + tok + `,"token":` + tok + `,"ssn":"000-\d\d-\d{4}"\}$`},
		{"suffix", `{"billingEmail":"a@b.co","user_email":"bob@example.org","customerPhone":"(415) 555-2671","billing-address":"1 Main St"}`,
			`^\{"billingEmail":"user-[0-9a-f]{10}@redacted\.invalid","user_email":"user-[0-9a-f]{10}@redacted\.invalid","customerPhone":` + tok + `,"billing-address":` + tok + `\}$`},
		{"not sensitive", `{"pageToken":"abc123xyz","relationship":"friend","name":"Alice","description":"x"}`,
			`^\{"pageToken":"abc123xyz","relationship":"friend","name":"Alice","description":"x"\}$`},
		{"case and separators", `{"First_Name":"Alice","LAST-NAME":"Smith","Date.Of.Birth":"1990-01-02"}`,
			`^\{"First_Name":` + tok + `,"LAST-NAME":` + tok + `,"Date\.Of\.Birth":` + tok + `\}$`},
		{"object subtree", `{"address":{"street":"1 Main St","zip":"94105","geo":{"lat":37.7,"verified":true,"note":null}}}`,
			`^\{"address":\{"street":` + tok + `,"zip":` + tok + `,"geo":\{"lat":0,"verified":true,"note":null\}\}\}$`},
		{"array subtree", `{"phone":["415-555-2671","n/a",5552671,""]}`,
			`^\{"phone":\[` + tok + `,` + tok + `,0,""\]\}$`},
		{"escaped key", `{"pass\u0077ord":"hunter2"}`, `^\{"pass\\u0077ord":` + tok + `\}$`},
		{"existing pseudonyms kept", `{"password":"tok_0123456789abcdef","email":"user-0123456789@redacted.invalid"}`,
			`^\{"password":"tok_0123456789abcdef","email":"user-0123456789@redacted\.invalid"\}$`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if out := scrubJSONBody(t, s, tt.in, nil); !matches(tt.pattern, out) {
				t.Errorf("got  %s\nwant %s", out, tt.pattern)
			}
		})
	}
}

func TestJSONAllowAndExtraKeys(t *testing.T) {
	s := newTestScrubber(t, func(c *Config) {
		c.AllowKeys = []string{"Email", "zip"}
		c.SensitiveKeys = []string{"customer_ref"}
		c.SensitiveKeySuffixes = []string{"Nickname"}
	})
	in := `{"email":"x","billingEmail":"y","zip":"94105","customerRef":"C-1","userNickname":"ace","password":"p"}`
	out := scrubJSONBody(t, s, in, nil)
	want := `^\{"email":"x","billingEmail":"y","zip":"94105","customerRef":"tok_[0-9a-f]{16}","userNickname":"tok_[0-9a-f]{16}","password":"tok_[0-9a-f]{16}"\}$`
	if !matches(want, out) {
		t.Errorf("got %s", out)
	}
}

func TestJSONSensitiveRulesDisabled(t *testing.T) {
	s := newTestScrubber(t, func(c *Config) { c.DisabledDetectors = []string{KindSensitiveKey} })
	in := `{"password":"hunter2","email":"alice@example.com","card":4111111111111111,"pin":1234}`
	out := scrubJSONBody(t, s, in, nil)
	if !matches(`^\{"password":"hunter2","email":"user-[0-9a-f]{10}@redacted\.invalid","card":0,"pin":1234\}$`, out) {
		t.Errorf("got %s", out)
	}
}

func TestJSONFailsClosed(t *testing.T) {
	s := newTestScrubber(t)
	deep := strings.Repeat("[", maxJSONDepth+1) + strings.Repeat("]", maxJSONDepth+1)
	ok := strings.Repeat(`{"a":`, maxJSONDepth) + "1" + strings.Repeat("}", maxJSONDepth)
	if res := s.Body(BodyInput{Data: []byte(ok), ContentType: "application/json"}, nil); res.Omitted != "" {
		t.Errorf("depth %d omitted: %s", maxJSONDepth, res.Omitted)
	}
	for _, in := range []string{
		deep,
		`{"email":"alice@example.com"`,
		`{"email":"alice@example.com"} {"b":1}`,
		`{"a":1,}`,
		`[1 2]`,
		`{"a" 1}`,
		`{'a':1}`,
		`{"a":01}`,
		`{"a":1.}`,
		`{"a":.5}`,
		`{"a":+1}`,
		`{"a":tru}`,
		`{"a":"\x"}`,
		`{"a":"\u12"}`,
		"{\"a\":\"tab\there\"}",
		"\xef\xbb\xbf{}",
		`   `,
		`NaN`,
	} {
		res := s.Body(BodyInput{Data: []byte(in), ContentType: "application/json"}, nil)
		if res.Omitted != capture.OmittedUnparseable || res.Data != nil {
			t.Errorf("Body(%.40q) = %q omitted %q, want omitted %q", in, res.Data, res.Omitted, capture.OmittedUnparseable)
		}
		if json.Valid([]byte(in)) && in != deep {
			t.Errorf("%.40q is valid JSON but was rejected", in)
		}
	}
}

func TestNDJSON(t *testing.T) {
	s := newTestScrubber(t)
	in := "{\"email\":\"alice@example.com\"}\r\n\n  \n{\"n\":1}\n[\"4111 1111 1111 1111\"]"
	rep := Report{}
	res := s.Body(BodyInput{Data: []byte(in), ContentType: "application/x-ndjson"}, rep)
	if res.Omitted != "" {
		t.Fatalf("omitted: %s", res.Omitted)
	}
	want := `^\{"email":"user-[0-9a-f]{10}@redacted\.invalid"\}\r\n\n  \n\{"n":1\}\n\["0\d{3} \d{4} \d{4} \d{4}"\]$`
	if !matches(want, string(res.Data)) {
		t.Errorf("got %q", res.Data)
	}
	if !maps.Equal(rep, Report{KindEmail: 1, KindCard: 1}) {
		t.Errorf("report = %v", rep)
	}
	bad := s.Body(BodyInput{Data: []byte("{\"a\":1}\n{\"email\":\"alice@example.com\"\n"), ContentType: "application/jsonl"}, nil)
	if bad.Omitted != capture.OmittedUnparseable {
		t.Errorf("bad line: omitted %q, want unparseable", bad.Omitted)
	}
	same := []byte("{\"a\":1}\n{\"b\":2}\n")
	if res := s.Body(BodyInput{Data: same, ContentType: "application/jsonlines"}, nil); string(res.Data) != string(same) {
		t.Errorf("unchanged NDJSON rewritten: %q", res.Data)
	}
}
