package scrub

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// The tap calls the scrubber through tap.Scrubber; this is its method set,
// spelled out so the test does not depend on the tap package.
var _ interface{ Exchange(*capture.Exchange) } = (*Scrubber)(nil)

func TestNewValidation(t *testing.T) {
	key := []byte(testKey)
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{"defaults", Config{Key: key}, ""},
		{"defaults passed back in", func() Config { c := Defaults(); c.Key = key; return c }(), ""},
		{"no key", Config{}, "key has 0 bytes"},
		{"short key", Config{Key: key[:MinKeyBytes-1]}, "need at least 16"},
		{"minimum key", Config{Key: key[:MinKeyBytes]}, ""},
		{"unknown detector", Config{Key: key, DisabledDetectors: []string{"email", "creditcard"}}, `unknown kind "creditcard"`},
		{"every kind can be disabled", Config{Key: key, DisabledDetectors: []string{
			KindSecret, KindJWT, KindEmail, KindCard, KindSSN, KindPhone, KindIPv4, KindIPv6, KindSensitiveKey, KindCredential,
		}}, ""},
		{"empty key name", Config{Key: key, SensitiveKeys: []string{"--"}}, "sensitive_keys"},
		{"empty header name", Config{Key: key, DropHeaders: []string{" "}}, "drop_headers"},
		{"negative cap", Config{Key: key, MaxDecodedBytes: -1}, "max_decoded_bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, err := New(tt.cfg)
			switch {
			case tt.wantErr == "" && err != nil:
				t.Fatalf("New: %v", err)
			case tt.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tt.wantErr)):
				t.Fatalf("New error = %v, want it to mention %q", err, tt.wantErr)
			case tt.wantErr == "" && s == nil:
				t.Fatal("New returned nil")
			}
		})
	}
}

func TestNewErrorDoesNotLeakKey(t *testing.T) {
	_, err := New(Config{Key: []byte("short-secret")})
	if err == nil || strings.Contains(err.Error(), "short-secret") {
		t.Errorf("error %v exposes the key", err)
	}
}

func TestNewCopiesKey(t *testing.T) {
	key := []byte(testKey)
	s, err := New(Config{Key: key})
	if err != nil {
		t.Fatal(err)
	}
	before := s.Text("alice@example.com", nil)
	key[0] ^= 0xff
	if after := s.Text("alice@example.com", nil); after != before {
		t.Error("changing the caller's key slice changed the scrubber")
	}
}

func TestDefaultsAreFresh(t *testing.T) {
	a := Defaults()
	n := len(a.CredentialHeaders)
	a.SensitiveKeys[0] = "changed"
	a.CredentialHeaders = append(a.CredentialHeaders, "x")
	b := Defaults()
	if b.SensitiveKeys[0] == "changed" || len(b.CredentialHeaders) != n {
		t.Error("Defaults returned shared slices")
	}
	if b.Key != nil || b.MaxDecodedBytes != DefaultMaxDecodedBytes {
		t.Errorf("Defaults key %v cap %d", b.Key, b.MaxDecodedBytes)
	}
	for _, n := range append(b.SensitiveKeys, b.SensitiveKeySuffixes...) {
		if normalizeKey(n) != n {
			t.Errorf("default name %q is not normalized", n)
		}
	}
}

func TestSensitiveNames(t *testing.T) {
	s := newTestScrubber(t)
	tests := map[string]bool{
		"password": true, "Password": true, "user_password": true, "passwd": true, "pwd": true,
		"email": true, "user_email": true, "billingEmail": true, "contact-email-address": true,
		"phone": true, "customerPhone": true, "mobile_number": true, "mobileNumber": true,
		"ssn": true, "taxId": true, "dob": true, "date_of_birth": true, "firstName": true,
		"last-name": true, "ip": true, "client_ip_address": true, "zip": true, "lat": true,
		"X-Api-Key": true, "access_token": true, "refreshToken": true, "client_secret": true,
		"cvv2": true, "addressLine1": true, "billing.address": true, "sessionId": true,
		"pageToken": false, "nextPageToken": false, "relationship": false, "tip": false,
		"name": false, "emails": true, "description": false, "status": false, "id": false,
		"mobiles": true, "phones": true, "userName": true, "authToken": true, "x-session-token": true,
		"client_ip": true, "secretAccessKey": true, "zipper": false, "": false, "___": false,
	}
	for name, want := range tests {
		if got := s.sensitiveName(name); got != want {
			t.Errorf("sensitiveName(%q) = %v, want %v", name, got, want)
		}
	}
}

// exchangeFixture builds a realistic exchange with personal data in the
// path, query, headers, a JSON request and a gzipped JSON response. It
// returns the exchange, the planted raw values and the expected counts.
func exchangeFixture(t testing.TB) (*capture.Exchange, []string, Report) {
	t.Helper()
	reqBody := `{"email":"alice@example.com","password":"hunter2","firstName":"Alice","card":{"number":"4111 1111 1111 1111","cvv":"737"},` +
		`"note":"call me at (415) 555-2671 or 415.555.2671","items":[{"sku":"A-1","qty":2,"price":19.99}],"orderTotal":4111111111111111}`
	respBody := `{"id":"u_42","email":"alice@example.com","phone":"+1 415 555 2671","ssn":"123-45-6789",` +
		`"address":{"street":"1 Main St","zip":"94105"},"lastLogin":{"ip":"203.0.113.7","at":"2026-10-03T12:00:00Z"},` +
		`"bio":"Reach me at alice@example.com"}`
	start := time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            "01928c3a-7b2e-7cc1-9e5f-3a1b2c3d4e5f",
		Service:       "demo-api",
		TraceID:       "4bf92f3577b34da6a3ce929d0e0e4736",
		Source:        capture.Source{Tap: "tap-0", Mode: capture.ModeObserve},
		Timing:        capture.Timing{StartedAt: start, DurationMicros: 1200},
		Request: capture.Request{
			Method:    "POST",
			Scheme:    "https",
			Authority: "demo-api:8080",
			Path:      "/api/users/alice%40example.com/orders?email=bob%40example.org&page=2&access_token=s3cr3t-session-token",
			Headers: capture.Headers{
				"authorization":         {"Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJhbGljZSJ9.c2lnbmF0dXJl"},
				"cookie":                {"session=s3cr3t-session-token; theme=dark"},
				"x-forwarded-for":       {"203.0.113.7, 2001:db8::1, 10.1.2.3"},
				"referer":               {"https://shop.example.com/checkout?phone=%2B14155552671&step=2"},
				"user-agent":            {"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"},
				"content-type":          {"application/json"},
				"x-envoy-peer-metadata": {"ChQKDkFQUF9DT05UQUlORVJTEgIaAA=="},
				"x-user-email":          {"carol@example.net"},
			},
			Body: capture.Body{ContentType: "application/json", Size: int64(len(reqBody))},
		},
		Response: capture.Response{
			Status: 200,
			Headers: capture.Headers{
				"content-type":     {"application/json; charset=utf-8"},
				"content-encoding": {"gzip"},
				"set-cookie":       {"sid=abc123def456; Path=/; HttpOnly; Secure", "theme=dark; Path=/"},
			},
		},
	}
	x.Request.Body.SetData([]byte(reqBody))
	gz := gzipBytes(t, []byte(respBody))
	x.Response.Body = capture.Body{Size: int64(len(gz))}
	x.Response.Body.SetData(gz) // binary, stored as base64
	planted := []string{
		"alice@example.com", "alice%40example.com", "bob@example.org", "bob%40example.org", "carol@example.net",
		"s3cr3t-session-token", "hunter2", `"Alice"`, "4111 1111 1111 1111", "4111111111111111", `"737"`,
		"(415) 555-2671", "415.555.2671", "+1 415 555 2671", "14155552671", "123-45-6789", "203.0.113.7",
		"2001:db8::1", "10.1.2.3", "1 Main St", "94105", "abc123def456", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9",
		"theme=dark", "ChQKDkFQUF9DT05UQUlORVJTEgIaAA==",
	}
	want := Report{
		KindEmail:        6, // path segment and query, x-user-email, request body, response body twice
		KindSensitiveKey: 6, // access_token; password, firstName, cvv; street, zip
		KindCredential:   5, // authorization, two cookies, two set-cookies
		KindIPv4:         3, // two in x-forwarded-for, one in the response body
		KindIPv6:         1,
		KindPhone:        4, // referer, two in the note, the response body
		KindCard:         2, // request body string and number
		KindSSN:          1,
	}
	return x, planted, want
}

func TestExchangeEndToEnd(t *testing.T) {
	s := newTestScrubber(t)
	x, planted, want := exchangeFixture(t)
	s.Exchange(x)
	enc, err := capture.Encode(x)
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range planted {
		if bytes.Contains(enc, []byte(p)) {
			t.Errorf("encoded exchange still contains %q", p)
		}
	}
	if !maps.Equal(Report(x.Redactions), want) {
		t.Errorf("redactions = %v\nwant         %v", x.Redactions, want)
	}
	if _, ok := x.Request.Headers["x-envoy-peer-metadata"]; ok {
		t.Error("drop header kept")
	}

	// Every string in the record is clean.
	var generic any
	if err := json.Unmarshal(enc, &generic); err != nil {
		t.Fatal(err)
	}
	walkStrings(generic, func(path, v string) {
		if ms := Detect(v); len(ms) > 0 {
			t.Errorf("%s = %q: Detect found %v", path, v, ms)
		}
	})

	resp := x.Response.Body
	if resp.DecodedFrom != "gzip" || resp.Encoding != capture.EncodingUTF8 || resp.Omitted != "" || resp.Truncated {
		t.Errorf("response body = %+v", resp)
	}
	if !json.Valid([]byte(resp.Data)) || !strings.Contains(resp.Data, `"lastLogin":{"ip":"2`) {
		t.Errorf("response data = %s", resp.Data)
	}
	if x.Request.Body.Size == 0 || x.Request.Body.ContentType != "application/json" {
		t.Errorf("request body metadata lost: %+v", x.Request.Body)
	}
	if err := x.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}

	// A second pass changes nothing, counts included.
	s.Exchange(x)
	again, err := capture.Encode(x)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(again, enc) {
		t.Errorf("Exchange is not idempotent:\n%s\n%s", enc, again)
	}
	// So does a scrubber built separately with the same key.
	y, _, _ := exchangeFixture(t)
	newTestScrubber(t).Exchange(y)
	if other, _ := capture.Encode(y); !bytes.Equal(other, enc) {
		t.Errorf("two scrubbers with one key disagree:\n%s\n%s", enc, other)
	}
}

func walkStrings(v any, f func(path, s string)) {
	var walk func(path string, v any)
	walk = func(path string, v any) {
		switch v := v.(type) {
		case string:
			f(path, v)
		case []any:
			for i, e := range v {
				walk(fmt.Sprintf("%s[%d]", path, i), e)
			}
		case map[string]any:
			for k, e := range v {
				f(path+" key", k)
				walk(path+"."+k, e)
			}
		}
	}
	walk("", v)
}

func TestExchangeNothingToRedact(t *testing.T) {
	s := newTestScrubber(t)
	x := &capture.Exchange{
		Request: capture.Request{Method: "GET", Path: "/healthz?verbose=1", Headers: capture.Headers{"accept": {"*/*"}}},
		Response: capture.Response{
			Status:  200,
			Headers: capture.Headers{"content-type": {"text/plain"}},
		},
	}
	x.Response.Body.SetData([]byte("ok\n"))
	s.Exchange(x)
	if x.Redactions != nil {
		t.Errorf("Redactions = %v, want nil", x.Redactions)
	}
	if x.Request.Path != "/healthz?verbose=1" || x.Response.Body.Data != "ok\n" {
		t.Errorf("clean exchange changed: %q %q", x.Request.Path, x.Response.Body.Data)
	}
	x.Redactions = map[string]int{KindEmail: 2}
	x.Request.Path = "/u/alice@example.com"
	s.Exchange(x)
	if x.Redactions[KindEmail] != 3 {
		t.Errorf("counts were not merged: %v", x.Redactions)
	}
	s.Exchange(nil) // must not panic
}

func TestExchangeBodies(t *testing.T) {
	s := newTestScrubber(t)
	x := &capture.Exchange{
		Request: capture.Request{
			Method: "POST", Path: "/",
			Headers: capture.Headers{"content-type": {"text/plain"}},
			Body:    capture.Body{Encoding: "rot13", Data: "nyvpr@rknzcyr.pbz", Size: 17},
		},
		Response: capture.Response{
			Status:  200,
			Headers: capture.Headers{"content-type": {"text/plain"}},
			Body:    capture.Body{Omitted: capture.OmittedTooLarge, Data: "alice@example.com", Size: 1 << 20, ContentType: "text/plain; name=alice@example.com"},
		},
	}
	s.Exchange(x)
	if b := x.Request.Body; b.Omitted != capture.OmittedUnparseable || b.Data != "" || b.Encoding != "" || b.Size != 17 {
		t.Errorf("unknown body encoding: %+v", b)
	}
	if b := x.Response.Body; b.Omitted != capture.OmittedTooLarge || b.Data != "" || strings.Contains(b.ContentType, "alice@") {
		t.Errorf("pre-omitted body: %+v", b)
	}

	// Binary data arrives base64-encoded and is omitted.
	y := &capture.Exchange{Response: capture.Response{Status: 200}}
	y.Response.Body = capture.Body{ContentType: "image/png", Size: 4}
	y.Response.Body.SetData([]byte("\x89PNG"))
	s.Exchange(y)
	if b := y.Response.Body; b.Omitted != capture.OmittedBinary || b.Data != "" || b.ContentType != "image/png" {
		t.Errorf("binary body: %+v", b)
	}

	// Truncated text keeps its complete lines and stays marked.
	z := &capture.Exchange{Response: capture.Response{Status: 200, Headers: capture.Headers{"content-type": {"text/plain"}}}}
	z.Response.Body = capture.Body{Size: 1000, Truncated: true}
	z.Response.Body.SetData([]byte("ok alice@example.com\npartial 4111 11"))
	s.Exchange(z)
	if b := z.Response.Body; !b.Truncated || !matches(`^ok user-[0-9a-f]{10}@redacted\.invalid\n$`, b.Data) {
		t.Errorf("truncated body: %+v", b)
	}
}

// TestConcurrentUse runs every entry point from 32 goroutines and checks the
// results match a serial run.
func TestConcurrentUse(t *testing.T) {
	s := newTestScrubber(t)
	text := "mail alice@example.com, card 4111 1111 1111 1111, ssn 123-45-6789 from 192.168.1.10"
	path := "/u/alice%40example.com?email=bob%40example.org&token=abc"
	headers := capture.Headers{"authorization": {"Bearer abc"}, "x-forwarded-for": {"203.0.113.7, 2001:db8::1"}}
	body := BodyInput{Data: gzipBytes(t, []byte(piiJSON)), ContentType: "application/json", ContentEncoding: "gzip"}
	wantText, wantPath := s.Text(text, nil), s.Path(path, nil)
	wantHeaders, wantBody := s.Headers(headers, nil), s.Body(body, nil)
	// Workers decode their own copy of the raw exchange.
	fx, _, _ := exchangeFixture(t)
	raw, err := capture.Encode(fx)
	if err != nil {
		t.Fatal(err)
	}
	scrubbed := func() ([]byte, error) {
		x, err := capture.Decode(raw)
		if err != nil {
			return nil, err
		}
		s.Exchange(x)
		return capture.Encode(x)
	}
	wantExchange, err := scrubbed()
	if err != nil {
		t.Fatal(err)
	}

	const goroutines, iterations = 32, 40
	var wg sync.WaitGroup
	errs := make(chan string, goroutines)
	for range goroutines {
		wg.Go(func() {
			for range iterations {
				rep := Report{}
				switch {
				case s.Text(text, rep) != wantText:
					errs <- "Text"
				case s.Path(path, rep) != wantPath:
					errs <- "Path"
				case fmt.Sprint(s.Headers(headers, rep)) != fmt.Sprint(wantHeaders):
					errs <- "Headers"
				case !bytes.Equal(s.Body(body, rep).Data, wantBody.Data):
					errs <- "Body"
				default:
					if enc, err := scrubbed(); err != nil || !bytes.Equal(enc, wantExchange) {
						errs <- "Exchange"
					}
				}
				if len(errs) > 0 {
					return
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		t.Errorf("%s differed under concurrency", e)
	}
}
