package scrub

import (
	"maps"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

func TestHeaders(t *testing.T) {
	s := newTestScrubber(t)
	const (
		tok   = `tok_[0-9a-f]{16}`
		ip4   = `(24\d|25[0-4])\.\d{1,3}\.\d{1,3}\.\d{1,3}`
		ip6   = `100::[0-9a-f:]+`
		email = `user-[0-9a-f]{10}@redacted\.invalid`
	)
	tests := []struct {
		name, header, in, pattern string
		report                    Report
	}{
		{"bearer", "authorization", "Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln", `^Bearer ` + tok + `$`, Report{KindCredential: 1}},
		{"basic", "authorization", "Basic dXNlcjpwYXNzd29yZA==", `^Basic ` + tok + `$`, Report{KindCredential: 1}},
		{"digest keeps scheme", "authorization", `Digest username="alice", realm="x", response="6629fae4"`, `^Digest ` + tok + `$`, Report{KindCredential: 1}},
		{"known scheme", "proxy-authorization", "SSWS 00abc-123", `^SSWS ` + tok + `$`, Report{KindCredential: 1}},
		// An unknown first word may be part of the secret ("hunter2 extra"): kept nowhere.
		{"unknown scheme", "authorization", "hunter2 extra", `^` + tok + `$`, Report{KindCredential: 1}},
		{"no scheme", "authorization", "opaque-token-value", `^` + tok + `$`, Report{KindCredential: 1}},
		{"digits are not a scheme", "authorization", "4111111111111111 abc", `^` + tok + `$`, Report{KindCredential: 1}},
		{"already tokenized", "authorization", "Bearer tok_0123456789abcdef", `^Bearer tok_0123456789abcdef$`, Report{}},
		{"api key", "x-api-key", "live key 123", `^` + tok + `$`, Report{KindCredential: 1}},
		{"api key with known scheme", "x-auth-token", "Bearer abc", `^Bearer ` + tok + `$`, Report{KindCredential: 1}},
		{"empty credential", "x-api-key", "", `^$`, Report{}},
		// Scheme and pseudonym are each clean but form a phone number together.
		{"scheme forming a match", "proxy-authorization", "Bearer 415 555 2671", `^Bearer ` + tok + `$`, Report{KindCredential: 1}},
		{"cookie", "cookie", "session=abc123; theme=dark;  lang=\"en-US\"; flag",
			`^session=` + tok + `; theme=` + tok + `;  lang="` + tok + `"; ` + tok + `$`, Report{KindCredential: 4}},
		{"cookie empty value", "cookie", "a=; b", `^a=; ` + tok + `$`, Report{KindCredential: 1}},
		{"set-cookie keeps attributes", "set-cookie", "sid=xyz789; Path=/; Domain=example.com; Expires=Wed, 21 Oct 2026 07:28:00 GMT; HttpOnly; Secure; SameSite=Lax",
			`^sid=` + tok + `; Path=/; Domain=example\.com; Expires=Wed, 21 Oct 2026 07:28:00 GMT; HttpOnly; Secure; SameSite=Lax$`, Report{KindCredential: 1}},
		{"x-forwarded-for", "x-forwarded-for", "203.0.113.7, 2001:db8::1,unknown, 10.0.0.1:8080, [2001:db8::2]:443, 127.0.0.1, ::1, _hidden, ::ffff:198.51.100.4",
			`^` + ip4 + `, ` + ip6 + `,unknown, ` + ip4 + `:8080, \[` + ip6 + `\]:443, 127\.0\.0\.1, ::1, _hidden, ::ffff:` + ip4 + `$`,
			Report{KindIPv4: 3, KindIPv6: 2}},
		{"x-real-ip", "x-real-ip", "198.51.100.23", `^` + ip4 + `$`, Report{KindIPv4: 1}},
		// Not an address (leading zeros), but its IPv4 pseudonym makes one.
		{"near-address", "x-forwarded-for", "::1.0.00.0", `^` + ip6 + `$`, Report{KindIPv4: 1, KindIPv6: 1}},
		{"forwarded", "forwarded", `for=192.0.2.60;proto=http;by=203.0.113.43, for="[2001:db8:cafe::17]:4711", For=unknown;host=example.com`,
			`^for=` + ip4 + `;proto=http;by=` + ip4 + `, for="\[` + ip6 + `\]:4711", For=unknown;host=example\.com$`,
			Report{KindIPv4: 2, KindIPv6: 1}},
		{"ip header with an email in it", "x-forwarded-for", "alice@example.com, 1.2.3.4", `^` + email + `, ` + ip4 + `$`, Report{KindEmail: 1, KindIPv4: 1}},
		{"referer", "referer", "https://shop.example.com/u/alice?email=alice%40example.com&step=2",
			`^https://shop\.example\.com/u/alice\?email=user-[0-9a-f]{10}%40redacted\.invalid&step=2$`, Report{KindEmail: 1}},
		{"location", "location", "/accounts/4111111111111111/verify", `^/accounts/0\d{15}/verify$`, Report{KindCard: 1}},
		{"other header", "x-user-email", "alice@example.com", `^` + email + `$`, Report{KindEmail: 1}},
		{"escaped data in other header", "x-envoy-original-path", "/api/users?email=alice%40example.com&page=2",
			`^/api/users\?email=user-[0-9a-f]{10}%40redacted\.invalid&page=2$`, Report{KindEmail: 1}},
		{"plus as space in other header", "x-note", "card+4111+1111+1111+1111",
			`^card\+0\d{3}\+\d{4}\+\d{4}\+\d{4}$`, Report{KindCard: 1}},
		{"user agent untouched", "user-agent", "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/120.0.6099.109",
			`^Mozilla/5\.0 \(X11; Linux x86_64\) AppleWebKit/537\.36 Chrome/120\.0\.6099\.109$`, Report{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := Report{}
			h := capture.Headers{tt.header: {tt.in}}
			out := s.Headers(h, rep)
			got := out[tt.header]
			if len(got) != 1 || !matches(tt.pattern, got[0]) {
				t.Fatalf("%s: %q -> %q, want %s", tt.header, tt.in, got, tt.pattern)
			}
			if !maps.Equal(rep, tt.report) {
				t.Errorf("report = %v, want %v", rep, tt.report)
			}
			assertClean(t, tt.header, got[0])
			if !cleanViews(got[0], textDetectors) {
				t.Errorf("%q is dirty once unescaped", got[0])
			}
			again := Report{}
			if twice := s.Headers(out, again); !reflect.DeepEqual(twice, out) || len(again) != 0 {
				t.Errorf("second pass = %q (report %v), want %q", twice[tt.header], again, got)
			}
		})
	}
}

func TestHeadersMapHandling(t *testing.T) {
	s := newTestScrubber(t)
	in := capture.Headers{
		"x-envoy-peer-metadata":    {"ChQKDkFQUF9DT05UQUlORVJTEgIaAA=="},
		"x-envoy-peer-metadata-id": {"sidecar~10.0.0.1~pod.ns~ns.svc.cluster.local"},
		"x-trace":                  {"a", "alice@example.com", "c"},
		"x-empty":                  {},
		"x-nil":                    nil,
		"content-type":             {"application/json"},
	}
	// Clone would turn the empty slice into nil, so copy by hand.
	orig := make(capture.Headers, len(in))
	for k, v := range in {
		if v != nil {
			v = slices.Clone(v)
		}
		orig[k] = v
	}
	out := s.Headers(in, nil)
	if !reflect.DeepEqual(in, orig) {
		t.Errorf("Headers modified its input: %v", in)
	}
	for _, name := range []string{"x-envoy-peer-metadata", "x-envoy-peer-metadata-id"} {
		if _, ok := out[name]; ok {
			t.Errorf("%s was not dropped", name)
		}
	}
	got := out["x-trace"]
	if len(got) != 3 || got[0] != "a" || got[2] != "c" || !strings.HasPrefix(got[1], "user-") {
		t.Errorf("multi-value header = %q, want order kept and the email replaced", got)
	}
	if v, ok := out["x-empty"]; !ok || v == nil || len(v) != 0 {
		t.Errorf("x-empty = %#v, want an empty slice", v)
	}
	if v, ok := out["x-nil"]; !ok || v != nil {
		t.Errorf("x-nil = %#v, want nil", v)
	}
	if s.Headers(nil, nil) != nil {
		t.Error("Headers(nil) != nil")
	}
	keys := slices.Sorted(maps.Keys(out))
	if want := []string{"content-type", "x-empty", "x-nil", "x-trace"}; !slices.Equal(keys, want) {
		t.Errorf("keys = %v, want %v", keys, want)
	}
}

func TestHeadersConfiguredLists(t *testing.T) {
	s := newTestScrubber(t, func(c *Config) {
		c.CredentialHeaders = []string{"X-Session"}
		c.IPHeaders = []string{" X-Origin-IP "}
		c.URLHeaders = []string{"x-return-to"}
		c.DropHeaders = []string{"X-Internal"}
	})
	out := s.Headers(capture.Headers{
		"x-session":     {"abc"},
		"x-origin-ip":   {"198.51.100.7"},
		"x-return-to":   {"/u?email=alice%40example.com"},
		"x-internal":    {"secret"},
		"authorization": {"Bearer abc"}, // defaults still apply
	}, nil)
	if v := out["x-session"][0]; !isTokenPseudonym(v) {
		t.Errorf("x-session = %q", v)
	}
	if v := out["x-origin-ip"][0]; !IsPseudonym(v) {
		t.Errorf("x-origin-ip = %q", v)
	}
	if v := out["x-return-to"][0]; !strings.Contains(v, "%40redacted.invalid") {
		t.Errorf("x-return-to = %q", v)
	}
	if _, ok := out["x-internal"]; ok {
		t.Error("x-internal was not dropped")
	}
	if v := out["authorization"][0]; !strings.HasPrefix(v, "Bearer tok_") {
		t.Errorf("authorization = %q", v)
	}
}

func TestHeadersCredentialRulesDisabled(t *testing.T) {
	s := newTestScrubber(t, func(c *Config) { c.DisabledDetectors = []string{KindCredential} })
	out := s.Headers(capture.Headers{
		"authorization": {"Bearer plain-token"},
		"cookie":        {"session=alice@example.com"},
	}, nil)
	if v := out["authorization"][0]; v != "Bearer plain-token" {
		t.Errorf("authorization = %q, want unchanged", v)
	}
	if v := out["cookie"][0]; !strings.HasPrefix(v, "session=user-") {
		t.Errorf("cookie = %q, want the text detectors applied", v)
	}
}
