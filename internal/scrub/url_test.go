package scrub

import (
	"maps"
	"strings"
	"testing"
)

func TestPath(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		name, in string
		pattern  string // regular expression for the whole output
		report   Report
	}{
		{"no pii", "/api/v1/orders/ORD-48213?page=2&sort=-created", `^/api/v1/orders/ORD-48213\?page=2&sort=-created$`, Report{}},
		{"email segment", "/users/alice@example.com/profile",
			`^/users/user-[0-9a-f]{10}@redacted\.invalid/profile$`, Report{KindEmail: 1}},
		{"encoded email segment", "/users/alice%40example.com",
			`^/users/user-[0-9a-f]{10}@redacted\.invalid$`, Report{KindEmail: 1}},
		{"card segment", "/cards/4111111111111111/charge", `^/cards/0\d{15}/charge$`, Report{KindCard: 1}},
		{"encoded plus phone segment", "/call/%2B14155552671", `^/call/\+0\d{10}$`, Report{KindPhone: 1}},
		{"sensitive query keys", "/search?email=alice%40example.com&page=2&password=hunter2&user_email=bob@example.org&pageToken=abc123",
			`^/search\?email=user-[0-9a-f]{10}%40redacted\.invalid&page=2&password=tok_[0-9a-f]{16}&user_email=user-[0-9a-f]{10}%40redacted\.invalid&pageToken=abc123$`,
			Report{KindEmail: 2, KindSensitiveKey: 1}},
		{"detected query value", "/q?term=call+me+at+(415)+555-2671&lang=en",
			`^/q\?term=call\+me\+at\+%280\d\d%29\+0\d\d-0\d{3}&lang=en$`, Report{KindPhone: 1}},
		{"plus as space before phone digits", "/q?q=call+14155552671", `^/q\?q=call\+0\d{10}$`, Report{KindPhone: 1}},
		{"untouched pairs keep their encoding", "/x?a=%7Euser&b=hello+world&c&d=&email=",
			`^/x\?a=%7Euser&b=hello\+world&c&d=&email=$`, Report{}},
		// '%' and '+' are local-part characters, so "%zz+alice" is one address.
		{"malformed escapes are raw text", "/x%zz/alice@example.com?q=%zz+alice@example.com&r=%zz+4111111111111111&password=%zz",
			`^/x%zz/user-[0-9a-f]{10}@redacted\.invalid\?q=user-[0-9a-f]{10}@redacted\.invalid&r=%zz\+0\d{15}&password=tok_[0-9a-f]{16}$`,
			Report{KindEmail: 2, KindCard: 1, KindSensitiveKey: 1}},
		{"pii in a query key", "/x?alice@example.com=1", `^/x\?user-[0-9a-f]{10}%40redacted\.invalid=1$`, Report{KindEmail: 1}},
		{"sensitive nested key", "/x?user[phone]=%2B1%20415%20555%202671",
			`^/x\?user\[phone\]=tok_[0-9a-f]{16}$`, Report{KindPhone: 1}},
		{"jwt and secret in query", "/cb?state=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln&k=AKIAIOSFODNN7EXAMPLE",
			`^/cb\?state=redacted\.jwt\.[0-9a-f]{16}&k=redacted\.secret\.[0-9a-f]{16}$`, Report{KindJWT: 1, KindSecret: 1}},
		{"ip in segment", "/hosts/192.168.1.10/status", `^/hosts/2[45]\d\.\d+\.\d+\.\d+/status$`, Report{KindIPv4: 1}},
		// Read as a query, '+' is a space and this segment holds a phone number.
		{"plus as space in segment", "/call/+1+415+555+2671", `^/call/\+1\+0\d\d\+0\d\d\+0\d{3}$`, Report{KindPhone: 1}},
		// Read as a path, '+' is literal and this value holds a phone number.
		{"literal plus before escaped digits", "/q?p=+%31%34%31%35%35%35%35%32%36%37%31", `^/q\?p=\+0\d{10}$`, Report{KindPhone: 1}},
		{"empty", "", `^$`, Report{}},
		{"root", "/", `^/$`, Report{}},
		{"bare question mark", "/?", `^/\?$`, Report{}},
		{"empty segments", "/a//b/", `^/a//b/$`, Report{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := Report{}
			out := s.Path(tt.in, rep)
			if !matches(tt.pattern, out) {
				t.Fatalf("Path(%q) = %q, want %s", tt.in, out, tt.pattern)
			}
			if !maps.Equal(rep, tt.report) {
				t.Errorf("Path(%q) report = %v, want %v", tt.in, rep, tt.report)
			}
			assertClean(t, "Path", out)
			if !cleanViews(out, textDetectors) {
				t.Errorf("Path(%q) = %q, dirty once unescaped", tt.in, out)
			}
			again := Report{}
			if got := s.Path(out, again); got != out || len(again) != 0 {
				t.Errorf("Path(%q) = %q (report %v), want unchanged", out, got, again)
			}
		})
	}
}

func TestPathUnchangedPartsAreByteIdentical(t *testing.T) {
	s := newTestScrubber(t)
	in := "/a%2Fb/c%20d/alice@example.com/e+f?x=%41%42&email=bob%40example.org&y=1%2B1&z"
	out := s.Path(in, nil)
	for _, part := range []string{"/a%2Fb/c%20d/", "/e+f?x=%41%42&", "&y=1%2B1&z"} {
		if !strings.Contains(out, part) {
			t.Errorf("Path(%q) = %q, lost %q", in, out, part)
		}
	}
}

func TestURLValue(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		name, in, pattern string
	}{
		{"query email", "https://shop.example.com/checkout?email=alice%40example.com&step=2",
			`^https://shop\.example\.com/checkout\?email=user-[0-9a-f]{10}%40redacted\.invalid&step=2$`},
		{"user info", "https://alice:s3cret@example.com/x", `^https://tok_[0-9a-f]{16}@example\.com/x$`},
		{"email-like user info", "ftp://alice@example.com/", `^ftp://tok_[0-9a-f]{16}@example\.com/$`},
		{"ipv4 host", "http://192.168.1.10:8080/admin", `^http://2[45]\d\.\d+\.\d+\.\d+:8080/admin$`},
		{"ipv6 host", "https://[2001:db8::1]:8443/x", `^https://\[100::[0-9a-f:]+\]:8443/x$`},
		{"near-address host", "http://[::1.0.00.0]:80/x", `^http://\[100::[0-9a-f:]+\]:80/x$`},
		{"relative", "/u/alice@example.com?x=1", `^/u/user-[0-9a-f]{10}@redacted\.invalid\?x=1$`},
		{"fragment token", "https://app.example.com/cb#access_token=abc123&token_type=bearer",
			`^https://app\.example\.com/cb#access_token=tok_[0-9a-f]{16}&token_type=bearer$`},
		{"mailto", "mailto:alice@example.com", `^mailto:user-[0-9a-f]{10}@redacted\.invalid$`},
		{"clean", "https://example.com/a/b?c=d#e", `^https://example\.com/a/b\?c=d#e$`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := s.urlValue(tt.in, nil)
			if !matches(tt.pattern, out) {
				t.Fatalf("urlValue(%q) = %q, want %s", tt.in, out, tt.pattern)
			}
			assertClean(t, "urlValue", out)
			if again := s.urlValue(out, nil); again != out {
				t.Errorf("urlValue(%q) = %q, want unchanged", out, again)
			}
		})
	}
}
