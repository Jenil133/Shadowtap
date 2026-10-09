package scrub

import (
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// Regression cases for personal data that once survived scrubbing. Each
// plants needles that must be absent from the output, and the output must
// stay stable when scrubbed again.

func TestBodyLeaks(t *testing.T) {
	s := newTestScrubber(t)
	const (
		jsonType = "application/json"
		formType = "application/x-www-form-urlencoded"
	)
	tests := []struct {
		name, contentType, body string
		needles                 []string
	}{
		{"plural field names", jsonType, `{"phones":["4155550123"],"emails":["a.b@example.com"]}`, []string{"4155550123", "a.b"}},
		{"token and key field names", jsonType,
			`{"authToken":"opaque1x","sessionToken":"opaque2x","apiToken":"opaque3x","secretKey":"opaque4x","otp":"493817","username":"alice.smith"}`,
			[]string{"opaque1x", "opaque2x", "opaque3x", "opaque4x", "493817", "alice.smith"}},
		{"temporary AWS credentials", jsonType,
			`{"Credentials":{"AccessKeyId":"ASIAIOSFODNN7EXAMPLE","SecretAccessKey":"wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY","SessionToken":"FwoGZXIvYXdzEBYaDHqa0AP"},"note":"key ASIAIOSFODNN7EXAMPLE"}`,
			[]string{"ASIAIOSFODNN7EXAMPLE", "wJalrXUtnFEMI", "FwoGZXIvYXdzEBYaDHqa0AP"}},
		{"client address fields", jsonType, `{"client_ip":"2001:db8::1","remote_addr":"2001:db8::2"}`, []string{"2001:db8"}},
		// Real identifiers can have the shape of a card or phone pseudonym
		// (a leading 0 and a valid check digit); under a sensitive name they
		// must still be redacted.
		{"national id shaped like a card pseudonym", jsonType, `{"nationalId":"0501015009084","accountNumber":"0001234567897"}`,
			[]string{"0501015009084", "0001234567897"}},
		{"00-prefixed international phone", jsonType, `{"phone":"0044 20 7946 0956"}`, []string{"0044 20 7946 0956"}},
		{"JSON inside a string", jsonType,
			`{"body":"{\"email\":\"alice@example.com\",\"password\":\"hunter2\",\"firstName\":\"Alice\"}"}`,
			[]string{"alice@example.com", "hunter2", `"Alice"`}},
		{"link with an encoded email", jsonType, `{"links":{"next":"/api/users?email=alice%40example.com&page=2"}}`, []string{"alice"}},
		{"nested URL token", jsonType, `{"redirect_uri":"https://x.example.com/cb?token=abc123opaque"}`, []string{"abc123opaque"}},
		{"JSON sent as text/plain", "text/plain;charset=UTF-8", `{"email":"alice@example.com","password":"hunter2","firstName":"Alice"}`,
			[]string{"alice@example.com", "hunter2", `"Alice"`}},
		{"JSON sent as form data", formType + "; charset=UTF-8", `{"email":"alice@example.com","password":"hunter2","firstName":"Alice"}`,
			[]string{"alice@example.com", "hunter2", `"Alice"`}},
		{"form array subscripts", formType, `phone%5B0%5D=4155550123&user%5Bphones%5D%5B0%5D=4155550199`, []string{"4155550123", "4155550199"}},
		{"form plus addressing", formType, `invitee=alice.smith+work@example.com&x=1`, []string{"alice.smith"}},
		{"assignments in text", "text/plain", "email=alice@example.com\r\npassword=hunter2\r\npin: 4821\r\nphone = (415) 555-2671\r\n",
			[]string{"alice@example.com", "hunter2", "4821", "555-2671"}},
		{"event stream", "text/event-stream", "data: {\"email\":\"alice@example.com\",\"password\":\"hunter2\"}\n\n", []string{"hunter2", "alice@example.com"}},
		{"GraphQL document", "application/graphql", `mutation { login(email: "alice@example.com", password: "hunter2") }`, []string{"hunter2", "alice@example.com"}},
		{"GraphQL in JSON", jsonType, `{"query":"mutation { signup(firstName: \"Alice\", password: \"hunter2\") }"}`, []string{"hunter2"}},
		{"HTML entities and encoded links", "text/html; charset=utf-8",
			`<a href="mailto:alice%40example.com">x</a> <span>bob&#64;example.org</span> <a href="/u?email=carol%40example.net">u</a>`,
			[]string{"alice", "bob", "carol"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := s.Body(BodyInput{Data: []byte(tt.body), ContentType: tt.contentType}, nil)
			if res.Omitted != "" {
				return // dropped entirely: nothing can leak
			}
			for _, n := range tt.needles {
				if strings.Contains(string(res.Data), n) {
					t.Errorf("%q survived:\n%s", n, res.Data)
				}
			}
			if again := s.Body(BodyInput{Data: res.Data, ContentType: tt.contentType}, nil); string(again.Data) != string(res.Data) {
				t.Errorf("not idempotent:\n first %s\nsecond %s", res.Data, again.Data)
			}
		})
	}
}

func TestHeaderLeaks(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		name, value string
		needles     []string
	}{
		{"x-access-token", "abc123opaque", []string{"abc123opaque"}},
		{"x-session-token", "abc123opaque", []string{"abc123opaque"}},
		{"api-key", "abc123opaque", []string{"abc123opaque"}},
		{"x-goog-api-key", "abc123opaque", []string{"abc123opaque"}},
		{"private-token", "glpat-abc123opaque", []string{"abc123opaque"}},
		{"ocp-apim-subscription-key", "abc123opaque", []string{"abc123opaque"}},
		{"x-user-phone", "4155550123", []string{"4155550123"}},
		{"x-user-email", "Alice.Smith@Example.com", []string{"alice", "Alice"}},
		{"authorization", "hunter2 extra", []string{"hunter2"}},
		{"x-original-forwarded-for", "2001:db8::1", []string{"2001:db8"}},
		{"cf-connecting-ipv6", "2001:db8::1", []string{"2001:db8"}},
		{"x-cluster-client-ip", "2001:db8::1", []string{"2001:db8"}},
		{"referer", "https://x.example.com/u/alice%2540example.com", []string{"alice"}},
		{"referer", "https://x.example.com/login?next=%2Freset%3Ftoken%3Dabc123opaque", []string{"abc123opaque"}},
		{"referer", "https://app.example.com/search?q=alice+tag@example.com", []string{"alice"}},
		{"x-envoy-original-path", "/users/alice%2540example.com", []string{"alice"}},
		// Numeric credentials can have the shape of a card pseudonym (a
		// leading 0 and a valid Luhn digit); they must not be kept as if they
		// were already scrubbed.
		{"authorization", "Bearer 0501015009084", []string{"0501015009084"}},
		{"x-api-key", "0001234567897", []string{"0001234567897"}},
		{"cookie", "session=0501015009084; theme=dark", []string{"0501015009084"}},
		{"cookie", `sid="0001234567897"`, []string{"0001234567897"}},
		{"set-cookie", "session=0501015009084; Path=/; HttpOnly", []string{"0501015009084"}},
		{"referer", "https://0001234567897:pw@app.example.com/x", []string{"0001234567897"}},
	}
	for _, tt := range tests {
		t.Run(tt.name+"/"+tt.value, func(t *testing.T) {
			h := capture.Headers{tt.name: {tt.value}}
			got := s.Headers(h, nil)
			for _, n := range tt.needles {
				if strings.Contains(strings.Join(got[tt.name], ","), n) {
					t.Errorf("%q survived: %q", n, got[tt.name])
				}
			}
			again := s.Headers(got, nil)
			if strings.Join(again[tt.name], ",") != strings.Join(got[tt.name], ",") {
				t.Errorf("not idempotent: %q then %q", got[tt.name], again[tt.name])
			}
		})
	}
}

func TestPathLeaks(t *testing.T) {
	s := newTestScrubber(t)
	for _, p := range []string{
		"/x?contact[phone][0]=4155550123",
		"/x?phone[]=4155550123",
		"/x?q=alice+tag@example.com",
		"/x?next=%2Freset%3Ftoken%3Dabc123opaque",
		"/x?link=https://a.example/p?email=alice%2540example.com",
	} {
		t.Run(p, func(t *testing.T) {
			got := s.Path(p, nil)
			for _, n := range []string{"4155550123", "alice", "abc123opaque"} {
				if strings.Contains(got, n) {
					t.Errorf("%q survived: %s", n, got)
				}
			}
			if again := s.Path(got, nil); again != got {
				t.Errorf("not idempotent: %s then %s", got, again)
			}
		})
	}
}

func TestAssignmentsKeepOrdinaryText(t *testing.T) {
	s := newTestScrubber(t)
	for _, in := range []string{
		"status: ok",
		"page=2&sort=name",
		"time 12:30:45 and ratio 3:2",
		"https://example.com/docs?lang=en",
		"nextPageToken: abc123",
		"relationship=friend",
		"password reset requested",
	} {
		if got := s.Text(in, nil); got != in {
			t.Errorf("Text(%q) = %q, want it unchanged", in, got)
		}
	}
}
