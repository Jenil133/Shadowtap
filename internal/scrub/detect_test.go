package scrub

import (
	"math/rand/v2"
	"strings"
	"testing"
)

func TestDetectTruePositives(t *testing.T) {
	tests := []struct {
		name, in, kind, match string
	}{
		// secret
		{"aws access key", "key=AKIAIOSFODNN7EXAMPLE;", KindSecret, "AKIAIOSFODNN7EXAMPLE"},
		{"github token", "token ghp_0123456789abcdefghijABCDEFGHIJ012345 end", KindSecret, "ghp_0123456789abcdefghijABCDEFGHIJ012345"},
		{"github oauth", "gho_" + strings.Repeat("a1", 20), KindSecret, "gho_" + strings.Repeat("a1", 20)},
		{"stripe live", "sk_live_EXAMPLEFIXTURE01", KindSecret, "sk_live_EXAMPLEFIXTURE01"},
		{"stripe restricted test", "rk_test_51Habcdefghijklmnop", KindSecret, "rk_test_51Habcdefghijklmnop"},
		{"slack bot", "xoxb-123456789012-abcdefABCDEF", KindSecret, "xoxb-123456789012-abcdefABCDEF"},
		{"google api key", `"AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI"`, KindSecret, "AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI"},
		// jwt
		{"jwt", "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
			KindJWT, "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U"},
		{"unsigned jwt", "t=eyJhbGciOiJub25lIn0.eyJzdWIiOiJhIn0.", KindJWT, "eyJhbGciOiJub25lIn0.eyJzdWIiOiJhIn0."},
		// email
		{"email", "mail alice@example.com now", KindEmail, "alice@example.com"},
		{"email mixed case and tags", "<Bob.Smith+news@Mail.Example.co.uk>", KindEmail, "Bob.Smith+news@Mail.Example.co.uk"},
		{"email in mailto", "mailto:carol_99@sub-domain.example.org?subject=hi", KindEmail, "carol_99@sub-domain.example.org"},
		{"email at sentence end", "Write to dave@example.com.", KindEmail, "dave@example.com"},
		{"email at other invalid domain", "x@redacted.invalid.example.com", KindEmail, "x@redacted.invalid.example.com"},
		// card
		{"visa 16", "card 4111111111111111 exp 12/27", KindCard, "4111111111111111"},
		{"visa spaced", "4111 1111 1111 1111", KindCard, "4111 1111 1111 1111"},
		{"visa dashed", "pan:4012-8888-8888-1881.", KindCard, "4012-8888-8888-1881"},
		{"visa 13", "old card 4222222222222", KindCard, "4222222222222"},
		{"visa 19", "4111111111111111110", KindCard, "4111111111111111110"},
		{"mastercard", "5555555555554444", KindCard, "5555555555554444"},
		{"mastercard 2-series", "2223 0031 2200 3222", KindCard, "2223 0031 2200 3222"},
		{"amex", "amex 3782 822463 10005", KindCard, "3782 822463 10005"},
		{"discover", "6011111111111117", KindCard, "6011111111111117"},
		{"discover 644", "6445644564456445", KindCard, "6445644564456445"},
		{"jcb", "3530111333300000", KindCard, "3530111333300000"},
		{"diners 14", "30569309025904", KindCard, "30569309025904"},
		{"unionpay", "6200000000000005", KindCard, "6200000000000005"},
		{"card after order number", "order 1234 4111 1111 1111 1111", KindCard, "4111 1111 1111 1111"},
		{"card after escape", `note:\n4111111111111111`, KindCard, "4111111111111111"},
		{"negative json number", "-4111111111111111", KindCard, "4111111111111111"},
		// ssn
		{"ssn", "SSN: 123-45-6789", KindSSN, "123-45-6789"},
		{"ssn itin", "itin 912-70-1234", KindSSN, "912-70-1234"},
		{"ssn in parens", "(078-05-1120)", KindSSN, "078-05-1120"},
		// phone
		{"nanp parens", "call (415) 555-2671 today", KindPhone, "(415) 555-2671"},
		{"nanp dashes", "415-555-2671", KindPhone, "415-555-2671"},
		{"nanp dots", "tel 415.555.2671", KindPhone, "415.555.2671"},
		{"nanp spaces", "415 555 2671", KindPhone, "415 555 2671"},
		{"nanp with country code", "1-415-555-2671", KindPhone, "415-555-2671"},
		{"e164 compact", "+14155552671", KindPhone, "+14155552671"},
		{"e164 grouped", "+1 (415) 555-2671", KindPhone, "+1 (415) 555-2671"},
		{"uk", "+44 20 7946 0958", KindPhone, "+44 20 7946 0958"},
		{"germany", "+49 (30) 901820.", KindPhone, "+49 (30) 901820"},
		{"france", "+33 1 23 45 67 89", KindPhone, "+33 1 23 45 67 89"},
		{"dotted", "+44.20.7946.0958", KindPhone, "+44.20.7946.0958"},
		{"phone then year", "+1 415 555 2671 2026", KindPhone, "+1 415 555 2671 2026"},
		// ipv4
		{"ipv4", "from 192.168.1.10 via", KindIPv4, "192.168.1.10"},
		{"ipv4 with port", "203.0.113.7:8080", KindIPv4, "203.0.113.7"},
		{"ipv4 private", "10.0.0.1", KindIPv4, "10.0.0.1"},
		{"ipv4 leading zeros", "192.168.001.010", KindIPv4, "192.168.001.010"},
		{"ipv4 at sentence end", "host 8.8.8.8.", KindIPv4, "8.8.8.8"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := Detect(tt.in)
			if len(ms) != 1 {
				t.Fatalf("Detect(%q) = %v, want one %s match", tt.in, ms, tt.kind)
			}
			m := ms[0]
			if got := tt.in[m.Start:m.End]; m.Kind != tt.kind || got != tt.match {
				t.Errorf("Detect(%q) = %s %q, want %s %q", tt.in, m.Kind, got, tt.kind, tt.match)
			}
		})
	}
}

func TestDetectFalsePositives(t *testing.T) {
	tests := []struct{ name, in string }{
		{"order id", "order ORD-48213-7731 shipped"},
		{"short order number", "order #1234567"},
		{"millisecond timestamp", `{"ts":1759449600000}`},
		{"snowflake id", "id 1580661436132757504"},
		{"luhn-invalid 16 digits", "4111111111111112"},
		{"luhn-valid without brand", "1234567812345670"},
		{"too many digits", "41111111111111111234"},
		{"card glued to letters", "x4111111111111111y"},
		{"decimal fraction", "0.4111111111111111"},
		{"decimal integer part", "4111111111111111.25"},
		{"float", "0.30000000000000004"},
		{"date", "2026-10-03"},
		{"datetime", "2026-10-03T12:00:00.000Z"},
		{"european date", "03.10.2026"},
		{"version", "v1.26.7"},
		{"four-part version", "Windows NT 10.0.19045.3803"},
		{"user agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.6099.109 Safari/537.36"},
		{"semver with v and four parts", "v1.2.3.4"},
		{"uuid", "550e8400-e29b-41d4-a716-446655440000"},
		{"uuid v7", "01928c3a-7b2e-7cc1-9e5f-3a1b2c3d4e5f"},
		{"sha256", "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"},
		{"sha1", "a94a8fe5ccb19ba61c4c0873d391e987982fbbd3"},
		{"price", "Total: $1,299.99 (USD 4111.11)"},
		{"zip code", "San Francisco, CA 94105-1234"},
		{"zip codes", "zips 94105 10001 60601 73301"},
		{"time", "12:30:45.123"},
		{"loopback", "connect to 127.0.0.1:5432"},
		{"unspecified", "listen 0.0.0.0:8080"},
		{"broadcast", "255.255.255.255"},
		{"reserved range", "240.1.2.3"},
		{"bad octet", "256.1.1.1"},
		{"five-part dotted", "1.2.3.4.5"},
		{"three-part dotted", "10.0.1"},
		{"invalid ssn area 000", "000-12-3456"},
		{"invalid ssn area 666", "666-12-3456"},
		{"invalid ssn group", "123-00-4567"},
		{"invalid ssn serial", "123-45-0000"},
		{"ssn too long", "123-45-67890"},
		{"ssn glued to digits", "1123-45-6789"},
		{"phone too short", "+1234567"},
		{"phone too long", "+1234567890123456"},
		{"phone starting with zero", "+0123456789"},
		{"nanp area starts with 1", "123-456-7890"},
		{"nanp exchange starts with 1", "415-155-2671"},
		{"nanp without separators", "4155552671"},
		{"timezone offset", "2026-10-03T12:00:00+05:30"},
		{"handle", "follow @shadowtap"},
		{"no tld", "user@localhost"},
		{"one-letter tld", "user@example.c"},
		{"pseudonym email", "user-0123456789@redacted.invalid"},
		{"tokenized user info", "https://tok_0123456789abcdef@example.com/"},
		{"jwt header only", "eyJhbGciOiJIUzI1NiJ9"},
		{"jwt segments too short", "eyJab.eyJcd.ef"},
		{"short aws key", "AKIA1234"},
		{"lowercase aws key", "akiaiosfodnn7example"},
		{"short github token", "ghp_abc"},
		{"short stripe key", "sk_live_123"},
		{"slack prefix only", "xoxb-123"},
		{"plain words", "The quick brown fox jumps over the lazy dog"},
		{"empty", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if ms := Detect(tt.in); len(ms) != 0 {
				t.Errorf("Detect(%q) = %v, want none", tt.in, ms)
			}
		})
	}
}

func TestDetectPrecedence(t *testing.T) {
	tests := []struct {
		name, in string
		want     []string // kinds in order
	}{
		{"secret wins over email", "sk_live_abcdefghijklmnop1234@example.com", []string{KindSecret}},
		{"jwt wins over card inside it", "eyJhbGciOi-4111111111111111-x.eyJzdWIiOiJhIn0.x", []string{KindJWT}},
		{"ssn before phone", "+1 012-34-5678", []string{KindSSN}},
		{"card before phone", "+4111 1111 1111 1111", []string{KindCard}},
		{"several kinds", "a@b.co 4111111111111111 123-45-6789 (415) 555-2671 10.1.2.3",
			[]string{KindEmail, KindCard, KindSSN, KindPhone, KindIPv4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ms := Detect(tt.in)
			var got []string
			for _, m := range ms {
				got = append(got, m.Kind)
			}
			if strings.Join(got, ",") != strings.Join(tt.want, ",") {
				t.Errorf("Detect(%q) kinds = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

// TestDetectSortedNonOverlapping checks the ordering contract on random
// mixtures of PII fragments and filler.
func TestDetectSortedNonOverlapping(t *testing.T) {
	parts := []string{
		"alice@example.com", "4111 1111 1111 1111", "123-45-6789", "(415) 555-2671", "+14155552671",
		"192.168.0.1", "AKIAIOSFODNN7EXAMPLE", "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.sig", " ", "-", ".",
		"@", "x", "0", "12", "(", ")", "+", "_", "\\n",
	}
	rng := rand.New(rand.NewPCG(1, 2))
	for range 2000 {
		var b strings.Builder
		for range 1 + rng.IntN(12) {
			b.WriteString(parts[rng.IntN(len(parts))])
		}
		in := b.String()
		ms := Detect(in)
		for i, m := range ms {
			if m.Start < 0 || m.End > len(in) || m.Start >= m.End {
				t.Fatalf("Detect(%q): bad span %v", in, m)
			}
			if i > 0 && m.Start < ms[i-1].End {
				t.Fatalf("Detect(%q): %v overlaps or precedes %v", in, m, ms[i-1])
			}
		}
	}
}

// TestMayContainPII checks that the prefilter never hides a detector match.
func TestMayContainPII(t *testing.T) {
	for _, in := range []string{
		"AKIAIOSFODNN7EXAMPLE", "AIzaSyDdI0hCZtE6vySjMm-WEfRq3CPzqKqqsHI", "xoxb-abcdefghijkl",
		"ghp_abc", "eyJ", "a@b", "7",
	} {
		if !mayContainPII(in) {
			t.Errorf("mayContainPII(%q) = false", in)
		}
	}
	for _, in := range []string{"", "hello world", "AKI", "xo", "ey", "Content-Type"} {
		if mayContainPII(in) {
			t.Errorf("mayContainPII(%q) = true", in)
		}
	}
}

func TestCardBrands(t *testing.T) {
	tests := []struct {
		digits string
		want   bool
	}{
		{"4111111111111111", true},
		{"4222222222222", true},
		{"4111111111111111110", true},
		{"411111111111111", false}, // Visa has no 15-digit numbers (and fails Luhn)
		{"2221000000000009", true},
		{"2720999999999996", true},
		{"2721000000000004", false},
		{"378282246310005", true},
		{"3782822463100050", false}, // Amex is 15 digits
		{"6534567890123458", true},
		{"352812345678901234", true},
		{"3012345678901231", true},
		{"620000000000000005", true},
		{"7000000000000005", false},
		{"0000000000000000", false},
	}
	for _, tt := range tests {
		if got := validCard(tt.digits); got != tt.want {
			t.Errorf("validCard(%s) = %v, want %v", tt.digits, got, tt.want)
		}
	}
}
