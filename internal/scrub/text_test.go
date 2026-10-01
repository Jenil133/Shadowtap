package scrub

import (
	"maps"
	"strings"
	"testing"
)

func TestTextReplacesEveryKind(t *testing.T) {
	s := newTestScrubber(t)
	planted := map[string]string{
		"alice@example.com":    KindEmail,
		"4111 1111 1111 1111":  KindCard,
		"123-45-6789":          KindSSN,
		"(415) 555-2671":       KindPhone,
		"+44 20 7946 0958":     KindPhone,
		"192.168.1.10":         KindIPv4,
		"AKIAIOSFODNN7EXAMPLE": KindSecret,
		"eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U": KindJWT,
	}
	var b strings.Builder
	want := Report{}
	for v, kind := range planted {
		b.WriteString("value: " + v + "\n")
		want[kind]++
	}
	rep := Report{}
	out := s.Text(b.String(), rep)
	for v := range planted {
		if strings.Contains(out, v) {
			t.Errorf("output still contains %q:\n%s", v, out)
		}
	}
	assertClean(t, "Text", out)
	if !maps.Equal(rep, want) {
		t.Errorf("report = %v, want %v", rep, want)
	}
	if again := s.Text(out, rep); again != out {
		t.Errorf("Text is not idempotent:\n%s\n%s", out, again)
	}
	if !maps.Equal(rep, want) {
		t.Errorf("re-scrubbing pseudonyms changed the report to %v", rep)
	}
}

func TestTextLeavesCleanTextAlone(t *testing.T) {
	s := newTestScrubber(t)
	for _, in := range []string{
		"",
		"plain text without anything personal",
		"order ORD-48213-7731 for $1,299.99 on 2026-10-03 at 12:30:45",
		"user-0123456789@redacted.invalid called +0 015 055 0671 about 0111 1111 1111 1111",
		"tok_0123456789abcdef redacted.jwt.0123456789abcdef 240.12.34.56 000-12-3456",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)",
		"invalid utf-8 \xff\xfe stays",
	} {
		rep := Report{}
		if out := s.Text(in, rep); out != in || len(rep) != 0 {
			t.Errorf("Text(%q) = %q, report %v; want unchanged and empty", in, out, rep)
		}
	}
}

// TestTextFixpoint covers a replacement that completes a new match with its
// neighbours: the SSN pseudonym 000-xx-xxxx after "+1 " is an international
// phone number, which needs a second pass.
func TestTextFixpoint(t *testing.T) {
	s := newTestScrubber(t)
	rep := Report{}
	out := s.Text("+1 012-34-5678", rep)
	assertClean(t, "Text", out)
	if rep[KindSSN] != 1 || rep[KindPhone] != 1 {
		t.Errorf("report = %v, want one ssn and one phone", rep)
	}
	if again := s.Text(out, nil); again != out {
		t.Errorf("Text(%q) = %q, want a fixpoint", out, again)
	}
}

func TestTextDisabledDetectors(t *testing.T) {
	s := newTestScrubber(t, func(c *Config) { c.DisabledDetectors = []string{KindIPv4, "EMAIL "} })
	in := "alice@example.com from 192.168.1.10 card 4111111111111111"
	rep := Report{}
	out := s.Text(in, rep)
	if !strings.Contains(out, "alice@example.com") || !strings.Contains(out, "192.168.1.10") {
		t.Errorf("disabled detectors still ran: %q", out)
	}
	if strings.Contains(out, "4111111111111111") || rep[KindCard] != 1 {
		t.Errorf("card not redacted: %q, %v", out, rep)
	}
}

func TestRedactRemaining(t *testing.T) {
	rep := Report{}
	got := redactRemaining("a 4111111111111111 b alice@example.com", textDetectors, rep)
	if want := "a [redacted] b [redacted]"; got != want {
		t.Errorf("redactRemaining = %q, want %q", got, want)
	}
	if rep[KindCard] != 1 || rep[KindEmail] != 1 {
		t.Errorf("report = %v", rep)
	}
	assertClean(t, "marker", redactedMarker)
}

func TestSensitiveValue(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		in, kind string // kind counted; "" when unchanged
		pattern  string
	}{
		{"", "", `^$`},
		{"hunter2", KindSensitiveKey, `^tok_[0-9a-f]{16}$`},
		{"alice@example.com", KindEmail, `^user-[0-9a-f]{10}@redacted\.invalid$`},
		{"415-555-2671", KindPhone, `^tok_[0-9a-f]{16}$`}, // a token: a phone pseudonym is only a shape
		{"123456789", KindSSN, `^000-\d\d-\d{4}$`},
		{"000123456", KindSensitiveKey, `^tok_`}, // zero-padded, not a pseudonym
		{"123-45-6789", KindSSN, `^000-\d\d-\d{4}$`},
		{"4111 1111 1111 1111", KindCard, `^tok_[0-9a-f]{16}$`},
		// Look like card and phone pseudonyms but are real identifiers: never kept.
		{"0501015009084", KindSensitiveKey, `^tok_`},
		{"0044 20 7946 0956", KindSensitiveKey, `^tok_`},
		{"192.168.1.10", KindIPv4, `^2[45]\d\.`},
		{" alice@example.com", KindSensitiveKey, `^tok_`}, // not exactly one match
		{"alice@example.com bob@example.com", KindSensitiveKey, `^tok_`},
		{"4155552671", KindSensitiveKey, `^tok_`},
		{"tok_0123456789abcdef", "", `^tok_0123456789abcdef$`},
		{"user-0123456789@redacted.invalid", "", `^user-0123456789@redacted\.invalid$`},
	}
	for _, tt := range tests {
		rep := Report{}
		got := s.sensitiveValue(tt.in, rep)
		if !matches(tt.pattern, got) {
			t.Errorf("sensitiveValue(%q) = %q, want %s", tt.in, got, tt.pattern)
		}
		want := Report{}
		if tt.kind != "" {
			want[tt.kind] = 1
		}
		if !maps.Equal(rep, want) {
			t.Errorf("sensitiveValue(%q) report = %v, want %v", tt.in, rep, want)
		}
		if again := s.sensitiveValue(got, nil); again != got {
			t.Errorf("sensitiveValue(%q) = %q, not idempotent", got, again)
		}
	}
}
