package scrub

import (
	"fmt"
	"math/rand/v2"
	"net/netip"
	"regexp"
	"testing"
)

func TestPseudonymFormats(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		kind, in string
		pattern  string // the pseudonym must match it
	}{
		{KindEmail, "Alice@Example.com", `^user-[0-9a-f]{10}@redacted\.invalid$`},
		{KindJWT, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.sig", `^redacted\.jwt\.[0-9a-f]{16}$`},
		{KindSecret, "AKIAIOSFODNN7EXAMPLE", `^redacted\.secret\.[0-9a-f]{16}$`},
		{KindSensitiveKey, "hunter2", `^tok_[0-9a-f]{16}$`},
		{KindCard, "4111 1111 1111 1111", `^0\d{3} \d{4} \d{4} \d{4}$`},
		{KindCard, "3782-822463-10005", `^0\d{3}-\d{6}-\d{5}$`},
		{KindCard, "4222222222222", `^0\d{12}$`},
		{KindSSN, "123-45-6789", `^000-\d{2}-\d{4}$`},
		{KindSSN, "123456789", `^000-\d{2}-\d{4}$`}, // bare SSNs get the dashed marker
		{KindPhone, "(415) 555-2671", `^\(0\d{2}\) 0\d{2}-0\d{3}$`},
		{KindPhone, "415.555.2671", `^0\d{2}\.0\d{2}\.0\d{3}$`},
		{KindPhone, "+44 20 7946 0958", `^\+0\d 0\d 0\d{3} 0\d{3}$`},
		{KindPhone, "+14155552671", `^\+0\d{10}$`},
		{KindIPv4, "192.168.1.10", `^(24\d|25[0-4])\.\d{1,3}\.\d{1,3}\.\d{1,3}$`},
		{KindIPv6, "2001:db8::1", `^100::`},
	}
	for _, tt := range tests {
		t.Run(tt.kind+"/"+tt.in, func(t *testing.T) {
			p := s.pseudonym(tt.kind, tt.in)
			if !regexp.MustCompile(tt.pattern).MatchString(p) {
				t.Errorf("pseudonym(%s, %q) = %q, want match for %s", tt.kind, tt.in, p, tt.pattern)
			}
			if !IsPseudonym(p) {
				t.Errorf("IsPseudonym(%q) = false", p)
			}
			assertClean(t, "pseudonym", p)
			if tt.kind == KindCard && !luhnValid([]byte(digitsOf(p))) {
				t.Errorf("card pseudonym %q fails the Luhn check", p)
			}
			if tt.kind == KindIPv6 {
				if a, err := netip.ParseAddr(p); err != nil || !inDiscardPrefix(a) {
					t.Errorf("ipv6 pseudonym %q is not in 100::/64", p)
				}
			}
		})
	}
}

func TestPseudonymDeterministic(t *testing.T) {
	in := "mail alice@example.com, card 4111 1111 1111 1111, ssn 123-45-6789, call +1 415 555 2671 from 192.168.1.10"
	s1, s2 := newTestScrubber(t), newTestScrubber(t)
	other := newTestScrubber(t, func(c *Config) { c.Key = []byte(otherKey) })
	a, b, c := s1.Text(in, nil), s2.Text(in, nil), other.Text(in, nil)
	if a != b {
		t.Errorf("two scrubbers with one key disagree:\n%s\n%s", a, b)
	}
	if c == a {
		t.Errorf("a different key gave the same output %q", c)
	}
	for _, tt := range []struct{ kind, v string }{
		{KindEmail, "alice@example.com"}, {KindCard, "4111111111111111"}, {KindSSN, "123-45-6789"},
		{KindPhone, "+14155552671"}, {KindIPv4, "192.168.1.10"}, {KindIPv6, "2001:db8::1"},
		{KindJWT, "eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.sig"}, {KindSecret, "AKIAIOSFODNN7EXAMPLE"},
		{KindSensitiveKey, "hunter2"},
	} {
		p, q := s1.pseudonym(tt.kind, tt.v), other.pseudonym(tt.kind, tt.v)
		if p == q {
			t.Errorf("%s pseudonym %q is the same under both keys", tt.kind, p)
		}
		if again := s2.pseudonym(tt.kind, tt.v); again != p {
			t.Errorf("%s pseudonym %q != %q from a second scrubber", tt.kind, again, p)
		}
	}
}

// TestPseudonymNormalization checks that formatting variants of one value
// share their pseudonym's digits.
func TestPseudonymNormalization(t *testing.T) {
	s := newTestScrubber(t)
	groups := []struct {
		kind     string
		variants []string
	}{
		{KindEmail, []string{"alice@example.com", "Alice@Example.COM", "ALICE@EXAMPLE.COM"}},
		{KindCard, []string{"4111111111111111", "4111 1111 1111 1111", "4111-1111-1111-1111"}},
		{KindPhone, []string{"(415) 555-2671", "415-555-2671", "415.555.2671", "415 555 2671"}},
		{KindIPv4, []string{"10.1.2.3", "010.001.002.003"}},
	}
	for _, g := range groups {
		want := digitsOrSelf(g.kind, s.pseudonym(g.kind, g.variants[0]))
		for _, v := range g.variants[1:] {
			if got := digitsOrSelf(g.kind, s.pseudonym(g.kind, v)); got != want {
				t.Errorf("%s: pseudonym of %q has %q, want %q (as for %q)", g.kind, v, got, want, g.variants[0])
			}
		}
	}
}

func digitsOrSelf(kind, p string) string {
	if kind == KindCard || kind == KindPhone {
		return digitsOf(p)
	}
	return p
}

// TestPseudonymRegenerates covers a phone layout whose digits, once zeroed
// at each run start, form a valid SSN. The generator must avoid it.
func TestPseudonymRegenerates(t *testing.T) {
	s := newTestScrubber(t)
	for i := range 50 {
		in := fmt.Sprintf("+1 2%02d-%d5-6%03d", i, i%10, i*7)
		p := s.pseudonym(KindPhone, in)
		assertClean(t, "phone pseudonym of "+in, p)
		if !IsPseudonym(p) {
			t.Errorf("IsPseudonym(%q) = false", p)
		}
	}
}

func TestZeroLayoutsAreInert(t *testing.T) {
	for _, tt := range []struct{ kind, in string }{
		{KindEmail, "a@b.co"}, {KindJWT, "x"}, {KindSecret, "x"}, {KindSensitiveKey, "x"},
		{KindCard, "4111 1111 1111 1111"}, {KindSSN, "123-45-6789"}, {KindSSN, "123456789"},
		{KindPhone, "+1 (415) 555-2671"}, {KindPhone, "415.555.2671"},
		{KindIPv4, "1.2.3.4"}, {KindIPv6, "2001:db8::1"},
	} {
		p := render(tt.kind, tt.in, nil)
		assertClean(t, "zero layout", p)
		if !IsPseudonym(p) {
			t.Errorf("IsPseudonym(zero layout %q) = false", p)
		}
	}
}

// TestPseudonymsAreStable generates pseudonyms for many random values and
// checks each one is recognized and left alone by Text.
func TestPseudonymsAreStable(t *testing.T) {
	s := newTestScrubber(t)
	rng := rand.New(rand.NewPCG(7, 11))
	digits := func(n int) string {
		b := make([]byte, n)
		for i := range b {
			b[i] = byte('0' + rng.IntN(10))
		}
		return string(b)
	}
	for range 3000 {
		var kind, v string
		switch rng.IntN(7) {
		case 0:
			kind, v = KindEmail, "u"+digits(6)+"@example.com"
		case 1:
			kind, v = KindCard, "4"+digits(3)+" "+digits(4)+" "+digits(4)+" "+digits(4)
		case 2:
			kind, v = KindSSN, digits(3)+"-"+digits(2)+"-"+digits(4)
		case 3:
			kind, v = KindPhone, "+"+fmt.Sprint(1+rng.IntN(9))+" "+digits(3)+"-"+digits(3)+"-"+digits(4)
		case 4:
			kind, v = KindPhone, "("+digits(3)+") "+digits(3)+"-"+digits(4)
		case 5:
			kind, v = KindIPv4, fmt.Sprintf("%d.%d.%d.%d", rng.IntN(256), rng.IntN(256), rng.IntN(256), rng.IntN(256))
		default:
			kind, v = KindSensitiveKey, digits(12)
		}
		p := s.pseudonym(kind, v)
		if !IsPseudonym(p) {
			t.Fatalf("IsPseudonym(%q) = false (%s of %q)", p, kind, v)
		}
		if got := s.Text(p, nil); got != p {
			t.Fatalf("Text(%q) = %q, want unchanged", p, got)
		}
	}
}

func TestIsPseudonym(t *testing.T) {
	yes := []string{
		"tok_0123456789abcdef",
		"user-0123456789@redacted.invalid",
		"redacted.jwt.0123456789abcdef",
		"redacted.secret.fedcba9876543210",
		"0000 0000 0000 0000",
		"0000-0000-0000-0000",
		"000-12-3456",
		"+0 012 034 0567",
		"(012) 034-0567",
		"012-034-0567",
		"240.1.2.3",
		"254.255.255.255",
		"100::1",
		"100::dead:beef:1:2",
		"::ffff:240.1.2.3",
	}
	for _, v := range yes {
		if !IsPseudonym(v) {
			t.Errorf("IsPseudonym(%q) = false, want true", v)
		}
	}
	no := []string{
		"", "tok_", "tok_0123456789ABCDEF", "tok_0123456789abcde", "tok_0123456789abcdef0",
		"user-0123456789@example.com", "user-xyz@redacted.invalid", "USER-0123456789@redacted.invalid",
		"redacted.jwt.", "redacted.secret.xyz",
		"4111 1111 1111 1111",   // a real card
		"0111 1111 1111 1112",   // Luhn fails
		"0 4111111111111111",    // card shape hiding a real card
		"123-45-6789",           // a real SSN
		"000123456",             // a zero-padded number, not the SSN marker
		"+1 415 555 2671",       // a real phone number
		"(415) 555-2671",        // a real phone number
		"+0 012 034 1567",       // a run starting with 1
		"192.168.1.10",          // a real address
		"255.1.2.3",             // outside 240-254
		"240.01.2.3",            // not canonical
		"2001:db8::1",           // not in 100::/64
		"100:0::1",              // not canonical
		"[100::1]",              // bracketed
		"tok_0123456789abcdef ", // trailing space
		"alice@example.com", "hunter2",
	}
	for _, v := range no {
		if IsPseudonym(v) {
			t.Errorf("IsPseudonym(%q) = true, want false", v)
		}
	}
}
