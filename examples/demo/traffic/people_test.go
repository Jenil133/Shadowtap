package traffic

import (
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestPeopleDeterministic(t *testing.T) {
	a, b := People(1, 50), People(1, 50)
	if !reflect.DeepEqual(a, b) {
		t.Fatal("same seed produced different people")
	}
	if !reflect.DeepEqual(People(1, 20), a[:20]) {
		t.Error("People(1, 20) is not a prefix of People(1, 50)")
	}
	if reflect.DeepEqual(People(2, 50), a) {
		t.Error("different seeds produced the same people")
	}
	for _, n := range []int{0, -1} {
		if got := People(1, n); len(got) != 0 {
			t.Errorf("People(1, %d) returned %d people", n, len(got))
		}
	}
}

func TestPeopleFormats(t *testing.T) {
	var (
		emailRe    = regexp.MustCompile(`^[a-z]+\.[a-z]+[0-9]+@example\.(com|org)$`)
		ssnRe      = regexp.MustCompile(`^987-65-432[0-9]$`)
		passwordRe = regexp.MustCompile(`^[` + passwordAlphabet + `]{12}![0-9]$`)
		streetRe   = regexp.MustCompile(`^[1-9][0-9]{1,3} [A-Z][a-z]+ [A-Z][a-z]+$`)
		phoneRes   = []*regexp.Regexp{
			regexp.MustCompile(`^\([2-9][0-9]{2}\) 555-01[0-9]{2}$`),
			regexp.MustCompile(`^[2-9][0-9]{2}-555-01[0-9]{2}$`),
			regexp.MustCompile(`^\+1 [2-9][0-9]{2} 555 01[0-9]{2}$`),
			regexp.MustCompile(`^[2-9][0-9]{2}\.555\.01[0-9]{2}$`),
		}
	)
	phoneLayouts := make([]int, len(phoneRes))
	cardLayouts := map[string]int{}
	emails := map[string]bool{}

	for _, p := range People(7, 2000) {
		if p.FullName != p.FirstName+" "+p.LastName || !slices.Contains(firstNames[:], p.FirstName) || !slices.Contains(lastNames[:], p.LastName) {
			t.Errorf("name %q not built from the fixed lists", p.FullName)
		}
		if !emailRe.MatchString(p.Email) || emails[p.Email] {
			t.Errorf("email %q malformed or repeated", p.Email)
		}
		emails[p.Email] = true
		matched := false
		for i, re := range phoneRes {
			if re.MatchString(p.Phone) {
				phoneLayouts[i]++
				matched = true
			}
		}
		if !matched {
			t.Errorf("phone %q outside the 555-01XX layouts", p.Phone)
		}
		if !ssnRe.MatchString(p.SSN) {
			t.Errorf("SSN %q outside 987-65-4320..4329", p.SSN)
		}
		if d, err := time.Parse(time.DateOnly, p.DOB); err != nil || d.Year() < 1950 || d.Year() > 2004 {
			t.Errorf("DOB %q invalid", p.DOB)
		}
		if !streetRe.MatchString(p.Street) {
			t.Errorf("street %q malformed", p.Street)
		}
		if !slices.ContainsFunc(places[:], func(pl struct{ city, zip string }) bool { return pl.city == p.City && pl.zip == p.Zip }) {
			t.Errorf("city/zip %q/%q not from the fixed list", p.City, p.Zip)
		}
		if !passwordRe.MatchString(p.Password) {
			t.Errorf("password %q malformed", p.Password)
		}
		cardLayouts[checkCard(t, p.Card)]++
		wantCVV := 3
		if strings.HasPrefix(p.Card, "34") || strings.HasPrefix(p.Card, "37") {
			wantCVV = 4 // American Express
		}
		if len(p.CVV) != wantCVV {
			t.Errorf("CVV %q for card %q: want %d digits", p.CVV, p.Card, wantCVV)
		}
	}
	for i, n := range phoneLayouts {
		if n == 0 {
			t.Errorf("phone layout %s never generated", phoneRes[i])
		}
	}
	for _, layout := range []string{"compact", " ", "-"} {
		if cardLayouts[layout] == 0 {
			t.Errorf("card layout %q never generated", layout)
		}
	}
}

// checkCard verifies a formatted card is a known test number grouped the
// way its brand prints it, and returns its layout: "compact", " " or "-".
func checkCard(t *testing.T, card string) string {
	t.Helper()
	digits := strings.NewReplacer(" ", "", "-", "").Replace(card)
	if !slices.Contains(testCards[:], digits) {
		t.Errorf("card %q is not a public test number", card)
		return ""
	}
	if digits == card {
		return "compact"
	}
	sep := " "
	if strings.Contains(card, "-") {
		sep = "-"
	}
	var lens []int
	for _, g := range strings.Split(card, sep) {
		lens = append(lens, len(g))
	}
	want := map[int][]int{14: {4, 6, 4}, 15: {4, 6, 5}, 16: {4, 4, 4, 4}}[len(digits)]
	if !slices.Equal(lens, want) {
		t.Errorf("card %q grouped %v, want %v", card, lens, want)
	}
	return sep
}

func TestTestCardsPassLuhn(t *testing.T) {
	for _, card := range testCards {
		sum := 0
		for i := range len(card) {
			d := int(card[len(card)-1-i] - '0')
			if i%2 == 1 {
				if d *= 2; d > 9 {
					d -= 9
				}
			}
			sum += d
		}
		if sum%10 != 0 {
			t.Errorf("test card %s fails the Luhn check", card)
		}
	}
}

func TestFormatCard(t *testing.T) {
	tests := []struct {
		digits string
		style  int
		want   string
	}{
		{"4111111111111111", 0, "4111111111111111"},
		{"4111111111111111", 2, "4111 1111 1111 1111"},
		{"4111111111111111", 3, "4111-1111-1111-1111"},
		{"378282246310005", 2, "3782 822463 10005"},
		{"36227206271667", 3, "3622-720627-1667"},
	}
	for _, tc := range tests {
		if got := formatCard(tc.digits, tc.style); got != tc.want {
			t.Errorf("formatCard(%s, %d) = %q, want %q", tc.digits, tc.style, got, tc.want)
		}
	}
}
