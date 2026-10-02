// Package traffic generates seeded, realistic request traffic for the demo
// API and records every piece of personal data it puts on the wire.
//
// All personal data is fake and drawn from reserved ranges: email addresses
// at example.com and example.org, phone numbers in the fictional NANP
// 555-0100..0199 block, SSNs from the 987-65-4320..4329 block reserved for
// advertising, card numbers from the public test-card list, and client IPs
// from documentation prefixes. The Ledger lets leak checks search captured
// data for any of it.
package traffic

import (
	"fmt"
	"math/rand/v2"
	"strings"
)

// Person is a fake person. Every field except City and Zip is personal data
// that Shadowtap must scrub.
type Person struct {
	FullName  string
	FirstName string
	LastName  string
	Email     string
	Phone     string // one of several layouts, e.g. "(415) 555-0123" or "+1 415 555 0199"
	SSN       string
	DOB       string // YYYY-MM-DD
	Street    string
	City      string
	Zip       string
	Card      string // a public test card number, possibly grouped with spaces or dashes
	CVV       string
	Password  string
}

// peopleStream separates people's random streams from the generator's.
const peopleStream = 0x70656f706c65 // "people"

// People returns n deterministic fake people. Person i depends only on seed
// and i, so People(s, n)[:m] equals People(s, m) for m <= n. Email addresses
// are unique within one call.
func People(seed int64, n int) []Person {
	if n <= 0 {
		return nil
	}
	people := make([]Person, n)
	for i := range people {
		rng := rand.New(rand.NewPCG(uint64(seed)^peopleStream, uint64(i)))
		people[i] = newPerson(rng, i)
	}
	return people
}

func newPerson(rng *rand.Rand, i int) Person {
	first := pick(rng, firstNames[:])
	last := pick(rng, lastNames[:])
	domain := pick(rng, emailDomains[:])
	place := pick(rng, places[:])
	card := pick(rng, testCards[:])
	return Person{
		FullName:  first + " " + last,
		FirstName: first,
		LastName:  last,
		// The index keeps addresses unique however often names repeat.
		Email:    fmt.Sprintf("%s.%s%d@%s", strings.ToLower(first), strings.ToLower(last), i+1, domain),
		Phone:    phone(rng),
		SSN:      fmt.Sprintf("987-65-432%d", rng.IntN(10)),
		DOB:      fmt.Sprintf("%04d-%02d-%02d", 1950+rng.IntN(55), 1+rng.IntN(12), 1+rng.IntN(28)),
		Street:   fmt.Sprintf("%d %s %s", 10+rng.IntN(9990), pick(rng, streetNames[:]), pick(rng, streetSuffixes[:])),
		City:     place.city,
		Zip:      place.zip,
		Card:     formatCard(card, rng.IntN(4)),
		CVV:      cvv(rng, card),
		Password: randomPassword(rng, 12),
	}
}

func pick[T any](rng *rand.Rand, xs []T) T { return xs[rng.IntN(len(xs))] }

// phone returns a number in the fictional 555-0100..0199 block in one of the
// layouts Shadowtap's phone detector recognizes.
func phone(rng *rand.Rand) string {
	area := pick(rng, areaCodes[:])
	line := 100 + rng.IntN(100)
	switch rng.IntN(4) {
	case 0:
		return fmt.Sprintf("(%d) 555-%04d", area, line)
	case 1:
		return fmt.Sprintf("%d-555-%04d", area, line)
	case 2:
		return fmt.Sprintf("+1 %d 555 %04d", area, line)
	default:
		return fmt.Sprintf("%d.555.%04d", area, line)
	}
}

// formatCard lays digits out compactly (styles 0 and 1) or grouped the way
// the brand prints them, with spaces (2) or dashes (3).
func formatCard(digits string, style int) string {
	var sep string
	switch style {
	case 2:
		sep = " "
	case 3:
		sep = "-"
	default:
		return digits
	}
	groups := []int{4, 4, 4, 4}
	switch len(digits) {
	case 15: // American Express
		groups = []int{4, 6, 5}
	case 14: // Diners Club
		groups = []int{4, 6, 4}
	}
	parts := make([]string, 0, len(groups))
	for _, g := range groups {
		if g > len(digits) {
			g = len(digits)
		}
		parts = append(parts, digits[:g])
		digits = digits[g:]
	}
	if digits != "" {
		parts = append(parts, digits)
	}
	return strings.Join(parts, sep)
}

// cvv returns four digits for American Express cards and three otherwise.
func cvv(rng *rand.Rand, card string) string {
	if strings.HasPrefix(card, "34") || strings.HasPrefix(card, "37") {
		return fmt.Sprintf("%04d", rng.IntN(10000))
	}
	return fmt.Sprintf("%03d", rng.IntN(1000))
}

// passwordAlphabet avoids look-alike characters and anything JSON or URL
// encoding would escape, so passwords appear verbatim on the wire.
const passwordAlphabet = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

func randomPassword(rng *rand.Rand, n int) string {
	b := make([]byte, n, n+3)
	for i := range b {
		b[i] = passwordAlphabet[rng.IntN(len(passwordAlphabet))]
	}
	return string(fmt.Appendf(b, "!%d", rng.IntN(10)))
}

// Read-only lookup tables.
var (
	firstNames = [...]string{
		"Alice", "Bruno", "Chloe", "Diego", "Elena", "Farah", "Gavin", "Hana", "Ivan", "Julia",
		"Kenji", "Leila", "Marco", "Nadia", "Oscar", "Priya", "Quinn", "Rosa", "Samir", "Tara",
	}
	lastNames = [...]string{
		"Anders", "Brooks", "Castillo", "Dubois", "Eriksen", "Fischer", "Garcia", "Haddad", "Ito", "Jensen",
		"Kowalski", "Lindqvist", "Moreau", "Novak", "Okafor", "Petrov", "Quintero", "Rossi", "Suzuki", "Tanaka",
	}
	emailDomains   = [...]string{"example.com", "example.org"}
	areaCodes      = [...]int{206, 212, 303, 305, 312, 404, 415, 512, 617, 702, 718, 808}
	streetNames    = [...]string{"Maple", "Oak", "Cedar", "Pine", "Birch", "Willow", "Elm", "Aspen", "Juniper", "Magnolia", "Sycamore", "Hawthorn"}
	streetSuffixes = [...]string{"Street", "Avenue", "Road", "Lane", "Drive", "Court", "Way", "Boulevard"}
	places         = [...]struct{ city, zip string }{
		{"Springfield", "62704"}, {"Riverton", "84065"}, {"Fairview", "97024"}, {"Lakewood", "80226"},
		{"Greenville", "29601"}, {"Madison", "53703"}, {"Franklin", "37064"}, {"Georgetown", "78626"},
		{"Salem", "97301"}, {"Clinton", "52732"},
	}
	// testCards are the payment networks' published test numbers.
	testCards = [...]string{
		"4111111111111111", "4012888888881881", // Visa
		"5555555555554444", "5105105105105100", "2223003122003222", // Mastercard
		"378282246310005", "371449635398431", // American Express
		"6011111111111117",                     // Discover
		"3530111333300000", "3566002020360505", // JCB
		"36227206271667", // Diners Club
	}
)
