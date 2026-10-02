package api

import (
	"net/mail"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Validation limits.
const (
	maxNameRunes     = 100
	maxStreetRunes   = 200
	maxCityRunes     = 100
	maxEmailBytes    = 254
	minPasswordRunes = 8
	maxPasswordRunes = 128
	maxOrderItems    = 50
	maxItemQty       = 1000
	maxPriceCents    = 100_000_000
)

// validationError lists every problem with a request. Messages name fields
// and never repeat submitted values, because error bodies are captured and
// logged.
type validationError struct {
	problems []string
}

func (e *validationError) Error() string { return strings.Join(e.problems, "; ") }

type problems []string

func (p *problems) add(field, msg string) { *p = append(*p, field+": "+msg) }

func (p problems) err() error {
	if len(p) == 0 {
		return nil
	}
	return &validationError{problems: p}
}

func (r *CreateUserRequest) normalize() {
	r.FullName = strings.TrimSpace(r.FullName)
	r.Email = strings.TrimSpace(r.Email)
	r.Phone = strings.TrimSpace(r.Phone)
	r.SSN = strings.TrimSpace(r.SSN)
	r.DOB = strings.TrimSpace(r.DOB)
	if r.Address != nil {
		a := *r.Address // never mutate the caller's address
		a.Street = strings.TrimSpace(a.Street)
		a.City = strings.TrimSpace(a.City)
		a.Zip = strings.TrimSpace(a.Zip)
		r.Address = &a
	}
}

// validate checks a normalized request. now bounds the date of birth.
func (r *CreateUserRequest) validate(now time.Time) error {
	var p problems
	switch {
	case r.FullName == "":
		p.add("full_name", "is required")
	case !validText(r.FullName, maxNameRunes):
		p.add("full_name", "must be at most 100 printable characters")
	}
	switch {
	case r.Email == "":
		p.add("email", "is required")
	case !validEmail(r.Email):
		p.add("email", "must be a valid email address")
	}
	if r.Phone != "" && !validPhone(r.Phone) {
		p.add("phone", "must hold 10 to 15 digits")
	}
	if r.SSN != "" && !validSSN(r.SSN) {
		p.add("ssn", "must look like 123-45-6789")
	}
	if r.DOB != "" && !validDOB(r.DOB, now) {
		p.add("dob", "must be a past date formatted YYYY-MM-DD")
	}
	if a := r.Address; a != nil {
		if a.Street == "" || !validText(a.Street, maxStreetRunes) {
			p.add("address.street", "is required and must be at most 200 printable characters")
		}
		if a.City == "" || !validText(a.City, maxCityRunes) {
			p.add("address.city", "is required and must be at most 100 printable characters")
		}
		if !validZip(a.Zip) {
			p.add("address.zip", "must look like 12345 or 12345-6789")
		}
	}
	if r.Password != "" {
		if n := utf8.RuneCountInString(r.Password); n < minPasswordRunes || n > maxPasswordRunes {
			p.add("password", "must be 8 to 128 characters")
		}
	}
	return p.err()
}

func (r *CreateOrderRequest) normalize() {
	r.UserID = strings.TrimSpace(r.UserID)
	r.Payment.CardNumber = strings.TrimSpace(r.Payment.CardNumber)
	r.Payment.CVV = strings.TrimSpace(r.Payment.CVV)
	r.Payment.Exp = strings.TrimSpace(r.Payment.Exp)
}

func (r *CreateOrderRequest) validate() error {
	var p problems
	if !validID(r.UserID, "u_") {
		p.add("user_id", "must be a user id like u_0123456789ab")
	}
	switch n := len(r.Items); {
	case n == 0:
		p.add("items", "must not be empty")
	case n > maxOrderItems:
		p.add("items", "must hold at most 50 lines")
	}
	for i, it := range r.Items {
		field := "items[" + strconv.Itoa(i) + "]"
		if !validSKU(it.SKU) {
			p.add(field+".sku", "must be 3 to 32 characters of A-Z, 0-9 and '-'")
		}
		if it.Qty < 1 || it.Qty > maxItemQty {
			p.add(field+".qty", "must be between 1 and 1000")
		}
		if it.PriceCents < 1 || it.PriceCents > maxPriceCents {
			p.add(field+".price_cents", "must be between 1 and 100000000")
		}
	}
	if !validCard(r.Payment.CardNumber) {
		p.add("payment.card_number", "must be a valid card number")
	}
	if n := len(r.Payment.CVV); n < 3 || n > 4 || !allDigits(r.Payment.CVV) {
		p.add("payment.cvv", "must be 3 or 4 digits")
	}
	if !validExp(r.Payment.Exp) {
		p.add("payment.exp", "must look like MM/YY")
	}
	return p.err()
}

func (r *LoginRequest) validate() error {
	var p problems
	if strings.TrimSpace(r.Email) == "" {
		p.add("email", "is required")
	}
	if r.Password == "" {
		p.add("password", "is required")
	}
	return p.err()
}

// validText reports whether s has at most max runes and no control
// characters.
func validText(s string, max int) bool {
	if !utf8.ValidString(s) || utf8.RuneCountInString(s) > max {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) {
			return false
		}
	}
	return true
}

// validEmail accepts a bare addr-spec ("local@domain.tld") without a display
// name or angle brackets.
func validEmail(s string) bool {
	if len(s) > maxEmailBytes || strings.ContainsAny(s, " <>\"") {
		return false
	}
	addr, err := mail.ParseAddress(s)
	if err != nil || addr.Address != s {
		return false
	}
	at := strings.LastIndexByte(s, '@')
	domain := s[at+1:]
	return at > 0 && strings.Contains(domain, ".") &&
		!strings.HasPrefix(domain, ".") && !strings.HasSuffix(domain, ".")
}

// validPhone accepts 10 to 15 digits with common separators and an optional
// leading '+'.
func validPhone(s string) bool {
	digits := 0
	for i, c := range s {
		switch {
		case '0' <= c && c <= '9':
			digits++
		case c == '+' && i == 0:
		case c == ' ' || c == '-' || c == '.' || c == '(' || c == ')':
		default:
			return false
		}
	}
	return digits >= 10 && digits <= 15
}

func validSSN(s string) bool {
	return len(s) == 11 && s[3] == '-' && s[6] == '-' &&
		allDigits(s[:3]) && allDigits(s[4:6]) && allDigits(s[7:])
}

func validDOB(s string, now time.Time) bool {
	d, err := time.Parse(time.DateOnly, s)
	if err != nil {
		return false
	}
	return d.Year() >= 1900 && d.Before(now)
}

func validZip(s string) bool {
	switch len(s) {
	case 5:
		return allDigits(s)
	case 10:
		return s[5] == '-' && allDigits(s[:5]) && allDigits(s[6:])
	}
	return false
}

// validID reports whether s is prefix followed by 12 lowercase hex digits.
func validID(s, prefix string) bool {
	hexPart, ok := strings.CutPrefix(s, prefix)
	if !ok || len(hexPart) != 2*idHexBytes {
		return false
	}
	for i := 0; i < len(hexPart); i++ {
		if c := hexPart[i]; !('0' <= c && c <= '9' || 'a' <= c && c <= 'f') {
			return false
		}
	}
	return true
}

func validSKU(s string) bool {
	if len(s) < 3 || len(s) > 32 || s[0] == '-' {
		return false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; !('A' <= c && c <= 'Z' || '0' <= c && c <= '9' || c == '-') {
			return false
		}
	}
	return true
}

// validCard accepts 13 to 19 digits, optionally grouped by single spaces or
// dashes, that pass the Luhn check.
func validCard(s string) bool {
	digits := make([]byte, 0, 19)
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case '0' <= c && c <= '9':
			digits = append(digits, c)
		case (c == ' ' || c == '-') && i > 0 && i < len(s)-1 && isDigit(s[i-1]) && isDigit(s[i+1]):
		default:
			return false
		}
	}
	return len(digits) >= 13 && len(digits) <= 19 && luhn(digits)
}

func luhn(digits []byte) bool {
	sum := 0
	for i := range digits {
		d := int(digits[len(digits)-1-i] - '0')
		if i%2 == 1 {
			if d *= 2; d > 9 {
				d -= 9
			}
		}
		sum += d
	}
	return sum%10 == 0
}

// validExp accepts MM/YY with a month from 01 to 12. Expiry is deliberately
// not compared with the clock so recorded traffic stays valid when replayed.
func validExp(s string) bool {
	if len(s) != 5 || s[2] != '/' || !allDigits(s[:2]) || !allDigits(s[3:]) {
		return false
	}
	month := int(s[0]-'0')*10 + int(s[1]-'0')
	return month >= 1 && month <= 12
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		if !isDigit(s[i]) {
			return false
		}
	}
	return true
}

func isDigit(c byte) bool { return '0' <= c && c <= '9' }
