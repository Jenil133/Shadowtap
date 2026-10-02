package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSeedRequestsAreValid(t *testing.T) {
	for _, req := range seedRequests() {
		req.normalize()
		if err := req.validate(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)); err != nil {
			t.Errorf("seed %s: %v", req.Email, err)
		}
		if req.Password == "" {
			t.Errorf("seed %s has no password", req.Email)
		}
	}
}

func TestSeedUsersCanLogIn(t *testing.T) {
	a, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, req := range seedRequests() {
		body, err := json.Marshal(LoginRequest{Email: req.Email, Password: req.Password})
		if err != nil {
			t.Fatal(err)
		}
		rec := httptest.NewRecorder()
		a.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/login", bytes.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Errorf("login as seed %s: status %d", req.Email, rec.Code)
		}
	}
}

func TestPanicBecomes500(t *testing.T) {
	a, err := New(Options{})
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /boom", func(http.ResponseWriter, *http.Request) { panic("boom") })
	rec := httptest.NewRecorder()
	a.middleware(mux).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/boom", nil))
	if rec.Code != http.StatusInternalServerError || !strings.Contains(rec.Body.String(), `"error"`) {
		t.Errorf("panic response: %d %s", rec.Code, rec.Body)
	}
}

func TestValidCard(t *testing.T) {
	tests := []struct {
		card string
		want bool
	}{
		{"4111111111111111", true},
		{"4111 1111 1111 1111", true},
		{"4111-1111-1111-1111", true},
		{"378282246310005", true},
		{"3782 822463 10005", true},
		{"36227206271667", true},
		{"4111111111111112", false}, // Luhn
		{"411111111111", false},     // 12 digits
		{"41111111111111111111", false},
		{" 4111111111111111", false},
		{"4111  1111 1111 1111", false},
		{"4111 -1111 1111 1111", false},
		{"4111_1111_1111_1111", false},
		{"", false},
	}
	for _, tc := range tests {
		if got := validCard(tc.card); got != tc.want {
			t.Errorf("validCard(%q) = %v, want %v", tc.card, got, tc.want)
		}
	}
}

func TestValidEmail(t *testing.T) {
	tests := []struct {
		email string
		want  bool
	}{
		{"alice.anders1@example.com", true},
		{"Alice.Anders1@Example.ORG", true},
		{"a+tag@sub.example.com", true},
		{"alice@example", false},
		{"alice@.example.com", false},
		{"alice@example.com.", false},
		{"@example.com", false},
		{"alice", false},
		{"Alice <alice@example.com>", false},
		{"alice@exa mple.com", false},
		{strings.Repeat("a", 250) + "@example.com", false},
	}
	for _, tc := range tests {
		if got := validEmail(tc.email); got != tc.want {
			t.Errorf("validEmail(%q) = %v, want %v", tc.email, got, tc.want)
		}
	}
}

func TestValidPhone(t *testing.T) {
	for phone, want := range map[string]bool{
		"(415) 555-0123":  true,
		"415-555-0147":    true,
		"+1 415 555 0199": true,
		"415.555.0123":    true,
		"+14155550123":    true,
		"555-0123":        false,
		"1+415 555 0199":  false,
		"415-555-01x3":    false,
	} {
		if got := validPhone(phone); got != want {
			t.Errorf("validPhone(%q) = %v, want %v", phone, got, want)
		}
	}
}

func TestPasswordHash(t *testing.T) {
	h, err := hashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if !h.matches("correct horse battery") || h.matches("correct horse battery!") || h.matches("") {
		t.Error("password hash matched the wrong passwords")
	}
	if (passwordHash{}).matches("") {
		t.Error("zero hash matched")
	}
	other, err := hashPassword("correct horse battery")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(h.key, other.key) {
		t.Error("hashes are not salted")
	}
}
