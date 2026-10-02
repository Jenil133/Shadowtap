package api_test

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"reflect"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

func TestCreateOrder(t *testing.T) {
	h := newAPI(t).Handler()
	user := createUser(t, h, validUser("rosa.lindqvist@example.com"))
	req := validOrder(user.ID)

	res := call(t, h, http.MethodPost, "/api/orders", req)
	wantStatus(t, res, http.StatusCreated)
	got := decode[api.Order](t, res)
	want := api.Order{
		ID:           api.OrderID(user.ID, req.Items, 1),
		UserID:       user.ID,
		Items:        req.Items,
		TotalCents:   2*1299 + 899,
		Status:       "confirmed",
		Payment:      api.Payment{CardNumber: "4111 1111 1111 1111", Exp: "12/29"},
		BillingEmail: user.Email,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("order:\n got %+v\nwant %+v", got, want)
	}
	if loc := res.header.Get("Location"); loc != "/api/orders/"+want.ID {
		t.Errorf("Location = %q", loc)
	}
	if strings.Contains(string(res.body), "cvv") {
		t.Errorf("response carries the CVV: %s", res.body)
	}

	// The same cart again is a new order with the next sequence number.
	again := decode[api.Order](t, call(t, h, http.MethodPost, "/api/orders", req))
	if again.ID != api.OrderID(user.ID, req.Items, 2) || again.ID == got.ID {
		t.Errorf("second order id = %q, want %q", again.ID, api.OrderID(user.ID, req.Items, 2))
	}

	for _, id := range []string{got.ID, again.ID} {
		res := call(t, h, http.MethodGet, "/api/orders/"+id, nil)
		wantStatus(t, res, http.StatusOK)
		if read := decode[api.Order](t, res); read.ID != id || read.Payment != want.Payment {
			t.Errorf("GET order %s = %+v", id, read)
		}
	}
	if msg := wantError(t, call(t, h, http.MethodGet, "/api/orders/o_000000000000", nil), http.StatusNotFound); msg != "order not found" {
		t.Errorf("404 message = %q", msg)
	}
}

func TestCreateOrderCardLayouts(t *testing.T) {
	h := newAPI(t).Handler()
	user := createUser(t, h, validUser("rosa.lindqvist@example.com"))
	for _, card := range []string{
		"4111111111111111", "4012-8888-8888-1881", "5555 5555 5555 4444", "2223003122003222",
		"3782 822463 10005", "371449635398431", "6011111111111117", "3530 1113 3330 0000", "3622-720627-1667",
	} {
		req := validOrder(user.ID)
		req.Payment.CardNumber = card
		res := call(t, h, http.MethodPost, "/api/orders", req)
		wantStatus(t, res, http.StatusCreated)
		if got := decode[api.Order](t, res).Payment.CardNumber; got != card {
			t.Errorf("card echoed as %q, want %q", got, card)
		}
	}
}

func TestCreateOrderValidation(t *testing.T) {
	h := newAPI(t).Handler()
	user := createUser(t, h, validUser("rosa.lindqvist@example.com"))
	mutate := func(f func(*api.CreateOrderRequest)) api.CreateOrderRequest {
		r := validOrder(user.ID)
		f(&r)
		return r
	}
	tests := []struct {
		name  string
		req   api.CreateOrderRequest
		field string
	}{
		{"malformed user id", mutate(func(r *api.CreateOrderRequest) { r.UserID = "rosa" }), "user_id"},
		{"uppercase user id", mutate(func(r *api.CreateOrderRequest) { r.UserID = strings.ToUpper(user.ID) }), "user_id"},
		{"no items", mutate(func(r *api.CreateOrderRequest) { r.Items = nil }), "items"},
		{"too many items", mutate(func(r *api.CreateOrderRequest) { r.Items = make([]api.Item, 51) }), "items"},
		{"zero qty", mutate(func(r *api.CreateOrderRequest) { r.Items[0].Qty = 0 }), "items[0].qty"},
		{"huge qty", mutate(func(r *api.CreateOrderRequest) { r.Items[1].Qty = 1001 }), "items[1].qty"},
		{"zero price", mutate(func(r *api.CreateOrderRequest) { r.Items[0].PriceCents = 0 }), "items[0].price_cents"},
		{"short sku", mutate(func(r *api.CreateOrderRequest) { r.Items[0].SKU = "AB" }), "items[0].sku"},
		{"lowercase sku", mutate(func(r *api.CreateOrderRequest) { r.Items[0].SKU = "book-1001" }), "items[0].sku"},
		{"card fails luhn", mutate(func(r *api.CreateOrderRequest) { r.Payment.CardNumber = "4111111111111112" }), "payment.card_number"},
		{"card too short", mutate(func(r *api.CreateOrderRequest) { r.Payment.CardNumber = "411111111111" }), "payment.card_number"},
		{"card double space", mutate(func(r *api.CreateOrderRequest) { r.Payment.CardNumber = "4111  1111 1111 1111" }), "payment.card_number"},
		{"card trailing dash", mutate(func(r *api.CreateOrderRequest) { r.Payment.CardNumber = "4111111111111111-" }), "payment.card_number"},
		{"card letters", mutate(func(r *api.CreateOrderRequest) { r.Payment.CardNumber = "4111-1111-1111-111x" }), "payment.card_number"},
		{"missing card", mutate(func(r *api.CreateOrderRequest) { r.Payment.CardNumber = "" }), "payment.card_number"},
		{"short cvv", mutate(func(r *api.CreateOrderRequest) { r.Payment.CVV = "12" }), "payment.cvv"},
		{"cvv letters", mutate(func(r *api.CreateOrderRequest) { r.Payment.CVV = "12a" }), "payment.cvv"},
		{"exp month 13", mutate(func(r *api.CreateOrderRequest) { r.Payment.Exp = "13/29" }), "payment.exp"},
		{"exp without slash", mutate(func(r *api.CreateOrderRequest) { r.Payment.Exp = "1229" }), "payment.exp"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg := wantError(t, call(t, h, http.MethodPost, "/api/orders", tc.req), http.StatusUnprocessableEntity)
			if !strings.Contains(msg, tc.field+": ") {
				t.Errorf("error %q does not name field %q", msg, tc.field)
			}
			if card := tc.req.Payment.CardNumber; card != "" && strings.Contains(msg, card) {
				t.Errorf("error %q repeats the card number", msg)
			}
		})
	}

	// Validation failures do not consume a sequence number.
	got := decode[api.Order](t, call(t, h, http.MethodPost, "/api/orders", validOrder(user.ID)))
	if want := api.OrderID(user.ID, validOrder(user.ID).Items, 1); got.ID != want {
		t.Errorf("first valid order id = %q, want %q", got.ID, want)
	}
}

func TestCreateOrderUnknownUser(t *testing.T) {
	msg := wantError(t, call(t, newAPI(t).Handler(), http.MethodPost, "/api/orders", validOrder("u_000000000000")), http.StatusNotFound)
	if msg != "user not found" {
		t.Errorf("404 message = %q", msg)
	}
}

func TestLogin(t *testing.T) {
	h := newAPI(t).Handler()
	req := validUser("rosa.lindqvist@example.com")
	user := createUser(t, h, req)
	noPassword := createUser(t, h, api.CreateUserRequest{FullName: "Tara Novak", Email: "tara.novak@example.org"})

	res := call(t, h, http.MethodPost, "/api/login", api.LoginRequest{Email: "Rosa.Lindqvist@example.com", Password: req.Password})
	wantStatus(t, res, http.StatusOK)
	got := decode[api.LoginResponse](t, res)
	if got.TokenType != "Bearer" || got.ExpiresIn != 3600 {
		t.Errorf("login = %+v", got)
	}
	if cc := res.header.Get("Cache-Control"); cc != "no-store" {
		t.Errorf("Cache-Control = %q", cc)
	}

	// The token is JWT-shaped: base64url JSON header and payload, then a
	// signature, so Shadowtap's JWT detector must find it.
	parts := strings.Split(got.Token, ".")
	if len(parts) != 3 || !strings.HasPrefix(parts[0], "eyJ") || !strings.HasPrefix(parts[1], "eyJ") || parts[2] == "" {
		t.Fatalf("token %q is not JWT-shaped", got.Token)
	}
	var header map[string]string
	var claims struct {
		Iss string `json:"iss"`
		Sub string `json:"sub"`
		Iat int64  `json:"iat"`
		Exp int64  `json:"exp"`
	}
	for i, v := range []any{&header, &claims} {
		raw, err := base64.RawURLEncoding.DecodeString(parts[i])
		if err != nil {
			t.Fatalf("token part %d: %v", i, err)
		}
		if err := json.Unmarshal(raw, v); err != nil {
			t.Fatalf("token part %d: %v", i, err)
		}
	}
	if header["alg"] != "HS256" || header["typ"] != "JWT" {
		t.Errorf("token header = %v", header)
	}
	if claims.Sub != user.ID || claims.Iat != fixedNow.Unix() || claims.Exp != fixedNow.Unix()+3600 || claims.Iss == "" {
		t.Errorf("token claims = %+v", claims)
	}
	if _, err := base64.RawURLEncoding.DecodeString(parts[2]); err != nil {
		t.Errorf("signature is not base64url: %v", err)
	}

	cookie := res.header.Get("Set-Cookie")
	value, ok := strings.CutPrefix(cookie, "session=")
	if !ok || !strings.HasSuffix(value, "; Path=/; HttpOnly") || len(strings.TrimSuffix(value, "; Path=/; HttpOnly")) < 16 {
		t.Errorf("Set-Cookie = %q, want session=<random>; Path=/; HttpOnly", cookie)
	}
	second := call(t, h, http.MethodPost, "/api/login", api.LoginRequest{Email: req.Email, Password: req.Password})
	wantStatus(t, second, http.StatusOK)
	if second.header.Get("Set-Cookie") == cookie {
		t.Error("two logins got the same session cookie")
	}

	failures := []struct {
		name string
		req  api.LoginRequest
		want int
	}{
		{"wrong password", api.LoginRequest{Email: req.Email, Password: "Not-The-Password-1"}, http.StatusUnauthorized},
		{"unknown email", api.LoginRequest{Email: "nobody@example.com", Password: req.Password}, http.StatusUnauthorized},
		{"user without password", api.LoginRequest{Email: noPassword.Email, Password: "anything-at-all"}, http.StatusUnauthorized},
		{"missing password", api.LoginRequest{Email: req.Email}, http.StatusUnprocessableEntity},
		{"missing email", api.LoginRequest{Password: req.Password}, http.StatusUnprocessableEntity},
	}
	for _, tc := range failures {
		t.Run(tc.name, func(t *testing.T) {
			res := call(t, h, http.MethodPost, "/api/login", tc.req)
			wantError(t, res, tc.want)
			if res.header.Get("Set-Cookie") != "" {
				t.Error("failed login set a cookie")
			}
		})
	}
}
