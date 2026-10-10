package api_test

import (
	"encoding/json"
	"net/http"
	"slices"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

func newV2(t *testing.T) *api.API {
	t.Helper()
	a, err := api.New(api.Options{Version: api.VersionV2, Now: func() time.Time { return fixedNow }})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestV2Regressions(t *testing.T) {
	v1, v2 := newAPI(t).Handler(), newV2(t).Handler()
	seed := api.SeedUsers()[0]

	// Users: phone is renamed phone_number.
	res := call(t, v2, http.MethodGet, "/api/users/"+seed.ID, nil)
	wantStatus(t, res, http.StatusOK)
	var u map[string]any
	if err := json.Unmarshal(res.body, &u); err != nil {
		t.Fatal(err)
	}
	if _, ok := u["phone"]; ok || u["phone_number"] != seed.Phone {
		t.Errorf("v2 user: %v", u)
	}
	res = call(t, v1, http.MethodGet, "/api/users/"+seed.ID, nil)
	if got := decode[api.User](t, res); got.Phone != seed.Phone {
		t.Errorf("v1 user changed: %+v", got)
	}

	// List: descending id order.
	ids := func(h http.Handler) []string {
		res := call(t, h, http.MethodGet, "/api/users", nil)
		var l struct {
			Users []struct {
				ID string `json:"id"`
			} `json:"users"`
			GeneratedAt string `json:"generated_at"`
		}
		if err := json.Unmarshal(res.body, &l); err != nil {
			t.Fatal(err)
		}
		if l.GeneratedAt == "" {
			t.Error("list without generated_at")
		}
		var out []string
		for _, u := range l.Users {
			out = append(out, u.ID)
		}
		return out
	}
	asc, desc := ids(v1), ids(v2)
	slices.Reverse(desc)
	if !slices.Equal(asc, desc) || slices.Equal(asc, ids(v2)) {
		t.Errorf("v1 %v, v2 %v: want reversed order", asc, ids(v2))
	}

	// Invalid order: 400 instead of 422, and 40 ms slower.
	bad := `{"user_id":"` + seed.ID + `","items":[{"sku":"BOOK-1","qty":0,"price_cents":100}],"payment":{"card_number":"4111111111111111","cvv":"123","exp":"12/30"}}`
	start := time.Now()
	res = call(t, v2, http.MethodPost, "/api/orders", bad)
	if res.status != http.StatusBadRequest || time.Since(start) < api.OrdersDelayV2 {
		t.Errorf("v2 invalid order: %d after %v", res.status, time.Since(start))
	}
	res = call(t, v1, http.MethodPost, "/api/orders", bad)
	wantStatus(t, res, http.StatusUnprocessableEntity)
}
