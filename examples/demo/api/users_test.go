package api_test

import (
	"net/http"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

func TestNewVersion(t *testing.T) {
	tests := []struct {
		version string
		wantErr bool
	}{
		{version: "", wantErr: false},
		{version: "v1", wantErr: false},
		{version: "v2", wantErr: true},
		{version: "V1", wantErr: true},
		{version: "latest", wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.version, func(t *testing.T) {
			a, err := api.New(api.Options{Version: tc.version})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("New(%q) succeeded, want error", tc.version)
				}
				return
			}
			if err != nil {
				t.Fatalf("New(%q): %v", tc.version, err)
			}
			if a.Version() != api.VersionV1 {
				t.Errorf("Version() = %q, want %q", a.Version(), api.VersionV1)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	res := call(t, newAPI(t).Handler(), http.MethodGet, "/healthz", nil)
	wantStatus(t, res, http.StatusOK)
	if got := decode[api.Health](t, res); got != (api.Health{Status: "ok", Version: "v1"}) {
		t.Errorf("healthz = %+v", got)
	}
}

func TestCreateUser(t *testing.T) {
	h := newAPI(t).Handler()
	req := validUser("rosa.lindqvist@example.com")
	res := call(t, h, http.MethodPost, "/api/users", req)
	wantStatus(t, res, http.StatusCreated)
	got := decode[api.User](t, res)

	wantID := api.UserID(req.Email)
	if got.ID != wantID {
		t.Errorf("id = %q, want %q", got.ID, wantID)
	}
	if loc := res.header.Get("Location"); loc != "/api/users/"+wantID {
		t.Errorf("Location = %q", loc)
	}
	echo := api.CreateUserRequest{
		FullName: got.FullName, Email: got.Email, Phone: got.Phone, SSN: got.SSN,
		DOB: got.DOB, Address: got.Address, Password: req.Password,
	}
	if !reflect.DeepEqual(echo, req) {
		t.Errorf("personal data not echoed:\n got %+v\nwant %+v", echo, req)
	}
	if got.Status != "active" || got.Links.Self != "/api/users/"+wantID {
		t.Errorf("status/links = %q/%q", got.Status, got.Links.Self)
	}
	if strings.Contains(string(res.body), "password") || strings.Contains(string(res.body), req.Password) {
		t.Errorf("response leaks the password: %s", res.body)
	}

	// The created user is readable and the email is unique, ignoring case and
	// surrounding space.
	wantStatus(t, call(t, h, http.MethodGet, "/api/users/"+wantID, nil), http.StatusOK)
	for _, email := range []string{req.Email, "ROSA.Lindqvist@Example.com", "  rosa.lindqvist@example.com "} {
		dup := validUser(email)
		if msg := wantError(t, call(t, h, http.MethodPost, "/api/users", dup), http.StatusConflict); msg != "email already registered" {
			t.Errorf("409 message = %q", msg)
		}
	}
}

func TestCreateUserMinimal(t *testing.T) {
	h := newAPI(t).Handler()
	got := createUser(t, h, api.CreateUserRequest{FullName: "Tara Novak", Email: "tara.novak@example.org"})
	if got.Phone != "" || got.Address != nil {
		t.Errorf("optional fields set: %+v", got)
	}
	if body := call(t, h, http.MethodGet, "/api/users/"+got.ID, nil).body; strings.Contains(string(body), `"address"`) {
		t.Errorf("absent address encoded: %s", body)
	}
}

func TestCreateUserValidation(t *testing.T) {
	mutate := func(f func(*api.CreateUserRequest)) api.CreateUserRequest {
		r := validUser("valid@example.com")
		f(&r)
		return r
	}
	tests := []struct {
		name  string
		req   api.CreateUserRequest
		field string
	}{
		{"missing full_name", mutate(func(r *api.CreateUserRequest) { r.FullName = "  " }), "full_name"},
		{"full_name too long", mutate(func(r *api.CreateUserRequest) { r.FullName = strings.Repeat("a", 101) }), "full_name"},
		{"full_name control char", mutate(func(r *api.CreateUserRequest) { r.FullName = "Rosa\x00Lindqvist" }), "full_name"},
		{"missing email", mutate(func(r *api.CreateUserRequest) { r.Email = "" }), "email"},
		{"email without at", mutate(func(r *api.CreateUserRequest) { r.Email = "not-an-email" }), "email"},
		{"email without dot", mutate(func(r *api.CreateUserRequest) { r.Email = "rosa@localhost" }), "email"},
		{"email with display name", mutate(func(r *api.CreateUserRequest) { r.Email = "Rosa <rosa@example.com>" }), "email"},
		{"phone too short", mutate(func(r *api.CreateUserRequest) { r.Phone = "555-0142" }), "phone"},
		{"phone with letters", mutate(func(r *api.CreateUserRequest) { r.Phone = "206-555-CALL" }), "phone"},
		{"ssn without dashes", mutate(func(r *api.CreateUserRequest) { r.SSN = "987654325" }), "ssn"},
		{"ssn misplaced dash", mutate(func(r *api.CreateUserRequest) { r.SSN = "98-765-4325" }), "ssn"},
		{"dob in the future", mutate(func(r *api.CreateUserRequest) { r.DOB = "2030-01-01" }), "dob"},
		{"dob wrong layout", mutate(func(r *api.CreateUserRequest) { r.DOB = "30/06/1991" }), "dob"},
		{"dob impossible date", mutate(func(r *api.CreateUserRequest) { r.DOB = "1991-02-30" }), "dob"},
		{"address without street", mutate(func(r *api.CreateUserRequest) { r.Address.Street = "" }), "address.street"},
		{"address without city", mutate(func(r *api.CreateUserRequest) { r.Address.City = "" }), "address.city"},
		{"address bad zip", mutate(func(r *api.CreateUserRequest) { r.Address.Zip = "6270" }), "address.zip"},
		{"short password", mutate(func(r *api.CreateUserRequest) { r.Password = "short" }), "password"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newAPI(t).Handler()
			msg := wantError(t, call(t, h, http.MethodPost, "/api/users", tc.req), http.StatusUnprocessableEntity)
			if !strings.HasPrefix(msg, tc.field+": ") {
				t.Errorf("error %q does not name field %q", msg, tc.field)
			}
			// Error bodies are captured and logged, so they must not repeat
			// submitted personal data.
			for _, v := range []string{tc.req.Email, tc.req.Phone, tc.req.SSN, tc.req.DOB, tc.req.Password} {
				if len(v) >= 6 && strings.Contains(msg, v) {
					t.Errorf("error %q repeats submitted value %q", msg, v)
				}
			}
			wantStatus(t, call(t, h, http.MethodGet, "/api/users/"+api.UserID(tc.req.Email), nil), http.StatusNotFound)
		})
	}
}

func TestCreateUserMultipleProblems(t *testing.T) {
	msg := wantError(t, call(t, newAPI(t).Handler(), http.MethodPost, "/api/users", api.CreateUserRequest{}), http.StatusUnprocessableEntity)
	if !strings.Contains(msg, "full_name: ") || !strings.Contains(msg, "email: ") {
		t.Errorf("error %q should list every problem", msg)
	}
}

func TestMalformedBodies(t *testing.T) {
	tests := []struct {
		name   string
		target string
		body   string
		want   int
	}{
		{"truncated JSON", "/api/users", `{"full_name": "Rosa`, http.StatusBadRequest},
		{"not an object", "/api/users", `["rosa"]`, http.StatusBadRequest},
		{"unknown field", "/api/users", `{"full_name":"Rosa Lindqvist","email":"rosa@example.com","nickname":"ro"}`, http.StatusBadRequest},
		{"trailing data", "/api/users", `{"full_name":"Rosa Lindqvist","email":"rosa@example.com"} {}`, http.StatusBadRequest},
		{"wrong type", "/api/orders", `{"user_id":"u_0123456789ab","items":[{"sku":"BOOK-1001","qty":"two","price_cents":1}]}`, http.StatusBadRequest},
		{"empty body", "/api/login", ``, http.StatusBadRequest},
		{"too large", "/api/users", `{"full_name":"` + strings.Repeat("a", 70<<10) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			wantError(t, call(t, newAPI(t).Handler(), http.MethodPost, tc.target, tc.body), tc.want)
		})
	}
	// Trailing whitespace is not trailing data.
	res := call(t, newAPI(t).Handler(), http.MethodPost, "/api/users", `{"full_name":"Rosa Lindqvist","email":"rosa@example.com"}`+"\n\t ")
	wantStatus(t, res, http.StatusCreated)
}

func TestGetUser(t *testing.T) {
	h := newAPI(t).Handler()
	seed := api.SeedUsers()[1]
	res := call(t, h, http.MethodGet, "/api/users/"+seed.ID, nil)
	wantStatus(t, res, http.StatusOK)
	if got := decode[api.User](t, res); !reflect.DeepEqual(got, seed) {
		t.Errorf("got %+v, want %+v", got, seed)
	}
	if msg := wantError(t, call(t, h, http.MethodGet, "/api/users/u_000000000000", nil), http.StatusNotFound); msg != "user not found" {
		t.Errorf("404 message = %q", msg)
	}
}

func TestSeedUsers(t *testing.T) {
	seeds := api.SeedUsers()
	if len(seeds) != 3 {
		t.Fatalf("SeedUsers() returned %d users, want 3", len(seeds))
	}
	seeds[0].FullName = "mutated"
	if api.SeedUsers()[0].FullName == "mutated" {
		t.Error("SeedUsers shares state between calls")
	}
	for _, u := range api.SeedUsers() {
		if u.ID != api.UserID(u.Email) {
			t.Errorf("seed %s: id does not match email", u.ID)
		}
		if !strings.HasSuffix(u.Email, "@example.com") && !strings.HasSuffix(u.Email, "@example.org") {
			t.Errorf("seed %s: email outside reserved domains", u.ID)
		}
		if !strings.Contains(u.Phone, "555-01") && !strings.Contains(u.Phone, "555 01") {
			t.Errorf("seed %s: phone outside 555-01XX", u.ID)
		}
		if !strings.HasPrefix(u.SSN, "987-65-432") {
			t.Errorf("seed %s: SSN outside the advertising range", u.ID)
		}
	}
}

func TestListUsers(t *testing.T) {
	h := newAPI(t).Handler()
	seeds := api.SeedUsers()
	slices.SortFunc(seeds, func(a, b api.User) int { return strings.Compare(a.ID, b.ID) })

	all := call(t, h, http.MethodGet, "/api/users", nil)
	wantStatus(t, all, http.StatusOK)
	if got := decode[api.UserList](t, all); !reflect.DeepEqual(got, api.UserList{Users: seeds, Count: 3, Total: 3}) {
		t.Errorf("list = %+v", got)
	}

	tests := []struct {
		name      string
		query     string
		wantIDs   []string
		wantTotal int
	}{
		{"search exact", "?email=" + seeds[0].Email, []string{seeds[0].ID}, 1},
		{"search ignores case", "?email=" + strings.ToUpper(seeds[1].Email), []string{seeds[1].ID}, 1},
		{"search escaped at", "?email=" + strings.Replace(seeds[2].Email, "@", "%40", 1), []string{seeds[2].ID}, 1},
		{"search miss", "?email=nobody@example.com", []string{}, 0},
		{"empty email lists all", "?email=", []string{seeds[0].ID, seeds[1].ID, seeds[2].ID}, 3},
		{"limit", "?limit=2", []string{seeds[0].ID, seeds[1].ID}, 3},
		{"limit above total", "?limit=1000", []string{seeds[0].ID, seeds[1].ID, seeds[2].ID}, 3},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res := call(t, h, http.MethodGet, "/api/users"+tc.query, nil)
			wantStatus(t, res, http.StatusOK)
			got := decode[api.UserList](t, res)
			ids := []string{}
			for _, u := range got.Users {
				ids = append(ids, u.ID)
			}
			if !slices.Equal(ids, tc.wantIDs) || got.Count != len(tc.wantIDs) || got.Total != tc.wantTotal {
				t.Errorf("got ids %v count %d total %d, want %v total %d", ids, got.Count, got.Total, tc.wantIDs, tc.wantTotal)
			}
			if len(tc.wantIDs) == 0 && !strings.Contains(string(res.body), `"users":[]`) {
				t.Errorf("empty result should encode an empty array: %s", res.body)
			}
		})
	}
	for _, q := range []string{"?limit=0", "?limit=-1", "?limit=1001", "?limit=ten"} {
		wantError(t, call(t, h, http.MethodGet, "/api/users"+q, nil), http.StatusBadRequest)
	}
}

func TestLookupUser(t *testing.T) {
	h := newAPI(t).Handler()
	seed := api.SeedUsers()[0]
	for _, path := range []string{
		seed.Email,
		strings.ToUpper(seed.Email),
		strings.Replace(seed.Email, "@", "%40", 1),
	} {
		res := call(t, h, http.MethodGet, "/api/users/lookup/"+path, nil)
		wantStatus(t, res, http.StatusOK)
		if got := decode[api.User](t, res); got.ID != seed.ID {
			t.Errorf("lookup %q = %s, want %s", path, got.ID, seed.ID)
		}
	}
	if msg := wantError(t, call(t, h, http.MethodGet, "/api/users/lookup/nobody@example.com", nil), http.StatusNotFound); msg != "user not found" {
		t.Errorf("404 message = %q", msg)
	}
}
