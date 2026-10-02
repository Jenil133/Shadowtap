package api_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

func TestGzipNegotiation(t *testing.T) {
	h := newAPI(t).Handler()
	plain := call(t, h, http.MethodGet, "/api/users", nil)
	if len(plain.body) < 1024 {
		t.Fatalf("seeded user list is %d bytes; it must be at least 1 KiB to exercise gzip", len(plain.body))
	}

	tests := []struct {
		acceptEncoding string
		wantGzip       bool
	}{
		{"", false},
		{"gzip", true},
		{"GZIP", true},
		{"x-gzip", true},
		{"gzip, deflate, br", true},
		{"br;q=1.0, gzip;q=0.8", true},
		{"*", true},
		{"identity", false},
		{"br", false},
		{"gzip;q=0", false},
		{"gzip;q=0.000", false},
		{"gzip;q=bogus", false},
		{"*;q=0", false},
		{"gzip;q=0, *", false}, // an explicit refusal beats the wildcard
		{"identity;q=1, *;q=0.1", true},
	}
	for _, tc := range tests {
		t.Run(tc.acceptEncoding, func(t *testing.T) {
			var headers []string
			if tc.acceptEncoding != "" {
				headers = []string{"Accept-Encoding", tc.acceptEncoding}
			}
			req := httpRequest(t, http.MethodGet, "/api/users", headers...)
			rec := serve(h, req)
			gotGzip := rec.Header().Get("Content-Encoding") == "gzip"
			if gotGzip != tc.wantGzip {
				t.Fatalf("Content-Encoding = %q, want gzip=%v", rec.Header().Get("Content-Encoding"), tc.wantGzip)
			}
			if vary := rec.Header().Get("Vary"); vary != "Accept-Encoding" {
				t.Errorf("Vary = %q, want Accept-Encoding", vary)
			}
			if cl := rec.Header().Get("Content-Length"); cl != strconv.Itoa(rec.Body.Len()) {
				t.Errorf("Content-Length = %s, body is %d bytes", cl, rec.Body.Len())
			}
			body := rec.Body.Bytes()
			if gotGzip {
				if len(body) >= len(plain.body) {
					t.Errorf("gzip body (%d bytes) not smaller than plain (%d)", len(body), len(plain.body))
				}
				body = gunzip(t, body)
			}
			if !bytes.Equal(body, plain.body) {
				t.Error("decoded body differs from the identity response")
			}
		})
	}

	// Multiple Accept-Encoding lines are combined.
	rec := serve(h, httpRequest(t, http.MethodGet, "/api/users", "Accept-Encoding", "br", "Accept-Encoding", "gzip"))
	if rec.Header().Get("Content-Encoding") != "gzip" {
		t.Error("gzip in a second Accept-Encoding line was ignored")
	}

	// Small bodies are never compressed and do not vary.
	small := call(t, h, http.MethodGet, "/api/users/"+api.SeedUsers()[0].ID, nil, "Accept-Encoding", "gzip")
	if small.header.Get("Content-Encoding") != "" || small.header.Get("Vary") != "" {
		t.Errorf("small response: Content-Encoding %q, Vary %q", small.header.Get("Content-Encoding"), small.header.Get("Vary"))
	}
}

func TestUnroutedRequests(t *testing.T) {
	h := newAPI(t).Handler()
	tests := []struct {
		method, target string
		want           int
		allow          string
	}{
		{http.MethodGet, "/nope", http.StatusNotFound, ""},
		{http.MethodGet, "/api/users/u_0123456789ab/orders", http.StatusNotFound, ""},
		{http.MethodDelete, "/api/users/u_0123456789ab", http.StatusMethodNotAllowed, "GET, HEAD"},
		{http.MethodPut, "/api/login", http.StatusMethodNotAllowed, "POST"},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.target, func(t *testing.T) {
			res := call(t, h, tc.method, tc.target, nil)
			wantError(t, res, tc.want)
			if got := res.header.Get("Allow"); got != tc.allow {
				t.Errorf("Allow = %q, want %q", got, tc.allow)
			}
		})
	}
	// Unclean paths still get the mux's redirect.
	res := call(t, h, http.MethodGet, "/api//users", nil)
	if res.status < 300 || res.status > 399 || res.header.Get("Location") != "/api/users" {
		t.Errorf("unclean path: status %d, Location %q; want a redirect to /api/users", res.status, res.header.Get("Location"))
	}
}

func TestIDs(t *testing.T) {
	idPattern := regexp.MustCompile(`^[uo]_[0-9a-f]{12}$`)
	items := []api.Item{{SKU: "BOOK-1001", Qty: 2, PriceCents: 1299}, {SKU: "MUG-2002", Qty: 1, PriceCents: 899}}
	reversed := []api.Item{items[1], items[0]}

	// Golden values: replays and the traffic generator rely on this scheme
	// staying stable.
	if got := api.UserID("alice@example.com"); got != "u_ff8d9819fc0e" {
		t.Errorf("UserID = %q, want u_ff8d9819fc0e", got)
	}
	if got := api.OrderID("u_ff8d9819fc0e", items, 1); got != "o_3328d829cb62" {
		t.Errorf("OrderID seq 1 = %q, want o_3328d829cb62", got)
	}
	if got := api.OrderID("u_ff8d9819fc0e", items, 2); got != "o_ae2cb7a0b590" {
		t.Errorf("OrderID seq 2 = %q, want o_ae2cb7a0b590", got)
	}

	for _, email := range []string{"Alice@Example.COM", "  alice@example.com\t"} {
		if api.UserID(email) != api.UserID("alice@example.com") {
			t.Errorf("UserID(%q) is not case- and space-insensitive", email)
		}
	}
	distinct := []string{
		api.UserID("alice@example.com"),
		api.UserID("alice@example.org"),
		api.OrderID("u_ff8d9819fc0e", items, 1),
		api.OrderID("u_ff8d9819fc0e", items, 2),
		api.OrderID("u_000000000000", items, 1),
		api.OrderID("u_ff8d9819fc0e", items[:1], 1),
	}
	seen := map[string]bool{}
	for _, id := range distinct {
		if !idPattern.MatchString(id) {
			t.Errorf("id %q does not match %s", id, idPattern)
		}
		if seen[id] {
			t.Errorf("id %q repeated for different content", id)
		}
		seen[id] = true
	}
	if api.OrderID("u_ff8d9819fc0e", reversed, 1) != api.OrderID("u_ff8d9819fc0e", items, 1) {
		t.Error("OrderID depends on line order")
	}
	if items[0].SKU != "BOOK-1001" {
		t.Error("OrderID reordered the caller's items")
	}
}

// TestDeterministicResponses replays the same requests against two fresh
// APIs and expects byte-identical responses: ids come from content, not from
// counters shared across users or from the clock.
func TestDeterministicResponses(t *testing.T) {
	type step struct{ method, target string }
	run := func() []string {
		a := newAPI(t)
		h := a.Handler()
		var out []string
		record := func(res result) { out = append(out, fmt.Sprintf("%d %s", res.status, res.body)) }
		for i := range 5 {
			u := validUser(fmt.Sprintf("user.%d@example.com", i))
			res := call(t, h, http.MethodPost, "/api/users", u)
			record(res)
			id := decode[api.User](t, res).ID
			for range 2 {
				res := call(t, h, http.MethodPost, "/api/orders", validOrder(id))
				record(res)
				record(call(t, h, http.MethodGet, "/api/orders/"+decode[api.Order](t, res).ID, nil))
			}
		}
		for _, s := range []step{
			{http.MethodGet, "/api/users"},
			{http.MethodGet, "/api/users?email=user.3@example.com"},
			{http.MethodGet, "/api/users/lookup/user.4@example.com"},
		} {
			record(call(t, h, s.method, s.target, nil))
		}
		return out
	}
	first, second := run(), run()
	if len(first) != len(second) {
		t.Fatalf("runs differ in length: %d vs %d", len(first), len(second))
	}
	for i := range first {
		if first[i] != second[i] {
			t.Errorf("response %d differs:\n%s\n%s", i, first[i], second[i])
		}
	}
}

// syncBuffer is a bytes.Buffer safe for concurrent log writes.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestRequestLogsOmitPersonalData(t *testing.T) {
	var logs syncBuffer
	a, err := api.New(api.Options{Logger: slog.New(slog.NewJSONHandler(&logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	h := a.Handler()
	u := validUser("rosa.lindqvist@example.com")
	traceparent := "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	call(t, h, http.MethodPost, "/api/users", u, "traceparent", traceparent)
	call(t, h, http.MethodGet, "/api/users/lookup/"+u.Email, nil)
	call(t, h, http.MethodGet, "/api/users?email="+u.Email, nil)
	call(t, h, http.MethodPost, "/api/login", api.LoginRequest{Email: u.Email, Password: "Wrong-Password-1"})
	call(t, h, http.MethodGet, "/nowhere/"+u.Email, nil)

	out := logs.String()
	for _, v := range []string{u.Email, u.FullName, u.Phone, u.SSN, u.DOB, u.Address.Street, u.Password, "Wrong-Password-1"} {
		if strings.Contains(out, v) {
			t.Errorf("logs contain personal data %q:\n%s", v, out)
		}
	}
	for _, want := range []string{
		`"route":"POST /api/users"`,
		`"route":"GET /api/users/lookup/{email}"`,
		`"route":"unrouted"`,
		`"status":401`,
		`"trace_id":"4bf92f3577b34da6a3ce929d0e0e4736"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("logs lack %s:\n%s", want, out)
		}
	}
}

func TestConcurrentUse(t *testing.T) {
	h := newAPI(t).Handler()
	const workers, perWorker = 8, 10
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				email := fmt.Sprintf("worker%d.user%d@example.com", w, i)
				create, err := newJSONRequest(http.MethodPost, "/api/users", validUser(email))
				if err != nil {
					errs <- err
					return
				}
				if rec := serve(h, create); rec.Code != http.StatusCreated {
					errs <- fmt.Errorf("create %s: %d", email, rec.Code)
					continue
				}
				// Every worker orders for the same seeded user, contending
				// on its sequence counter.
				order, err := newJSONRequest(http.MethodPost, "/api/orders", validOrder(api.SeedUsers()[0].ID))
				if err != nil {
					errs <- err
					return
				}
				if rec := serve(h, order); rec.Code != http.StatusCreated {
					errs <- fmt.Errorf("order: %d", rec.Code)
				}
				serve(h, httptest.NewRequest(http.MethodGet, "/api/users", nil))
				serve(h, httptest.NewRequest(http.MethodGet, "/api/users/lookup/"+email, nil))
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	list := decode[api.UserList](t, call(t, h, http.MethodGet, "/api/users", nil))
	if want := 3 + workers*perWorker; list.Total != want {
		t.Errorf("total users = %d, want %d", list.Total, want)
	}
	// The seeded user's orders took sequence numbers 1..n exactly once.
	seed := api.SeedUsers()[0].ID
	items := validOrder(seed).Items
	for seq := 1; seq <= workers*perWorker; seq++ {
		if res := call(t, h, http.MethodGet, "/api/orders/"+api.OrderID(seed, items, seq), nil); res.status != http.StatusOK {
			t.Fatalf("order with sequence %d missing: %d", seq, res.status)
		}
	}
}

func TestNowOption(t *testing.T) {
	// The clock bounds dates of birth, and seeding never depends on it.
	early := time.Date(1991, 1, 1, 0, 0, 0, 0, time.UTC)
	a, err := api.New(api.Options{Now: func() time.Time { return early }})
	if err != nil {
		t.Fatal(err)
	}
	u := validUser("rosa.lindqvist@example.com") // born 1991
	wantError(t, call(t, a.Handler(), http.MethodPost, "/api/users", u), http.StatusUnprocessableEntity)
}
