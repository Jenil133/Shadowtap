package traffic_test

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
	"github.com/Jenil133/Shadowtap/examples/demo/traffic"
)

const testBase = "http://demo.test"

func next(t *testing.T, g *traffic.Generator, base string) *http.Request {
	t.Helper()
	req, err := g.Next(base)
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	return req
}

// wire renders everything a request puts on the wire: method, URL, sorted
// headers and body.
func wire(t *testing.T, req *http.Request) string {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "%s %s\n", req.Method, req.URL)
	for _, name := range slices.Sorted(maps.Keys(req.Header)) {
		for _, v := range req.Header[name] {
			fmt.Fprintf(&b, "%s: %s\n", name, v)
		}
	}
	if req.GetBody != nil {
		body, err := req.GetBody()
		if err != nil {
			t.Fatalf("GetBody: %v", err)
		}
		data, err := io.ReadAll(body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		b.Write(data)
	}
	return b.String()
}

func TestGeneratorDeterministic(t *testing.T) {
	run := func(seed int64) []string {
		g := traffic.NewGenerator(seed, traffic.People(seed, 100), nil)
		out := make([]string, 500)
		for i := range out {
			req := next(t, g, testBase)
			exp, _ := traffic.ExpectationOf(req)
			out[i] = fmt.Sprintf("%s %d\n%s", exp.Op, exp.Status, wire(t, req))
		}
		return out
	}
	a, b := run(42), run(42)
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("request %d differs between runs with the same seed:\n%s\n---\n%s", i, a[i], b[i])
		}
	}
	if slices.Equal(a, run(43)) {
		t.Error("different seeds produced the same requests")
	}
}

func TestNextErrors(t *testing.T) {
	g := traffic.NewGenerator(1, traffic.People(1, 5), nil)
	for _, base := range []string{"", "demo.test:8080", "ftp://demo.test", "http://", "http://demo.test?x=1", "http://demo.test/#top", "http://[::1"} {
		if _, err := g.Next(base); err == nil {
			t.Errorf("Next(%q) succeeded, want error", base)
		}
	}
	if _, err := traffic.NewGenerator(1, nil, nil).Next(testBase); err == nil {
		t.Error("Next without people succeeded")
	}

	req := next(t, g, "https://demo.test:8443/prefix/")
	if u := req.URL; u.Scheme != "https" || u.Host != "demo.test:8443" || !strings.HasPrefix(u.Path, "/prefix/api/") {
		t.Errorf("URL %s does not keep the base URL's scheme, host and prefix", u)
	}
	if _, ok := traffic.ExpectationOf(httptest.NewRequest(http.MethodGet, "/", nil)); ok {
		t.Error("ExpectationOf found an expectation on a foreign request")
	}
}

func TestGeneratorHeaders(t *testing.T) {
	g := traffic.NewGenerator(5, traffic.People(5, 200), nil)
	docRanges := []netip.Prefix{
		netip.MustParsePrefix("203.0.113.0/24"),
		netip.MustParsePrefix("198.51.100.0/24"),
		netip.MustParsePrefix("2001:db8::/32"),
	}
	const n = 3000
	counts := map[string]int{}
	for range n {
		req := next(t, g, testBase)
		h := req.Header
		if h.Get("Accept") != "application/json" || h.Get("User-Agent") == "" {
			t.Fatalf("missing Accept or User-Agent: %v", h)
		}
		ips := strings.Split(h.Get("X-Forwarded-For"), ", ")
		if len(ips) < 1 || len(ips) > 2 {
			t.Fatalf("X-Forwarded-For %q should hold 1 or 2 addresses", h.Get("X-Forwarded-For"))
		}
		for _, s := range ips {
			ip, err := netip.ParseAddr(s)
			if err != nil || !slices.ContainsFunc(docRanges, func(p netip.Prefix) bool { return p.Contains(ip) }) {
				t.Fatalf("X-Forwarded-For entry %q is not a documentation address", s)
			}
			if ip.Is6() {
				counts["ipv6"]++
			}
		}
		if len(ips) == 2 {
			counts["proxy"]++
		}
		if tp := h.Get("Traceparent"); tp != "" {
			counts["traceparent"]++
			if len(tp) != 55 || !strings.HasPrefix(tp, "00-") || !strings.HasSuffix(tp, "-01") {
				t.Fatalf("malformed traceparent %q", tp)
			}
		}
		if auth := h.Get("Authorization"); auth != "" {
			counts["bearer"]++
			parts := strings.Split(strings.TrimPrefix(auth, "Bearer "), ".")
			if !strings.HasPrefix(auth, "Bearer eyJ") || len(parts) != 3 || !strings.HasPrefix(parts[1], "eyJ") {
				t.Fatalf("Authorization %q is not a JWT-shaped bearer token", auth)
			}
		}
		if c := h.Get("Cookie"); c != "" {
			counts["cookie"]++
			if !strings.HasPrefix(c, "session=") {
				t.Fatalf("Cookie %q", c)
			}
		}
		if h.Get("X-User-Email") != "" {
			counts["x-user-email"]++
		}
		if strings.Contains(h.Get("Accept-Encoding"), "gzip") {
			counts["gzip"]++
		}
	}
	// Generous bounds around the configured rates; n is large enough that a
	// correct generator never strays outside them.
	for name, want := range map[string][2]float64{
		"traceparent":  {0.85, 0.95},
		"bearer":       {0.25, 0.35},
		"cookie":       {0.25, 0.35},
		"x-user-email": {0.15, 0.25},
		"ipv6":         {0.10, 0.20},
		"proxy":        {0.35, 0.45},
		"gzip":         {0.55, 0.80},
	} {
		if got := float64(counts[name]) / n; got < want[0] || got > want[1] {
			t.Errorf("%s on %.1f%% of requests, want %.0f%%-%.0f%%", name, 100*got, 100*want[0], 100*want[1])
		}
	}
}

// matcher finds which of a set of strings, each at least four bytes long,
// occur in a text, in one pass over the text.
type matcher map[string][]string

func newMatcher(values []string) matcher {
	m := matcher{}
	for _, v := range values {
		m[v[:4]] = append(m[v[:4]], v)
	}
	return m
}

func (m matcher) find(text string, found map[string]bool) {
	for i := 0; i+4 <= len(text); i++ {
		for _, v := range m[text[i:i+4]] {
			if strings.HasPrefix(text[i:], v) {
				found[v] = true
			}
		}
	}
}

// TestLedgerCompleteness checks that every planted personal-data value that
// appears anywhere in a generated request is in the ledger, as are the
// per-request values: client IPs, bearer tokens, session cookies, header and
// query emails and login passwords.
func TestLedgerCompleteness(t *testing.T) {
	people := traffic.People(3, 400)
	ledger := traffic.NewLedger()
	g := traffic.NewGenerator(3, people, ledger)

	kinds := map[string]string{} // planted value -> kind
	for _, p := range people {
		for kind, v := range map[string]string{
			"full_name": p.FullName, "email": p.Email, "phone": p.Phone, "ssn": p.SSN,
			"dob": p.DOB, "street": p.Street, "card": p.Card, "password": p.Password,
		} {
			kinds[v] = kind
		}
	}
	m := newMatcher(slices.Collect(maps.Keys(kinds)))
	found := map[string]bool{}
	missing := func(what, v string) {
		t.Helper()
		if !ledger.Contains(v) {
			t.Errorf("%s %q is on the wire but not in the ledger", what, v)
		}
	}
	for range 2000 {
		req := next(t, g, testBase)
		m.find(wire(t, req), found)

		for _, ip := range strings.Split(req.Header.Get("X-Forwarded-For"), ", ") {
			missing("client IP", ip)
		}
		if auth := req.Header.Get("Authorization"); auth != "" {
			missing("bearer token", strings.TrimPrefix(auth, "Bearer "))
		}
		if c := req.Header.Get("Cookie"); c != "" {
			missing("session cookie", strings.TrimPrefix(c, "session="))
		}
		if e := req.Header.Get("X-User-Email"); e != "" {
			missing("X-User-Email", e)
		}
		if e := req.URL.Query().Get("email"); e != "" {
			missing("query email", e)
		}
		if req.URL.Path == "/api/login" {
			var login api.LoginRequest
			body, err := req.GetBody()
			if err != nil {
				t.Fatal(err)
			}
			if err := json.NewDecoder(body).Decode(&login); err != nil {
				t.Fatal(err)
			}
			missing("login email", login.Email)
			missing("login password", login.Password)
		}
	}

	seenKinds := map[string]bool{}
	for v := range found {
		seenKinds[kinds[v]] = true
		if len(v) >= traffic.MinLedgerValueLen {
			missing(kinds[v], v)
		}
	}
	for _, kind := range []string{"full_name", "email", "phone", "ssn", "dob", "street", "card", "password"} {
		if !seenKinds[kind] {
			t.Errorf("no %s ever appeared on the wire; the check is vacuous", kind)
		}
	}
	for _, v := range ledger.Values() {
		if len(v) < traffic.MinLedgerValueLen {
			t.Errorf("ledger holds short value %q", v)
		}
	}
	for _, u := range api.SeedUsers() {
		if !ledger.Contains(u.Email) || !ledger.Contains(u.FullName) || !ledger.Contains(u.Address.Street) {
			t.Errorf("ledger lacks seeded user %s, whose data list responses carry", u.ID)
		}
	}
}

func TestGeneratorAgainstAPI(t *testing.T) {
	a, err := api.New(api.Options{Now: func() time.Time { return time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC) }})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	client := srv.Client()

	g := traffic.NewGenerator(11, traffic.People(11, 500), traffic.NewLedger())
	const n = 1500
	statuses := map[traffic.Op]map[int]int{}
	gzipped := 0
	for i := range n {
		req := next(t, g, srv.URL)
		exp, ok := traffic.ExpectationOf(req)
		if !ok {
			t.Fatal("request without expectation")
		}
		explicitGzip := strings.Contains(req.Header.Get("Accept-Encoding"), "gzip")
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("request %d (%s): %v", i, exp.Op, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("request %d (%s): read body: %v", i, exp.Op, err)
		}
		// Served one at a time against a fresh API, the model is exact.
		if resp.StatusCode != exp.Status {
			t.Errorf("request %d %s %s: status %d, want %d; body %.200s", i, req.Method, exp.Op, resp.StatusCode, exp.Status, body)
		}
		if statuses[exp.Op] == nil {
			statuses[exp.Op] = map[int]int{}
		}
		statuses[exp.Op][resp.StatusCode]++
		if exp.Op == traffic.OpListUsers && explicitGzip {
			if resp.Header.Get("Content-Encoding") != "gzip" {
				t.Errorf("list response not gzipped for Accept-Encoding %q", req.Header.Get("Accept-Encoding"))
			}
			gzipped++
		}
	}
	for _, op := range []traffic.Op{
		traffic.OpCreateUser, traffic.OpCreateOrder, traffic.OpGetUser, traffic.OpSearchUsers,
		traffic.OpLookupUser, traffic.OpGetOrder, traffic.OpLogin, traffic.OpListUsers,
	} {
		if len(statuses[op]) == 0 {
			t.Errorf("operation %s never generated", op)
		}
	}
	for op, want := range map[traffic.Op]int{
		traffic.OpCreateUser:  http.StatusConflict,
		traffic.OpCreateOrder: http.StatusUnprocessableEntity,
		traffic.OpLookupUser:  http.StatusNotFound,
		traffic.OpLogin:       http.StatusUnauthorized,
	} {
		if statuses[op][want] == 0 {
			t.Errorf("%s never produced the expected %d", op, want)
		}
	}
	if gzipped == 0 {
		t.Error("no gzip-compressed list response was exercised")
	}
	t.Logf("statuses by operation: %v", statuses)
}

// TestGeneratorConcurrent shares one generator between senders. Requests may
// overtake the ones they depend on, so the model is not exact, but at least
// 95% must succeed or fail the way the model expects, and none may be
// malformed or crash the API.
func TestGeneratorConcurrent(t *testing.T) {
	a, err := api.New(api.Options{})
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(a.Handler())
	defer srv.Close()
	client := srv.Client()
	g := traffic.NewGenerator(23, traffic.People(23, 500), traffic.NewLedger())

	const workers, perWorker = 8, 250
	var (
		mu       sync.Mutex
		statuses = map[int]int{}
		misses   = map[string]int{} // "op expected->got" for unexpected statuses
		good     int
		errs     []error
		wg       sync.WaitGroup
	)
	for range workers {
		wg.Go(func() {
			for range perWorker {
				req, err := g.Next(srv.URL)
				if err != nil {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				exp, _ := traffic.ExpectationOf(req)
				resp, err := client.Do(req)
				if err == nil {
					_, err = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
				}
				mu.Lock()
				if err != nil {
					errs = append(errs, err)
				} else {
					statuses[resp.StatusCode]++
					if resp.StatusCode/100 == 2 || resp.StatusCode == exp.Status {
						good++
					}
					if resp.StatusCode != exp.Status {
						misses[fmt.Sprintf("%s %d->%d", exp.Op, exp.Status, resp.StatusCode)]++
					}
				}
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	for _, err := range errs {
		t.Error(err)
	}
	for _, bad := range []int{http.StatusBadRequest, http.StatusMethodNotAllowed, http.StatusRequestEntityTooLarge, http.StatusInternalServerError} {
		if statuses[bad] > 0 {
			t.Errorf("%d responses with status %d", statuses[bad], bad)
		}
	}
	total := workers * perWorker
	if rate := float64(good) / float64(total); rate < 0.95 {
		t.Errorf("only %.1f%% of requests got 2xx or their expected status; statuses %v", 100*rate, statuses)
	}
	t.Logf("%d requests, %d good; statuses %v; differences from the model %v", total, good, statuses, misses)
}
