package api_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

// services is the application split into its three processes, each with its
// own in-memory span exporter so a test can tell which service produced a
// span.
type services struct {
	gateway, users, orders       *httptest.Server
	gatewayS, usersS, ordersS    *tracetest.InMemoryExporter
	gatewayTP, usersTP, ordersTP *sdktrace.TracerProvider
}

func newServices(t *testing.T, version string) *services {
	t.Helper()
	provider := func() (*tracetest.InMemoryExporter, *sdktrace.TracerProvider) {
		exp := tracetest.NewInMemoryExporter()
		tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
		t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
		return exp, tp
	}
	s := &services{}
	s.usersS, s.usersTP = provider()
	s.ordersS, s.ordersTP = provider()
	s.gatewayS, s.gatewayTP = provider()
	start := func(opts api.Options) *httptest.Server {
		a, err := api.New(opts)
		if err != nil {
			t.Fatal(err)
		}
		srv := httptest.NewServer(a.Handler())
		t.Cleanup(srv.Close)
		return srv
	}
	s.users = start(api.Options{Version: version, Role: api.RoleUsers, TracerProvider: s.usersTP})
	s.orders = start(api.Options{Version: version, Role: api.RoleOrders, UsersURL: s.users.URL, TracerProvider: s.ordersTP})
	s.gateway = start(api.Options{Version: version, Role: api.RoleGateway, UsersURL: s.users.URL, OrdersURL: s.orders.URL, TracerProvider: s.gatewayTP})
	return s
}

func do(t *testing.T, base, method, path, body string, header ...string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, base+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(header); i += 2 {
		req.Header.Set(header[i], header[i+1])
	}
	client := &http.Client{Transport: &http.Transport{DisableCompression: true}}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, b
}

const (
	newUser  = `{"full_name":"Dana Fox","email":"dana.fox@example.com","phone":"(415) 555-0144","ssn":"987-65-4321","dob":"1990-04-02","address":{"street":"12 Pine St","city":"Oakland","zip":"94607"},"password":"correct-horse-battery"}`
	newOrder = `{"user_id":"%s","items":[{"sku":"A-1","qty":2,"price_cents":1999}],"payment":{"card_number":"4111 1111 1111 1111","cvv":"737","exp":"12/29"}}`
)

// The split services answer exactly what one process answers.
func TestRolesMatchSingleProcess(t *testing.T) {
	split := newServices(t, api.VersionV1)
	whole, err := api.New(api.Options{})
	if err != nil {
		t.Fatal(err)
	}
	single := httptest.NewServer(whole.Handler())
	defer single.Close()

	steps := []struct {
		method, path, body string
		status             int
	}{
		{"POST", "/api/users", newUser, 201},
		{"POST", "/api/users", newUser, 409},
		{"GET", "/api/users/lookup/dana.fox@example.com", "", 200},
		{"GET", "/api/users?email=dana.fox@example.com", "", 200},
		{"GET", "/api/users/" + api.UserID("dana.fox@example.com"), "", 200},
		{"POST", "/api/orders", strings.ReplaceAll(newOrder, "%s", api.UserID("dana.fox@example.com")), 201},
		{"POST", "/api/orders", strings.ReplaceAll(newOrder, "%s", "u_000000000000"), 404},
		{"POST", "/api/orders", `{"user_id":"x","items":[]}`, 422},
		{"POST", "/api/login", `{"email":"dana.fox@example.com","password":"correct-horse-battery"}`, 200},
		{"GET", "/api/orders/nope", "", 404},
		{"GET", "/nowhere", "", 404},
		{"DELETE", "/api/users/u_1", "", 405},
	}
	var orderLocation string
	for _, st := range steps {
		gw, gwBody := do(t, split.gateway.URL, st.method, st.path, st.body)
		one, oneBody := do(t, single.URL, st.method, st.path, st.body)
		if gw.StatusCode != st.status || one.StatusCode != st.status {
			t.Errorf("%s %s: gateway %d, single %d, want %d", st.method, st.path, gw.StatusCode, one.StatusCode, st.status)
		}
		if st.path == "/api/login" {
			continue // tokens and session cookies are random by design
		}
		if !bytes.Equal(stripNoise(gwBody), stripNoise(oneBody)) {
			t.Errorf("%s %s: bodies differ\n gateway %s\n single  %s", st.method, st.path, gwBody, oneBody)
		}
		for _, h := range []string{"Content-Type", "Location", "Allow"} {
			if gw.Header.Get(h) != one.Header.Get(h) {
				t.Errorf("%s %s: header %s %q vs %q", st.method, st.path, h, gw.Header.Get(h), one.Header.Get(h))
			}
		}
		if st.method == "POST" && st.path == "/api/orders" && st.status == 201 {
			orderLocation = gw.Header.Get("Location")
		}
	}
	gw, gwBody := do(t, split.gateway.URL, "GET", orderLocation, "")
	one, oneBody := do(t, single.URL, "GET", orderLocation, "")
	if gw.StatusCode != 200 || one.StatusCode != 200 || !bytes.Equal(gwBody, oneBody) || !strings.Contains(string(gwBody), `"billing_email":"dana.fox@example.com"`) {
		t.Errorf("order fetch: %d %s vs %d %s", gw.StatusCode, gwBody, one.StatusCode, oneBody)
	}
}

// Compressed responses pass through the gateway untouched.
func TestGatewayKeepsEncoding(t *testing.T) {
	split := newServices(t, api.VersionV1)
	resp, body := do(t, split.gateway.URL, "GET", "/api/users", "", "Accept-Encoding", "gzip")
	if resp.Header.Get("Content-Encoding") != "gzip" || len(body) < 2 || body[0] != 0x1f || body[1] != 0x8b {
		t.Errorf("gateway did not pass the gzip body through: encoding %q, %d bytes", resp.Header.Get("Content-Encoding"), len(body))
	}
}

// The candidate's regressions show through the gateway too.
func TestRolesVersion2(t *testing.T) {
	split := newServices(t, api.VersionV2)
	resp, body := do(t, split.gateway.URL, "GET", "/api/users?limit=1", "")
	if resp.StatusCode != 200 || !strings.Contains(string(body), `"phone_number"`) {
		t.Errorf("v2 rename missing: %d %s", resp.StatusCode, body)
	}
	resp, _ = do(t, split.gateway.URL, "POST", "/api/orders", `{"user_id":"x","items":[]}`)
	if resp.StatusCode != 400 {
		t.Errorf("v2 status regression missing: %d", resp.StatusCode)
	}
}

// One request through the gateway leaves a span in every service, all with
// the same trace id and parent links across the hops.
func TestRolesShareOneTrace(t *testing.T) {
	split := newServices(t, api.VersionV1)
	id := api.UserID(api.SeedUsers()[0].Email)
	resp, _ := do(t, split.gateway.URL, "POST", "/api/orders", strings.ReplaceAll(newOrder, "%s", id),
		"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	if resp.StatusCode != 201 {
		t.Fatalf("order: %d", resp.StatusCode)
	}
	spans := map[string]tracetest.SpanStubs{"gateway": split.gatewayS.GetSpans(), "orders": split.ordersS.GetSpans(), "users": split.usersS.GetSpans()}
	byID := map[trace.SpanID]tracetest.SpanStub{}
	for svc, ss := range spans {
		if len(ss) == 0 {
			t.Fatalf("no spans in %s", svc)
		}
		for _, s := range ss {
			if s.SpanContext.TraceID().String() != "4bf92f3577b34da6a3ce929d0e0e4736" {
				t.Errorf("%s span %s has trace %s", svc, s.Name, s.SpanContext.TraceID())
			}
			byID[s.SpanContext.SpanID()] = s
		}
	}
	server := func(svc, name string) tracetest.SpanStub {
		for _, s := range spans[svc] {
			if s.SpanKind == trace.SpanKindServer && s.Name == name {
				return s
			}
		}
		t.Fatalf("%s has no server span %q; got %v", svc, name, names(spans[svc]))
		return tracetest.SpanStub{}
	}
	gw := server("gateway", "POST /api/orders")
	if gw.Parent.SpanID().String() != "00f067aa0ba902b7" {
		t.Errorf("gateway parent %s, want the caller's span", gw.Parent.SpanID())
	}
	orders := server("orders", "POST /api/orders")
	if p, ok := byID[orders.Parent.SpanID()]; !ok || p.SpanKind != trace.SpanKindClient || !strings.HasPrefix(p.Name, "") {
		t.Errorf("orders server span parent %s is not the gateway's client span", orders.Parent.SpanID())
	}
	users := server("users", "GET /api/users/{id}")
	if p, ok := byID[users.Parent.SpanID()]; !ok || p.SpanKind != trace.SpanKindClient {
		t.Errorf("users server span parent %s is not the orders service's client span", users.Parent.SpanID())
	}
	for _, s := range spans["users"] {
		for _, a := range s.Attributes {
			if strings.Contains(a.Value.AsString(), "@") || strings.Contains(a.Value.Emit(), "4111") {
				t.Errorf("span attribute %s carries request data", a.Key)
			}
		}
	}
}

func TestRoleOptions(t *testing.T) {
	for _, opts := range []api.Options{
		{Role: "edge"},
		{Role: api.RoleGateway},
		{Role: api.RoleGateway, UsersURL: "http://u"},
		{Role: api.RoleOrders},
		{Role: api.RoleGateway, UsersURL: "u", OrdersURL: "http://o"},
	} {
		if _, err := api.New(opts); err == nil {
			t.Errorf("New(%+v) accepted", opts)
		}
	}
	if _, err := api.ParseRole(""); err != nil {
		t.Error(err)
	}
}

// A users service that is down becomes a 502 from the gateway, not a hang.
func TestGatewayUpstreamDown(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	a, err := api.New(api.Options{Role: api.RoleGateway, UsersURL: dead.URL, OrdersURL: dead.URL})
	if err != nil {
		t.Fatal(err)
	}
	gw := httptest.NewServer(a.Handler())
	defer gw.Close()
	resp, body := do(t, gw.URL, "GET", "/api/users", "")
	if resp.StatusCode != http.StatusBadGateway || !strings.Contains(string(body), "upstream unavailable") {
		t.Errorf("got %d %s", resp.StatusCode, body)
	}
}

// generatedAt is the list endpoint's deliberate noise field.
var generatedAt = regexp.MustCompile(`"generated_at":"[^"]*"`)

func stripNoise(b []byte) []byte { return generatedAt.ReplaceAll(b, []byte(`"generated_at":""`)) }

func names(ss tracetest.SpanStubs) []string {
	var out []string
	for _, s := range ss {
		out = append(out, s.Name)
	}
	return out
}
