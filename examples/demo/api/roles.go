package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// Role is the part of the application one process serves. One binary runs
// any of them, so a request can cross three services and leave a trace to
// follow, or one process can serve everything.
type Role string

const (
	// RoleAll serves every route from one process: no request leaves it,
	// and every response is byte for byte what the split services return.
	RoleAll Role = "all"
	// RoleGateway serves the public API and forwards each request to the
	// users or the orders service.
	RoleGateway Role = "gateway"
	// RoleUsers owns the user store and login.
	RoleUsers Role = "users"
	// RoleOrders owns the order store and asks the users service whether an
	// order's user exists.
	RoleOrders Role = "orders"
)

// ParseRole returns the role named by s; "" means RoleAll.
func ParseRole(s string) (Role, error) {
	switch r := Role(s); r {
	case "", RoleAll:
		return RoleAll, nil
	case RoleGateway, RoleUsers, RoleOrders:
		return r, nil
	}
	return "", fmt.Errorf("unknown role %q (want all, gateway, users or orders)", s)
}

// serverSpanName is the name of every HTTP server span before routing; the
// route handler renames the span to its pattern, which carries no data.
const serverSpanName = "demo-api"

// propagator carries the trace context between the services as W3C
// traceparent and baggage headers, whatever the global propagator is.
var propagator = propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})

// routes registers the role's routes on mux. Each handler is wrapped so the
// server span is named by the route pattern and tagged with it.
func (a *API) routes(mux *http.ServeMux) error {
	handle := func(pattern string, h http.HandlerFunc) {
		mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
			span := trace.SpanFromContext(r.Context())
			span.SetName(pattern)
			span.SetAttributes(semconv.HTTPRoute(pattern))
			h(w, r)
		})
	}
	handle("GET /healthz", a.healthz)
	switch a.role {
	case RoleAll, RoleUsers:
		handle("POST /api/users", a.createUser)
		handle("GET /api/users", a.listUsers)
		handle("GET /api/users/{id}", a.getUser)
		handle("GET /api/users/lookup/{email}", a.lookupUser)
		handle("POST /api/login", a.login)
	}
	switch a.role {
	case RoleAll, RoleOrders:
		handle("POST /api/orders", a.createOrder)
		handle("GET /api/orders/{id}", a.getOrder)
	}
	if a.role == RoleGateway {
		users, err := a.proxy(a.usersURL)
		if err != nil {
			return err
		}
		orders, err := a.proxy(a.ordersURL)
		if err != nil {
			return err
		}
		for _, p := range []string{"POST /api/users", "GET /api/users", "GET /api/users/{id}", "GET /api/users/lookup/{email}", "POST /api/login"} {
			handle(p, users)
		}
		for _, p := range []string{"POST /api/orders", "GET /api/orders/{id}"} {
			handle(p, orders)
		}
	}
	return nil
}

// instrument wraps the whole handler in an HTTP server span. otelhttp
// writes nothing to the response, so bodies stay byte for byte the same.
func (a *API) instrument(h http.Handler) http.Handler {
	return otelhttp.NewHandler(h, serverSpanName, otelhttp.WithTracerProvider(a.tracerProvider), otelhttp.WithPropagators(propagator))
}

// client returns the HTTP client the gateway and the orders service use to
// reach the other services: no transparent compression, so what a backend
// sends is what the client gets, and the trace context on every request.
func (a *API) client() *http.Client {
	if a.httpClient != nil {
		return a.httpClient
	}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.DisableCompression = true
	a.httpClient = &http.Client{Transport: otelhttp.NewTransport(t, otelhttp.WithTracerProvider(a.tracerProvider), otelhttp.WithPropagators(propagator))}
	return a.httpClient
}

// proxy forwards requests to base unchanged, apart from the hop headers and
// the trace context the transport adds.
func (a *API) proxy(base string) (http.HandlerFunc, error) {
	target, err := url.Parse(base)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return nil, fmt.Errorf("api: service URL %q: want scheme://host[:port]", base)
	}
	rp := &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(target)
			pr.Out.Host = target.Host
		},
		Transport: a.client().Transport,
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			a.log.ErrorContext(r.Context(), "upstream request failed", "route", r.Pattern, "error", err)
			a.writeError(w, r, http.StatusBadGateway, "upstream unavailable")
		},
	}
	return rp.ServeHTTP, nil
}

// userEmail returns the email of the user with id, or false when there is
// no such user: from the store when this process owns users, otherwise from
// the users service, under the request's trace.
func (a *API) userEmail(ctx context.Context, id string) (string, bool, error) {
	if a.role != RoleOrders {
		u, ok := a.store.user(id)
		return u.Email, ok, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSuffix(a.usersURL, "/")+"/api/users/"+url.PathEscape(id), nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := a.client().Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		var u struct {
			Email string `json:"email"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&u); err != nil {
			return "", false, fmt.Errorf("users service: %w", err)
		}
		return u.Email, true, nil
	case http.StatusNotFound:
		return "", false, nil
	}
	return "", false, fmt.Errorf("users service answered %d", resp.StatusCode)
}
