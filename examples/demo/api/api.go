// Package api implements the Shadowtap demo application: an in-memory users,
// orders and login JSON API that is deliberately full of personal data, so
// that Shadowtap's capture and scrubbing have something to find.
//
// Personal data only ever appears where a scrubber is expected to catch it:
// under sensitive JSON field names (full_name, email, phone, ssn, dob,
// address, card_number, password, token, billing_email), in the path and
// query of lookups, and in detectable formats. Ids are derived from request
// content (see UserID and OrderID) so replayed traffic reproduces them.
//
// Routes:
//
//	GET  /healthz
//	POST /api/users                 create a user (201, 409, 422)
//	GET  /api/users?email=&limit=   list or search users (200)
//	GET  /api/users/{id}            get a user (200, 404)
//	GET  /api/users/lookup/{email}  find a user by email (200, 404)
//	POST /api/orders                create an order (201, 404, 422)
//	GET  /api/orders/{id}           get an order (200, 404)
//	POST /api/login                 issue a token and session cookie (200, 401, 422)
//
// Malformed JSON bodies get 400, oversized ones 413. Every response is JSON;
// errors are {"error": "..."}. Bodies of at least 1 KiB are gzip-compressed
// when the client accepts it.
package api

import (
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"

	"github.com/Jenil133/Shadowtap/internal/tracectx"
)

// API versions. VersionV2 is a "candidate build" with deliberate regressions
// for the replayer to catch:
//
//   - users are returned with "phone_number" instead of "phone";
//   - an invalid order is rejected with 400 instead of 422;
//   - GET /api/users lists users in descending id order;
//   - every /api/orders request takes 40 ms longer.
const (
	VersionV1 = "v1"
	VersionV2 = "v2"
)

// OrdersDelayV2 is the latency VersionV2 adds to /api/orders.
const OrdersDelayV2 = 40 * time.Millisecond

// Options configures an API. The zero value is valid.
type Options struct {
	// Version selects the API behaviour: VersionV1 (the default when empty)
	// or VersionV2.
	Version string
	// Now returns the current time. It dates tokens and bounds dates of
	// birth. Defaults to time.Now.
	Now func() time.Time
	// Logger receives one log line per request and internal errors. Request
	// logs name the route pattern, never the raw path, because paths carry
	// personal data. Defaults to discarding logs.
	Logger *slog.Logger
	// Role is the part of the application to serve; the default RoleAll
	// serves everything from one process.
	Role Role
	// UsersURL and OrdersURL are the other services' base URLs, needed by
	// RoleGateway (both) and RoleOrders (users).
	UsersURL  string
	OrdersURL string
	// TracerProvider produces the HTTP server and client spans; nil uses the
	// global provider.
	TracerProvider trace.TracerProvider
}

// API is the demo application. It is safe for concurrent use.
type API struct {
	version             string
	now                 func() time.Time
	log                 *slog.Logger
	store               *store
	signer              tokenSigner
	role                Role
	usersURL, ordersURL string
	tracerProvider      trace.TracerProvider
	httpClient          *http.Client
	// dummy is checked when a login names an unknown email so that the
	// response time does not reveal whether the account exists.
	dummy       passwordHash
	gzipWriters sync.Pool // *gzip.Writer
	handler     http.Handler
}

// New returns an API seeded with three users.
func New(opts Options) (*API, error) {
	role, err := ParseRole(string(opts.Role))
	if err != nil {
		return nil, fmt.Errorf("api: %w", err)
	}
	a := &API{
		version:        opts.Version,
		now:            opts.Now,
		log:            opts.Logger,
		store:          newStore(),
		signer:         newTokenSigner(),
		role:           role,
		usersURL:       opts.UsersURL,
		ordersURL:      opts.OrdersURL,
		tracerProvider: opts.TracerProvider,
	}
	if a.tracerProvider == nil {
		a.tracerProvider = otel.GetTracerProvider()
	}
	if (role == RoleGateway || role == RoleOrders) && a.usersURL == "" {
		return nil, fmt.Errorf("api: role %s needs the users service URL", role)
	}
	if role == RoleGateway && a.ordersURL == "" {
		return nil, fmt.Errorf("api: role %s needs the orders service URL", role)
	}
	if a.version == "" {
		a.version = VersionV1
	}
	if a.version != VersionV1 && a.version != VersionV2 {
		return nil, fmt.Errorf("api: unsupported version %q (supported: %s, %s)", opts.Version, VersionV1, VersionV2)
	}
	if a.now == nil {
		a.now = time.Now
	}
	if a.log == nil {
		a.log = slog.New(slog.DiscardHandler)
	}
	if a.dummy, err = hashPassword("not-a-real-password"); err != nil {
		return nil, fmt.Errorf("api: %w", err)
	}
	for _, req := range seedRequests() {
		if err := a.seed(req); err != nil {
			return nil, fmt.Errorf("api: seed user: %w", err)
		}
	}

	mux := http.NewServeMux()
	if err := a.routes(mux); err != nil {
		return nil, err
	}
	a.handler = a.instrument(a.middleware(mux))
	return a, nil
}

// seed stores a built-in user. Seeds are not validated here, so an injected
// clock cannot make New fail; a unit test keeps them valid.
func (a *API) seed(req CreateUserRequest) error {
	req.normalize()
	pw, err := hashPassword(req.Password)
	if err != nil {
		return err
	}
	return a.store.addUser(newUserRecord(&req, pw))
}

// Handler returns the HTTP handler serving every route.
func (a *API) Handler() http.Handler { return a.handler }

// Version returns the API version being served.
func (a *API) Version() string { return a.version }

// middleware logs each request, turns panics into 500 responses and answers
// unrouted requests with JSON errors instead of the mux's plain text.
func (a *API) middleware(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &responseRecorder{ResponseWriter: w}
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				a.log.ErrorContext(r.Context(), "handler panic", "route", r.Pattern, "panic", fmt.Sprint(v))
				if !rec.wroteHeader {
					a.writeError(rec, r, http.StatusInternalServerError, "internal server error")
				}
			}
			a.logRequest(r, rec, time.Since(start))
		}()

		// An empty pattern means the mux would reply 404 or 405 in plain text
		// (or redirect to a cleaned path, which passes through unchanged).
		if h, pattern := mux.Handler(r); pattern == "" {
			h.ServeHTTP(&unroutedWriter{ResponseWriter: rec, api: a, r: r}, r)
			return
		}
		mux.ServeHTTP(rec, r)
	})
}

func (a *API) logRequest(r *http.Request, rec *responseRecorder, elapsed time.Duration) {
	route := r.Pattern
	if route == "" {
		route = "unrouted"
	}
	status := rec.status
	if !rec.wroteHeader {
		status = http.StatusOK // what net/http sends for an empty response
	}
	level := slog.LevelInfo
	if status >= http.StatusInternalServerError {
		level = slog.LevelError
	}
	attrs := []slog.Attr{
		slog.String("method", r.Method),
		slog.String("route", route),
		slog.Int("status", status),
		slog.Int64("bytes", rec.bytes),
		slog.Duration("duration", elapsed),
	}
	if tc, ok := tracectx.FromHeaders(r.Header.Get); ok {
		attrs = append(attrs, slog.String("trace_id", tc.TraceID))
	}
	a.log.LogAttrs(r.Context(), level, "request", attrs...)
}

// responseRecorder records the status and size of a response for logging.
type responseRecorder struct {
	http.ResponseWriter
	status      int
	bytes       int64
	wroteHeader bool
}

func (w *responseRecorder) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseRecorder) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(b)
	w.bytes += int64(n)
	return n, err
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *responseRecorder) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// unroutedWriter replaces the mux's plain-text 404 and 405 replies with JSON
// errors, keeping headers such as Allow. Other replies pass through.
type unroutedWriter struct {
	http.ResponseWriter
	api      *API
	r        *http.Request
	replaced bool
}

func (w *unroutedWriter) WriteHeader(code int) {
	switch code {
	case http.StatusNotFound:
		w.replaced = true
		w.api.writeError(w.ResponseWriter, w.r, code, "not found")
	case http.StatusMethodNotAllowed:
		w.replaced = true
		w.api.writeError(w.ResponseWriter, w.r, code, "method not allowed")
	default:
		w.ResponseWriter.WriteHeader(code)
	}
}

func (w *unroutedWriter) Write(b []byte) (int, error) {
	if w.replaced {
		return len(b), nil // drop the mux's plain-text body
	}
	return w.ResponseWriter.Write(b)
}
