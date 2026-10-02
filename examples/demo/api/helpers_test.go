package api_test

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

var fixedNow = time.Date(2026, 10, 3, 12, 0, 0, 0, time.UTC)

func newAPI(t *testing.T) *api.API {
	t.Helper()
	a, err := api.New(api.Options{Now: func() time.Time { return fixedNow }})
	if err != nil {
		t.Fatalf("api.New: %v", err)
	}
	return a
}

// result is a response with its body decompressed.
type result struct {
	status int
	header http.Header
	body   []byte
}

// call serves one request. body may be nil, a string (sent verbatim) or a
// value encoded as JSON. headers are name/value pairs.
func call(t *testing.T, h http.Handler, method, target string, body any, headers ...string) result {
	t.Helper()
	var r io.Reader
	switch b := body.(type) {
	case nil:
	case string:
		r = strings.NewReader(b)
	default:
		data, err := json.Marshal(b)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
		r = bytes.NewReader(data)
	}
	req := httptest.NewRequest(method, target, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Add(headers[i], headers[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	res := result{status: rec.Code, header: rec.Header(), body: rec.Body.Bytes()}
	if res.header.Get("Content-Encoding") == "gzip" {
		res.body = gunzip(t, res.body)
	}
	return res
}

// httpRequest builds a bodiless request; headers are name/value pairs.
func httpRequest(t *testing.T, method, target string, headers ...string) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, target, nil)
	for i := 0; i+1 < len(headers); i += 2 {
		req.Header.Add(headers[i], headers[i+1])
	}
	return req
}

// newJSONRequest builds a request with a JSON body. It reports errors instead
// of failing the test so that worker goroutines can use it.
func newJSONRequest(method, target string, body any) (*http.Request, error) {
	data, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req := httptest.NewRequest(method, target, bytes.NewReader(data))
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

func serve(h http.Handler, req *http.Request) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func gunzip(t *testing.T, b []byte) []byte {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	out, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip: %v", err)
	}
	return out
}

// decode unmarshals a JSON response body, failing on unknown fields so tests
// notice when the wire format grows.
func decode[T any](t *testing.T, res result) T {
	t.Helper()
	if ct := res.header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var v T
	dec := json.NewDecoder(bytes.NewReader(res.body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&v); err != nil {
		t.Fatalf("decode %T from %s: %v", v, res.body, err)
	}
	return v
}

func wantStatus(t *testing.T, res result, want int) {
	t.Helper()
	if res.status != want {
		t.Fatalf("status = %d, want %d; body: %s", res.status, want, res.body)
	}
}

// wantError checks an error response and returns its message.
func wantError(t *testing.T, res result, status int) string {
	t.Helper()
	wantStatus(t, res, status)
	e := decode[api.ErrorResponse](t, res)
	if e.Error == "" {
		t.Fatalf("error response without message: %s", res.body)
	}
	return e.Error
}

func validUser(email string) api.CreateUserRequest {
	return api.CreateUserRequest{
		FullName: "Rosa Lindqvist",
		Email:    email,
		Phone:    "(206) 555-0142",
		SSN:      "987-65-4325",
		DOB:      "1991-06-30",
		Address:  &api.Address{Street: "77 Juniper Drive", City: "Springfield", Zip: "62704"},
		Password: "Lindqvist-Pass-91",
	}
}

func createUser(t *testing.T, h http.Handler, req api.CreateUserRequest) api.User {
	t.Helper()
	res := call(t, h, http.MethodPost, "/api/users", req)
	wantStatus(t, res, http.StatusCreated)
	return decode[api.User](t, res)
}

func validOrder(userID string) api.CreateOrderRequest {
	return api.CreateOrderRequest{
		UserID: userID,
		Items: []api.Item{
			{SKU: "BOOK-1001", Qty: 2, PriceCents: 1299},
			{SKU: "MUG-2002", Qty: 1, PriceCents: 899},
		},
		Payment: api.PaymentInput{CardNumber: "4111 1111 1111 1111", CVV: "123", Exp: "12/29"},
	}
}
