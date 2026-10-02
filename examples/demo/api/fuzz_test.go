package api

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
)

// FuzzHandler sends arbitrary requests to the API. It must never fail with a
// 5xx (a recovered panic shows up as a 500), and every response other than a
// redirect or a HEAD reply must be valid JSON, gzip-compressed only when
// announced.
func FuzzHandler(f *testing.F) {
	a, err := New(Options{Now: func() time.Time { return time.Date(2026, 10, 3, 0, 0, 0, 0, time.UTC) }})
	if err != nil {
		f.Fatal(err)
	}
	h := a.Handler()
	f.Add("POST", "/api/users", `{"full_name":"Rosa Lindqvist","email":"rosa@example.com","address":{"street":"1 Elm Way","city":"Salem","zip":"97301"}}`, "gzip")
	f.Add("POST", "/api/orders", `{"user_id":"u_ff8d9819fc0e","items":[{"sku":"BOOK-1001","qty":1,"price_cents":5}],"payment":{"card_number":"4111 1111 1111 1111","cvv":"123","exp":"12/29"}}`, "")
	f.Add("POST", "/api/login", `{"email":"maria.castillo@example.com","password":"garden-Lantern-42"}`, "br")
	f.Add("GET", "/api/users?email=maria.castillo%40example.com&limit=5", "", "gzip;q=0.5, *;q=0")
	f.Add("GET", "/api/users/lookup/maria.castillo@example.com", "", "*")
	f.Add("DELETE", "/api/orders/o_000000000000", "{}", "x-gzip;q=1")
	f.Add("HEAD", "/api/users", "", "gzip")

	f.Fuzz(func(t *testing.T, method, target, body, acceptEncoding string) {
		req, err := http.NewRequest(method, "http://demo.test"+target, strings.NewReader(body))
		if err != nil || !strings.HasPrefix(target, "/") {
			t.Skip()
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept-Encoding", acceptEncoding)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code >= 500 {
			t.Fatalf("%s %q: status %d: %s", method, target, rec.Code, rec.Body)
		}
		if rec.Code/100 == 3 || method == http.MethodHead {
			return
		}
		if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("%s %q: Content-Type %q", method, target, ct)
		}
		data := rec.Body.Bytes()
		if rec.Header().Get("Content-Encoding") == "gzip" {
			zr, err := gzip.NewReader(bytes.NewReader(data))
			if err != nil {
				t.Fatalf("bad gzip: %v", err)
			}
			if data, err = io.ReadAll(zr); err != nil {
				t.Fatalf("bad gzip: %v", err)
			}
		}
		if !json.Valid(data) {
			t.Fatalf("%s %q: invalid JSON body %q", method, target, data)
		}
	})
}
