package capture

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func sampleExchange() *Exchange {
	start := time.Date(2026, 10, 3, 12, 0, 0, 123456789, time.UTC)
	x := &Exchange{
		SchemaVersion: SchemaVersion,
		ID:            NewID(),
		Service:       "demo-api",
		TraceID:       "4bf92f3577b34da6a3ce929d0e0e4736",
		ParentSpanID:  "00f067aa0ba902b7",
		RequestID:     "7d5c8a2e-0e3f-4c55-9a51-3f1f0f3b1a10",
		Source:        Source{Tap: "shadowtap-tap-0", Mode: ModeObserve},
		Sampling:      Sampling{Rate: 0.05, Rule: "default", KeySource: "traceparent"},
		Timing: Timing{
			StartedAt:         start,
			ResponseStartedAt: start.Add(12 * time.Millisecond),
			CompletedAt:       start.Add(13 * time.Millisecond),
			DurationMicros:    12000,
		},
		Request: Request{
			Method:    "POST",
			Scheme:    "http",
			Authority: "demo-api:8080",
			Path:      "/api/users?q=<b>&x=1",
			Headers:   Headers{"content-type": {"application/json"}},
		},
		Response: Response{
			Status:  201,
			Headers: Headers{"content-type": {"application/json"}, "set-cookie": {"a=tok_1", "b=tok_2"}},
		},
		Redactions: map[string]int{"email": 2},
	}
	x.Request.Body = Body{ContentType: "application/json", Size: 27}
	x.Request.Body.SetData([]byte(`{"email":"user-1@redacted.invalid"}`))
	x.Response.Body = Body{ContentType: "application/json", Size: 11}
	x.Response.Body.SetData([]byte(`{"id":"u1"}`))
	return x
}

func TestEncodeDecodeRoundTrip(t *testing.T) {
	x := sampleExchange()
	b, err := Encode(x)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.HasSuffix(b, []byte("\n")) {
		t.Error("encoding has trailing newline")
	}
	if !bytes.Contains(b, []byte(`q=<b>`)) {
		t.Errorf("HTML characters were escaped: %s", b)
	}
	got, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, x) {
		t.Errorf("round trip mismatch\n got %+v\nwant %+v", got, x)
	}
	if err := got.Validate(); err != nil {
		t.Errorf("Validate: %v", err)
	}
}

func TestZeroTimesOmitted(t *testing.T) {
	x := sampleExchange()
	x.Timing.ResponseStartedAt = time.Time{}
	x.Timing.CompletedAt = time.Time{}
	b, err := Encode(x)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"response_started_at", "completed_at"} {
		if bytes.Contains(b, []byte(field)) {
			t.Errorf("zero %s was encoded", field)
		}
	}
}

func TestDecodeRejectsUnknownSchema(t *testing.T) {
	_, err := Decode([]byte(`{"schema_version":2,"id":"x"}`))
	if !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("err = %v, want ErrUnsupportedSchema", err)
	}
	if _, err := Decode([]byte(`{`)); err == nil {
		t.Fatal("expected error for malformed JSON")
	}
}

func TestBodySetData(t *testing.T) {
	var b Body
	b.SetData([]byte("héllo"))
	if b.Encoding != EncodingUTF8 || b.Data != "héllo" {
		t.Errorf("utf8 body = %+v", b)
	}
	bin := []byte{0xff, 0x00, 0xfe}
	b.SetData(bin)
	if b.Encoding != EncodingBase64 {
		t.Errorf("binary body encoding = %q", b.Encoding)
	}
	got, err := b.Bytes()
	if err != nil || !bytes.Equal(got, bin) {
		t.Errorf("Bytes() = %v, %v", got, err)
	}
	b.SetData(nil)
	if b.Data != "" || b.Encoding != "" {
		t.Errorf("empty SetData left %+v", b)
	}
	b.Encoding = "rot13"
	if _, err := b.Bytes(); err == nil {
		t.Error("expected error for unknown encoding")
	}
}

func TestFitDropsBodiesInOrder(t *testing.T) {
	x := sampleExchange()
	x.Request.Body.SetData([]byte(strings.Repeat("a", 2000)))
	x.Response.Body.SetData([]byte(strings.Repeat("b", 2000)))
	full, _ := Encode(x)

	// Room for everything.
	if b, err := x.Fit(len(full)); err != nil || len(b) != len(full) {
		t.Fatalf("Fit(len) = %d, %v", len(b), err)
	}

	// Room for the request body only: the response body goes first.
	b, err := x.Fit(len(full) - 1500)
	if err != nil {
		t.Fatal(err)
	}
	if x.Response.Body.Omitted != OmittedTooLarge || x.Response.Body.Data != "" {
		t.Errorf("response body not omitted: %+v", x.Response.Body)
	}
	if x.Request.Body.Data == "" {
		t.Error("request body dropped too early")
	}
	if len(b) > len(full)-1500 {
		t.Errorf("Fit result %d bytes exceeds limit", len(b))
	}

	// Room for neither body.
	x = sampleExchange()
	x.Request.Body.SetData([]byte(strings.Repeat("a", 2000)))
	x.Response.Body.SetData([]byte(strings.Repeat("b", 2000)))
	if _, err := x.Fit(1500); err != nil {
		t.Fatal(err)
	}
	if x.Request.Body.Omitted != OmittedTooLarge || x.Response.Body.Omitted != OmittedTooLarge {
		t.Errorf("bodies not omitted: req=%q resp=%q", x.Request.Body.Omitted, x.Response.Body.Omitted)
	}

	// Impossible.
	if _, err := sampleExchange().Fit(10); !errors.Is(err, ErrTooLarge) {
		t.Errorf("Fit(10) err = %v, want ErrTooLarge", err)
	}
	// Disabled limit.
	if _, err := sampleExchange().Fit(0); err != nil {
		t.Errorf("Fit(0) err = %v", err)
	}
}

func TestKeyPreference(t *testing.T) {
	x := sampleExchange()
	if string(x.Key()) != x.TraceID {
		t.Errorf("key = %s, want trace id", x.Key())
	}
	x.TraceID = ""
	if string(x.Key()) != x.RequestID {
		t.Errorf("key = %s, want request id", x.Key())
	}
	x.RequestID = ""
	if string(x.Key()) != x.ID {
		t.Errorf("key = %s, want capture id", x.Key())
	}
}

func TestRecordHeaders(t *testing.T) {
	x := sampleExchange()
	got := map[string]string{}
	for _, h := range x.RecordHeaders() {
		got[h.Key] = string(h.Value)
	}
	want := map[string]string{
		HeaderSchemaVersion: "1",
		HeaderContentType:   ContentTypeJSON,
		HeaderService:       "demo-api",
		HeaderTraceID:       x.TraceID,
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("headers = %v, want %v", got, want)
	}
	x.TraceID = ""
	for _, h := range x.RecordHeaders() {
		if h.Key == HeaderTraceID {
			t.Error("trace-id header present without trace id")
		}
	}
}

func TestValidate(t *testing.T) {
	if err := (&Exchange{}).Validate(); err == nil {
		t.Fatal("empty exchange validated")
	} else {
		for _, want := range []string{"missing id", "missing service", "request.method", "request.path", "started_at", "response.status"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q does not mention %q", err, want)
			}
		}
	}
}

func TestHeaders(t *testing.T) {
	h := Headers{}
	h.Add("X-Foo", "1")
	h.Add("x-foo", "2")
	if h.Get("X-FOO") != "1" || len(h["x-foo"]) != 2 {
		t.Errorf("headers = %v", h)
	}
	c := h.Clone()
	c["x-foo"][0] = "changed"
	if h.Get("x-foo") != "1" {
		t.Error("Clone shares value slices")
	}
	if Headers(nil).Clone() != nil || Headers(nil).Get("a") != "" {
		t.Error("nil headers misbehave")
	}
}

func TestNewIDIsTimeOrdered(t *testing.T) {
	a := NewID()
	time.Sleep(2 * time.Millisecond)
	b := NewID()
	if len(a) != 36 || a >= b {
		t.Errorf("ids not ordered: %s >= %s", a, b)
	}
}
