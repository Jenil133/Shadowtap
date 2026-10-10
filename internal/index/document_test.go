package index

import (
	"testing"

	coltracev1 "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonv1 "go.opentelemetry.io/proto/otlp/common/v1"
)

// coltraceRequestEmpty is an export request with nothing in it.
var coltraceRequestEmpty coltracev1.ExportTraceServiceRequest

func TestPut(t *testing.T) {
	m := map[string]any{}
	put(m, "http.request.method", "GET")
	put(m, "http.request.id", "r1")
	put(m, "http.response.status_code", 200)
	put(m, "flat", true)
	if str(m, "http.request.method") != "GET" || str(m, "http.request.id") != "r1" || field(m, "flat") != true {
		t.Errorf("put built %v", m)
	}
	// A scalar in the way is replaced by an object.
	put(m, "flat.inner", 1)
	if field(m, "flat.inner") != 1 {
		t.Errorf("put over a scalar built %v", m)
	}
}

func TestHexID(t *testing.T) {
	tests := []struct {
		name string
		in   []byte
		size int
		want string
		ok   bool
	}{
		{"trace id", []byte{0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e, 0x0e, 0x47, 0x36}, 16, fixtureTraceHex, true},
		{"span id", []byte{0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7}, 8, fixtureSpanHex, true},
		{"wrong size", []byte{1, 2, 3}, 8, "", false},
		{"all zero", make([]byte, 8), 8, "", false},
		{"nil", nil, 16, "", false},
	}
	for _, tt := range tests {
		got, ok := hexID(tt.in, tt.size)
		if got != tt.want || ok != tt.ok {
			t.Errorf("%s: hexID = %q, %v; want %q, %v", tt.name, got, ok, tt.want, tt.ok)
		}
	}
}

func TestAttributeHelpers(t *testing.T) {
	attrs := attributeMap([]*commonv1.KeyValue{
		kv("s", "text"), kv("i", 7), kv("f", 2.5), kv("b", true), kv("whole", 3.0), kv("numeric", " 42 "),
		kv("", "dropped"), nil, {Key: "nilvalue"},
	})
	if _, ok := attrs[""]; ok {
		t.Error("empty key kept")
	}
	if v, ok := attrs["nilvalue"]; !ok || v != nil {
		t.Errorf("nil value = %v, %v", v, ok)
	}
	for _, tt := range []struct {
		keys []string
		want string
		ok   bool
	}{
		{[]string{"s"}, "text", true},
		{[]string{"missing", "i"}, "7", true},
		{[]string{"f"}, "2.5", true},
		{[]string{"b"}, "true", true},
		{[]string{"missing"}, "", false},
		{[]string{"nilvalue"}, "", false},
	} {
		if got, ok := stringAttr(attrs, tt.keys...); got != tt.want || ok != tt.ok {
			t.Errorf("stringAttr(%v) = %q, %v; want %q, %v", tt.keys, got, ok, tt.want, tt.ok)
		}
	}
	for _, tt := range []struct {
		keys []string
		want int64
		ok   bool
	}{
		{[]string{"i"}, 7, true},
		{[]string{"whole"}, 3, true},
		{[]string{"numeric"}, 42, true},
		{[]string{"f"}, 0, false},
		{[]string{"s"}, 0, false},
		{[]string{"missing"}, 0, false},
	} {
		if got, ok := intAttr(attrs, tt.keys...); got != tt.want || ok != tt.ok {
			t.Errorf("intAttr(%v) = %d, %v; want %d, %v", tt.keys, got, ok, tt.want, tt.ok)
		}
	}
	if attributeMap(nil) != nil {
		t.Error("attributeMap(nil) is not nil")
	}
}

func TestAnyValueSpecialFloats(t *testing.T) {
	for _, f := range []float64{nan(), inf(1), inf(-1)} {
		v := anyValue(anyValueOf(f))
		if _, ok := v.(string); !ok {
			t.Errorf("anyValue(%v) = %T, want a string since JSON has no NaN or Inf", f, v)
		}
	}
	if anyValue(nil) != nil || anyValue(&commonv1.AnyValue{}) != nil {
		t.Error("empty values are not nil")
	}
}

func TestSourceTraceID(t *testing.T) {
	tp := "00-" + fixtureTraceHex + "-" + fixtureSpanHex + "-01"
	tests := []struct {
		name    string
		headers map[string]string
		want    string
	}{
		{"traceparent", map[string]string{"traceparent": tp}, fixtureTraceHex},
		{"traceparent in any case", map[string]string{"Traceparent": tp}, fixtureTraceHex},
		{"trace-id header", map[string]string{"trace-id": fixtureLinkTrace}, fixtureLinkTrace},
		{"upper-case trace-id header", map[string]string{"trace-id": " 4BF92F3577B34DA6A3CE929D0E0E4736 "}, fixtureTraceHex},
		{"traceparent wins", map[string]string{"traceparent": tp, "trace-id": fixtureLinkTrace}, fixtureTraceHex},
		{"invalid traceparent falls back", map[string]string{"traceparent": "garbage", "trace-id": fixtureLinkTrace}, fixtureLinkTrace},
		{"invalid trace-id", map[string]string{"trace-id": "not-hex"}, ""},
		{"short trace-id", map[string]string{"trace-id": fixtureSpanHex}, ""},
		{"none", nil, ""},
	}
	for _, tt := range tests {
		if got := (Source{Headers: tt.headers}).traceID(); got != tt.want {
			t.Errorf("%s: traceID = %q, want %q", tt.name, got, tt.want)
		}
	}
	p := Source{Headers: map[string]string{"traceparent": tp}}.propagation()
	if str(p, "trace.id") != fixtureTraceHex || str(p, "span.id") != fixtureSpanHex || p["sampled"] != true || p["traceparent"] != tp {
		t.Errorf("propagation = %v", p)
	}
	if (Source{}).propagation() != nil {
		t.Error("propagation without a header is not nil")
	}
}

func nan() float64      { z := 0.0; return z / z }
func inf(s int) float64 { z := 0.0; return float64(s) / z }
