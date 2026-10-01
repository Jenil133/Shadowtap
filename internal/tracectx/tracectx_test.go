package tracectx

import (
	"strings"
	"testing"
)

func TestParseTraceparent(t *testing.T) {
	const tid, sid = "4bf92f3577b34da6a3ce929d0e0e4736", "00f067aa0ba902b7"
	tests := []struct {
		in      string
		ok      bool
		sampled bool
	}{
		{"00-" + tid + "-" + sid + "-01", true, true},
		{"00-" + tid + "-" + sid + "-00", true, false},
		{"  00-" + tid + "-" + sid + "-03 ", true, true},
		{"01-" + tid + "-" + sid + "-01-future-fields", true, true}, // higher version may extend
		{"01-" + tid + "-" + sid + "-01", true, true},
		{"00-" + tid + "-" + sid + "-01-extra", false, false}, // version 00 must be exact
		{"01-" + tid + "-" + sid + "-01x", false, false},
		{"ff-" + tid + "-" + sid + "-01", false, false},
		{"00-" + strings.ToUpper(tid) + "-" + sid + "-01", false, false},
		{"00-00000000000000000000000000000000-" + sid + "-01", false, false},
		{"00-" + tid + "-0000000000000000-01", false, false},
		{"00-" + tid + "-" + sid + "-0g", false, false},
		{"00-" + tid[:31] + "-" + sid + "-01", false, false},
		{"00_" + tid + "_" + sid + "_01", false, false},
		{"", false, false},
	}
	for _, tt := range tests {
		c, ok := ParseTraceparent(tt.in)
		if ok != tt.ok {
			t.Errorf("ParseTraceparent(%q) ok = %v, want %v", tt.in, ok, tt.ok)
			continue
		}
		if !ok {
			continue
		}
		if c.TraceID != tid || c.SpanID != sid || c.Sampled != tt.sampled || c.Source != SourceTraceparent {
			t.Errorf("ParseTraceparent(%q) = %+v", tt.in, c)
		}
	}
}

func TestParseB3(t *testing.T) {
	tests := []struct {
		in      string
		ok      bool
		trace   string
		sampled bool
	}{
		{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1-05e3ac9a4f6e3b90", true, "80f198ee56343ba864fe8b2a57d3eff7", true},
		{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-d", true, "80f198ee56343ba864fe8b2a57d3eff7", true},
		{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-0", true, "80f198ee56343ba864fe8b2a57d3eff7", false},
		{"80F198EE56343BA864FE8B2A57D3EFF7-E457B5A2E4D86BD1", true, "80f198ee56343ba864fe8b2a57d3eff7", false},
		{"64fe8b2a57d3eff7-e457b5a2e4d86bd1-1", true, "000000000000000064fe8b2a57d3eff7", true},
		{"1", false, "", false},
		{"0", false, "", false},
		{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-x", false, "", false},
		{"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1-zz", false, "", false},
		{"80f198ee56343ba864fe8b2a57d3eff7-short", false, "", false},
		{"0000000000000000-e457b5a2e4d86bd1", false, "", false},
		{"a-b-c-d-e", false, "", false},
	}
	for _, tt := range tests {
		c, ok := ParseB3(tt.in)
		if ok != tt.ok {
			t.Errorf("ParseB3(%q) ok = %v, want %v", tt.in, ok, tt.ok)
			continue
		}
		if ok && (c.TraceID != tt.trace || c.Sampled != tt.sampled || c.Source != SourceB3) {
			t.Errorf("ParseB3(%q) = %+v", tt.in, c)
		}
	}
}

func TestFromHeadersPrecedence(t *testing.T) {
	h := map[string]string{
		"traceparent":  "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"b3":           "80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1",
		"x-b3-traceid": "463ac35c9f6413ad48485a3953bb6124",
	}
	get := func(k string) string { return h[k] }

	c, ok := FromHeaders(get)
	if !ok || c.Source != SourceTraceparent {
		t.Fatalf("got %+v, want traceparent", c)
	}
	h["traceparent"] = "garbage"
	if c, _ = FromHeaders(get); c.Source != SourceB3 {
		t.Fatalf("got %+v, want b3 fallback", c)
	}
	delete(h, "b3")
	h["x-b3-spanid"] = "a2fb4a1d1a96d312"
	h["x-b3-sampled"] = "1"
	c, ok = FromHeaders(get)
	if !ok || c.Source != SourceB3Multi || c.TraceID != "463ac35c9f6413ad48485a3953bb6124" || c.SpanID != "a2fb4a1d1a96d312" || !c.Sampled {
		t.Fatalf("got %+v, want x-b3 multi", c)
	}
	h["x-b3-spanid"] = "bad"
	h["x-b3-sampled"] = "0"
	h["x-b3-flags"] = "1"
	if c, _ = FromHeaders(get); c.SpanID != "" || !c.Sampled {
		t.Fatalf("got %+v, want debug-sampled without span", c)
	}
	delete(h, "x-b3-traceid")
	if _, ok := FromHeaders(get); ok {
		t.Fatal("expected no context")
	}
}

func FuzzParseTraceparent(f *testing.F) {
	f.Add("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
	f.Add("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x")
	f.Add("")
	f.Fuzz(func(t *testing.T, s string) {
		c, ok := ParseTraceparent(s)
		if !ok {
			return
		}
		if len(c.TraceID) != 32 || len(c.SpanID) != 16 || !isLowerHex(c.TraceID) || !isLowerHex(c.SpanID) {
			t.Fatalf("invalid ids from %q: %+v", s, c)
		}
	})
}

func FuzzParseB3(f *testing.F) {
	f.Add("80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1-05e3ac9a4f6e3b90")
	f.Add("64fe8b2a57d3eff7-e457b5a2e4d86bd1")
	f.Fuzz(func(t *testing.T, s string) {
		c, ok := ParseB3(s)
		if !ok {
			return
		}
		if len(c.TraceID) != 32 || len(c.SpanID) != 16 || !isLowerHex(c.TraceID) || isZero(c.TraceID) {
			t.Fatalf("invalid ids from %q: %+v", s, c)
		}
	})
}
