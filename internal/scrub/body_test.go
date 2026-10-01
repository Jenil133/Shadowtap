package scrub

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

const piiJSON = `{"email":"alice@example.com","note":"card 4111 1111 1111 1111"}`

func TestBodyEncodings(t *testing.T) {
	s := newTestScrubber(t)
	plain := []byte(piiJSON)
	tests := []struct {
		name, encoding string
		data           []byte
		decodedFrom    string
		omitted        string
	}{
		{"identity", "identity", plain, "", ""},
		{"gzip", "gzip", gzipBytes(t, plain), "gzip", ""},
		{"x-gzip upper case", " X-GZIP ", gzipBytes(t, plain), "x-gzip", ""},
		{"deflate zlib", "deflate", zlibBytes(t, plain), "deflate", ""},
		{"deflate raw", "deflate", flateBytes(t, plain), "deflate", ""},
		{"zstd", "zstd", zstdBytes(t, plain), "zstd", ""},
		{"stacked", "deflate, identity, gzip", gzipBytes(t, zlibBytes(t, plain)), "deflate, gzip", ""},
		{"brotli", "br", []byte{0x1b, 0x2c, 0x00}, "", capture.OmittedUnsupportedEncoding},
		{"unknown in a list", "gzip, compress", gzipBytes(t, plain), "", capture.OmittedUnsupportedEncoding},
		{"corrupt gzip", "gzip", []byte("this is not gzip at all"), "", capture.OmittedUnparseable},
		{"corrupt zstd", "zstd", []byte("this is not zstd at all"), "", capture.OmittedUnparseable},
		{"plain data labelled deflate", "deflate", plain, "", capture.OmittedUnparseable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rep := Report{}
			res := s.Body(BodyInput{Data: tt.data, ContentType: "application/json", ContentEncoding: tt.encoding}, rep)
			if res.Omitted != tt.omitted || res.DecodedFrom != tt.decodedFrom {
				t.Fatalf("omitted %q decodedFrom %q, want %q %q", res.Omitted, res.DecodedFrom, tt.omitted, tt.decodedFrom)
			}
			if tt.omitted != "" {
				if res.Data != nil || len(rep) != 0 {
					t.Errorf("omitted body kept data %q or counted %v", res.Data, rep)
				}
				return
			}
			if !matches(`^\{"email":"user-[0-9a-f]{10}@redacted\.invalid","note":"card 0\d{3} \d{4} \d{4} \d{4}"\}$`, string(res.Data)) {
				t.Errorf("data = %s", res.Data)
			}
			if res.Truncated {
				t.Error("complete body marked truncated")
			}
			if rep[KindEmail] != 1 || rep[KindCard] != 1 {
				t.Errorf("report = %v", rep)
			}
		})
	}
}

func TestBodyDecompressionBomb(t *testing.T) {
	s := newTestScrubber(t)
	bomb := gzipBytes(t, make([]byte, 50<<20))
	start := time.Now()
	res := s.Body(BodyInput{Data: bomb, ContentType: "text/plain", ContentEncoding: "gzip"}, nil)
	elapsed := time.Since(start)
	if elapsed > 2*time.Second {
		t.Errorf("decoding a %d-byte bomb took %v", len(bomb), elapsed)
	}
	if !res.Truncated || res.DecodedFrom != "gzip" {
		t.Errorf("truncated %v decodedFrom %q", res.Truncated, res.DecodedFrom)
	}
	// Zeros contain no newline, so the truncated text cannot be cut safely.
	if res.Omitted != capture.OmittedTruncatedUnsafe {
		t.Errorf("omitted %q, want %q", res.Omitted, capture.OmittedTruncatedUnsafe)
	}

	lines := bytes.Repeat([]byte("call 415-555-2671 now\n"), 200_000) // 4.4 MB
	small := newTestScrubber(t, func(c *Config) { c.MaxDecodedBytes = 64 << 10 })
	res = small.Body(BodyInput{Data: zstdBytes(t, lines), ContentType: "text/plain", ContentEncoding: "zstd"}, nil)
	if res.Omitted != "" || !res.Truncated || len(res.Data) > 64<<10 || !bytes.HasSuffix(res.Data, []byte("\n")) {
		t.Errorf("capped zstd text: omitted %q truncated %v len %d", res.Omitted, res.Truncated, len(res.Data))
	}
	assertClean(t, "capped body", string(res.Data))
}

func TestBodyTruncatedCompressedStream(t *testing.T) {
	s := newTestScrubber(t)
	text := bytes.Repeat([]byte("contact alice@example.com today\n"), 2000)
	gz := gzipBytes(t, text)
	cut := gz[:len(gz)/2]
	res := s.Body(BodyInput{Data: cut, ContentType: "text/plain", ContentEncoding: "gzip", Truncated: true}, nil)
	if res.Omitted != "" || !res.Truncated || res.DecodedFrom != "gzip" || len(res.Data) == 0 {
		t.Fatalf("omitted %q truncated %v decodedFrom %q len %d", res.Omitted, res.Truncated, res.DecodedFrom, len(res.Data))
	}
	if !bytes.HasSuffix(res.Data, []byte("\n")) || bytes.Contains(res.Data, []byte("alice@")) {
		t.Errorf("truncated text not cut at a line or not scrubbed: ...%q", res.Data[max(0, len(res.Data)-60):])
	}
	// The tap may not know it cut the stream; unexpected EOF still marks it.
	res = s.Body(BodyInput{Data: cut, ContentType: "text/plain", ContentEncoding: "gzip"}, nil)
	if !res.Truncated {
		t.Error("unexpected EOF did not mark the body truncated")
	}
	res = s.Body(BodyInput{Data: gzipBytes(t, []byte(piiJSON))[:30], ContentType: "application/json", ContentEncoding: "gzip"}, nil)
	if res.Omitted != capture.OmittedTruncatedUnsafe {
		t.Errorf("truncated gzip JSON: omitted %q, want %q", res.Omitted, capture.OmittedTruncatedUnsafe)
	}
	res = s.Body(BodyInput{Data: gzipBytes(t, []byte(piiJSON))[:5], ContentType: "application/json", ContentEncoding: "gzip"}, nil)
	if res.Omitted != "" || res.Data != nil || !res.Truncated {
		t.Errorf("gzip cut inside its header: %+v", res)
	}
}

func TestBodyTruncation(t *testing.T) {
	s := newTestScrubber(t)
	tests := []struct {
		name, contentType, in string
		want, omitted         string // want is a regular expression
	}{
		{"json", "application/json", `{"email":"alice@exa`, "", capture.OmittedTruncatedUnsafe},
		{"json that parses", "application/json", `{"a":1}`, "", capture.OmittedTruncatedUnsafe},
		{"ndjson", "application/x-ndjson", "{\"email\":\"alice@example.com\"}\n{\"email\":\"bob@exa", `^\{"email":"user-[0-9a-f]{10}@redacted\.invalid"\}\n$`, ""},
		{"ndjson without newline", "application/x-ndjson", `{"email":"alice@exa`, "", capture.OmittedTruncatedUnsafe},
		{"text", "text/plain", "line one alice@example.com\nline two 4111 1111 11", `^line one user-[0-9a-f]{10}@redacted\.invalid\n$`, ""},
		{"text without newline", "text/plain; charset=utf-8", "4111 1111 1111 11", "", capture.OmittedTruncatedUnsafe},
		{"form", "application/x-www-form-urlencoded", "a=1&email=alice%40example.com&card=4111+1111+11", `^a=1&email=user-[0-9a-f]{10}%40redacted\.invalid&$`, ""},
		{"form without separator", "application/x-www-form-urlencoded", "email=alice%40exa", "", capture.OmittedTruncatedUnsafe},
		{"unknown type that looks like json", "", `{"a":`, "", capture.OmittedTruncatedUnsafe},
		{"unknown binary", "", "\x89PNG\r\n", "", capture.OmittedBinary},
		{"multipart", "multipart/form-data; boundary=x", "--x\r\n", "", capture.OmittedMultipart},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res := s.Body(BodyInput{Data: []byte(tt.in), ContentType: tt.contentType, Truncated: true}, nil)
			if res.Omitted != tt.omitted || !res.Truncated {
				t.Fatalf("omitted %q truncated %v, want %q and truncated", res.Omitted, res.Truncated, tt.omitted)
			}
			if tt.omitted == "" && !matches(tt.want, string(res.Data)) {
				t.Errorf("data %q, want %s", res.Data, tt.want)
			}
			if tt.omitted != "" && res.Data != nil {
				t.Errorf("omitted body kept %q", res.Data)
			}
			if tt.omitted == "" {
				// Cutting is idempotent.
				again := s.Body(BodyInput{Data: res.Data, ContentType: tt.contentType, Truncated: true}, nil)
				if !bytes.Equal(again.Data, res.Data) {
					t.Errorf("second pass %q, want %q", again.Data, res.Data)
				}
			}
		})
	}
}

func TestBodyMediaTypes(t *testing.T) {
	s := newTestScrubber(t, func(c *Config) { c.TextContentTypes = []string{"Text/Markdown"} })
	const email = "alice@example.com"
	tests := []struct {
		contentType, in, omitted string
		scrubbed                 bool
	}{
		{"application/json; charset=utf-8", `{"a":"` + email + `"}`, "", true},
		{"application/problem+json", `{"detail":"` + email + `"}`, "", true},
		{"application/vnd.api+json", `{"a":"` + email + `"}`, "", true},
		{"text/json", `["` + email + `"]`, "", true},
		{"application/x-www-form-urlencoded", "user=" + email + "&x=1", "", true},
		{"text/plain", "hi " + email, "", true},
		{"text/html", "<p>" + email + "</p>", "", true},
		{"text/csv", "name,email\nAlice," + email + "\n", "", true},
		{"text/event-stream", "data: " + email + "\n\n", "", true},
		{"application/graphql", "{ user(email: \"" + email + "\") { id } }", "", true},
		{"text/markdown", "*" + email + "*", "", true},
		{"", `{"a":"` + email + `"}`, "", true},
		{"application/x-custom", `{"a":"` + email + `"}`, "", true},
		{"application/x-custom", "not json " + email, capture.OmittedBinary, false},
		{"", "plain words " + email, capture.OmittedBinary, false},
		{"text/plain", "bad utf-8 \xff " + email, capture.OmittedBinary, false},
		{"multipart/form-data; boundary=b", "--b\r\n\r\n" + email + "\r\n--b--", capture.OmittedMultipart, false},
		{"application/xml", "<email>" + email + "</email>", capture.OmittedBinary, false},
		{"text/xml", "<a/>", capture.OmittedBinary, false},
		{"application/soap+xml", "<a/>", capture.OmittedBinary, false},
		{"application/grpc", "\x00\x00\x00\x00\x05hello", capture.OmittedBinary, false},
		{"application/grpc+json", `{"a":"` + email + `"}`, capture.OmittedBinary, false},
		{"application/x-protobuf", "\x0a\x05hello", capture.OmittedBinary, false},
		{"application/octet-stream", `{"a":1}`, capture.OmittedBinary, false},
		{"image/png", "\x89PNG", capture.OmittedBinary, false},
		{"application/json", `{"a":`, capture.OmittedUnparseable, false},
		{"application/json;;", `{"a":"` + email + `"}`, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.contentType+"/"+tt.in, func(t *testing.T) {
			res := s.Body(BodyInput{Data: []byte(tt.in), ContentType: tt.contentType}, nil)
			if res.Omitted != tt.omitted {
				t.Fatalf("omitted %q, want %q", res.Omitted, tt.omitted)
			}
			if tt.scrubbed {
				if bytes.Contains(res.Data, []byte(email)) || !bytes.Contains(res.Data, []byte("redacted.invalid")) {
					t.Errorf("data %q not scrubbed", res.Data)
				}
				assertClean(t, "body", string(res.Data))
			}
		})
	}
}

func TestBodyEmpty(t *testing.T) {
	s := newTestScrubber(t)
	for _, in := range []BodyInput{
		{},
		{ContentType: "application/json"},
		{ContentType: "image/png", ContentEncoding: "br", Truncated: true},
	} {
		res := s.Body(in, nil)
		if res.Omitted != "" || res.Data != nil || res.Truncated != in.Truncated {
			t.Errorf("Body(%+v) = %+v, want an empty result", in, res)
		}
	}
}

func TestBodyUnchangedDataIsReturned(t *testing.T) {
	s := newTestScrubber(t)
	for _, ct := range []string{"application/json", "text/plain", "application/x-www-form-urlencoded", "application/x-ndjson"} {
		data := []byte(`{"a": 1,  "b": [true, null]}`)
		if ct == "application/x-www-form-urlencoded" {
			data = []byte("a=1&b=%7E&c")
		}
		res := s.Body(BodyInput{Data: data, ContentType: ct}, nil)
		if !bytes.Equal(res.Data, data) {
			t.Errorf("%s: %q rewritten to %q", ct, data, res.Data)
		}
	}
}

func TestBodyCountsOnlyKeptData(t *testing.T) {
	s := newTestScrubber(t)
	rep := Report{}
	in := "{\"email\":\"alice@example.com\"}\n{\"broken\":\n"
	if res := s.Body(BodyInput{Data: []byte(in), ContentType: "application/x-ndjson"}, rep); res.Omitted != capture.OmittedUnparseable {
		t.Fatalf("omitted %q", res.Omitted)
	}
	if len(rep) != 0 {
		t.Errorf("omitted body counted %v", rep)
	}
}

func TestGuardFailsClosed(t *testing.T) {
	rep := Report{}
	failed := false
	guard(rep, func(r Report) {
		r.Add(KindEmail, 1)
		var m map[string]int
		m["boom"] = 1 // panics
	}, func() { failed = true })
	if !failed || len(rep) != 0 {
		t.Errorf("failed %v, report %v; want the fallback and no counts", failed, rep)
	}
	guard(rep, func(r Report) { r.Add(KindEmail, 2) }, func() { t.Error("fallback ran without a panic") })
	if rep[KindEmail] != 2 {
		t.Errorf("report = %v", rep)
	}
}

func TestMediaType(t *testing.T) {
	for in, want := range map[string]string{
		"application/json; charset=utf-8": "application/json",
		"Application/JSON":                "application/json",
		"text/plain; charset":             "text/plain",
		" text/html ;":                    "text/html",
		"":                                "",
		"json":                            "json",
	} {
		if got := mediaType(in); got != want {
			t.Errorf("mediaType(%q) = %q, want %q", in, got, want)
		}
	}
	if strings.Contains(mediaType("a/b; c=\"d"), ";") {
		t.Error("parameters leaked into the media type")
	}
}
