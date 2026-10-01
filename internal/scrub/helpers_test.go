package scrub

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/zlib"
	"regexp"
	"testing"

	"github.com/klauspost/compress/zstd"
)

const (
	testKey  = "shadowtap-test-key-0123456789abcdef"
	otherKey = "another-key-for-shadowtap-tests!!"
)

// newTestScrubber returns a Scrubber with the test key and the defaults,
// adjusted by opts.
func newTestScrubber(tb testing.TB, opts ...func(*Config)) *Scrubber {
	tb.Helper()
	cfg := Config{Key: []byte(testKey)}
	for _, o := range opts {
		o(&cfg)
	}
	s, err := New(cfg)
	if err != nil {
		tb.Fatalf("New: %v", err)
	}
	return s
}

// matches reports whether s matches the regular expression pattern.
func matches(pattern, s string) bool { return regexp.MustCompile(pattern).MatchString(s) }

// assertClean fails if Detect finds anything in out.
func assertClean(tb testing.TB, what, out string) {
	tb.Helper()
	if ms := Detect(out); len(ms) > 0 {
		tb.Errorf("%s: Detect(%q) = %v, want no matches", what, out, ms)
	}
}

func gzipBytes(tb testing.TB, data []byte) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func zlibBytes(tb testing.TB, data []byte) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w := zlib.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func flateBytes(tb testing.TB, data []byte) []byte {
	tb.Helper()
	var buf bytes.Buffer
	w, err := flate.NewWriter(&buf, flate.DefaultCompression)
	if err != nil {
		tb.Fatal(err)
	}
	if _, err := w.Write(data); err != nil {
		tb.Fatal(err)
	}
	if err := w.Close(); err != nil {
		tb.Fatal(err)
	}
	return buf.Bytes()
}

func zstdBytes(tb testing.TB, data []byte) []byte {
	tb.Helper()
	enc, err := zstd.NewWriter(nil)
	if err != nil {
		tb.Fatal(err)
	}
	defer enc.Close()
	return enc.EncodeAll(data, nil)
}
