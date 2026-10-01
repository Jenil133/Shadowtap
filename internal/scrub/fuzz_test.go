package scrub

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// The fuzz targets check the scrubber's core promises on arbitrary input:
// no panics, idempotence, nothing Detect would flag in the output, and valid
// JSON staying valid.

func FuzzScrubText(f *testing.F) {
	for _, seed := range []string{
		"Contact alice@example.com or (415) 555-2671 about card 4111 1111 1111 1111.",
		"ssn 123-45-6789, itin 912-70-1234, ip 192.168.1.10:8080, +44 20 7946 0958",
		"Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U",
		"keys AKIAIOSFODNN7EXAMPLE sk_live_EXAMPLEFIXTURE01 xoxb-123456789012-abcdefABCDEF",
		"+1 012-34-5678",
		"note:\\n4111111111111111 and \\t415.555.2671",
		"order ORD-48213-7731 on 2026-10-03 for $1,299.99, ts 1759449600000",
		"user-0123456789@redacted.invalid tok_0123456789abcdef 240.1.2.3 000-12-3456 +0 015 055 0671",
		"0 4111111111111111",
		"Auth:0",
		"email=alice@example.com&password = hunter2; pin: 4821 \"token\": \"abc\"",
		"a@b.co.uk@c.de 1.2.3.4.5 v1.2.3.4 0.4111111111111111",
		"\xff\xfe invalid utf-8 alice@example.com",
		"",
	} {
		f.Add(seed)
	}
	s := newTestScrubber(f)
	f.Fuzz(func(t *testing.T, in string) {
		rep := Report{}
		out := s.Text(in, rep)
		assertClean(t, "Text", out)
		if again := s.Text(out, nil); again != out {
			t.Fatalf("Text not idempotent:\n in: %q\nout: %q\nagain: %q", in, out, again)
		}
		if (out != in) != (len(rep) > 0) {
			t.Fatalf("Text(%q) = %q with report %v", in, out, rep)
		}
		// Only an assignment to a sensitive name (password=x) changes text no
		// detector matches.
		if len(Detect(in)) == 0 && out != in && rep[KindSensitiveKey]+rep[KindEmail]+rep[KindPhone]+rep[KindCard]+rep[KindSSN]+rep[KindIPv4] == 0 {
			t.Fatalf("clean text changed: %q -> %q", in, out)
		}
	})
}

func FuzzScrubPath(f *testing.F) {
	for _, p := range []string{
		"/api/users/alice%40example.com/orders?email=bob%40example.org&page=2&access_token=abc",
		"/cards/4111111111111111?q=call+14155552671&x=%zz",
		"/x?user[phone]=%2B1%20415%20555%202671&a=%7E&b=hello+world&c",
		"/u/%2B1%20012-34-5678/?password=%zz&email=",
		"/search?q=%20%40example.com&r=%35%35%35",
		"/a//b/?#frag",
		"/call/+1+415+555+2671?q=+%31%34%31%35%35%35%35%32%36%37%31",
		"",
		"?",
	} {
		f.Add(p)
	}
	s := newTestScrubber(f)
	f.Fuzz(func(t *testing.T, in string) {
		out := s.Path(in, nil)
		assertClean(t, "Path", out)
		if !cleanViews(out, textDetectors) {
			t.Fatalf("Path(%q) = %q, dirty once unescaped", in, out)
		}
		rep := Report{}
		if again := s.Path(out, rep); again != out || len(rep) != 0 {
			t.Fatalf("Path not idempotent:\n in: %q\nout: %q\nagain: %q (%v)", in, out, again, rep)
		}
	})
}

func FuzzScrubJSON(f *testing.F) {
	for _, j := range []string{
		piiJSON,
		`{"email":"alice@example.com","password":"hunter2","card":{"number":"4111 1111 1111 1111","cvv":"737"},"items":[{"qty":2,"price":19.99}]}`,
		`{"address":{"street":"1 Main St","zip":"94105","geo":{"lat":37.7,"ok":true}},"phone":["415-555-2671",5552671,null]}`,
		`{"n":12345678901234567890,"f":1e-7,"x":0.4111111111111111,"c":4111111111111111,"e":1e+14155552671}`,
		`{"s":"caf\u00e9 \ud83d\ude00 \ud800 <b>&</b>","k":"alice\u0040example.com","\u0020@example.com":"\u001f@example.com"}`,
		`{"a":1,"a":2}`,
		"[\n  \"x\" ,\t{\"pass\\u0077ord\" : \"p\"}\r\n]",
		`"just a string 123-45-6789"`,
		`-0.5e-10`,
		`{"a":`,
		`[1,2,]`,
		"{\"a\":\"\xff\"}",
	} {
		f.Add([]byte(j))
	}
	s := newTestScrubber(f)
	f.Fuzz(func(t *testing.T, in []byte) {
		res := s.Body(BodyInput{Data: in, ContentType: "application/json"}, nil)
		valid := json.Valid(in)
		switch {
		case len(in) == 0:
			if res.Omitted != "" || res.Data != nil {
				t.Fatalf("empty body: %+v", res)
			}
			return
		case !valid:
			if res.Omitted != capture.OmittedUnparseable {
				t.Fatalf("invalid JSON %q: omitted %q", in, res.Omitted)
			}
			return
		case jsonDepth(in) > maxJSONDepth:
			if res.Omitted != capture.OmittedUnparseable {
				t.Fatalf("too deep JSON kept: %+v", res)
			}
			return
		}
		if res.Omitted != "" {
			t.Fatalf("valid JSON %q omitted: %s", in, res.Omitted)
		}
		if !json.Valid(res.Data) {
			t.Fatalf("valid JSON %q became invalid: %q", in, res.Data)
		}
		assertClean(t, "JSON", string(res.Data))
		rep := Report{}
		again := s.Body(BodyInput{Data: res.Data, ContentType: "application/json"}, rep)
		if !bytes.Equal(again.Data, res.Data) || len(rep) != 0 {
			t.Fatalf("JSON not idempotent:\n in: %q\nout: %q\nagain: %q (%v)", in, res.Data, again.Data, rep)
		}
	})
}

// jsonDepth returns the deepest nesting of objects and arrays in valid JSON.
func jsonDepth(b []byte) int {
	depth, deepest, inString := 0, 0, false
	for i := 0; i < len(b); i++ {
		switch c := b[i]; {
		case inString && c == '\\':
			i++
		case c == '"':
			inString = !inString
		case inString:
		case c == '{' || c == '[':
			depth++
			deepest = max(deepest, depth)
		case c == '}' || c == ']':
			depth--
		}
	}
	return deepest
}

func FuzzScrubHeaderValue(f *testing.F) {
	seeds := []string{
		"Bearer eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJhIn0.c2ln",
		"Basic dXNlcjpwYXNz",
		"session=abc123; theme=dark;  lang=\"en-US\"; flag",
		"sid=xyz; Path=/; Expires=Wed, 21 Oct 2026 07:28:00 GMT; HttpOnly",
		"203.0.113.7, 2001:db8::1,unknown, 10.0.0.1:8080, [2001:db8::2]:443, ::ffff:198.51.100.4",
		`for=192.0.2.60;proto=http;by=203.0.113.43, for="[2001:db8:cafe::17]:4711"`,
		"https://alice:pw@shop.example.com/u/alice@example.com?email=carol%40example.com#access_token=xyz",
		"http://[2001:db8::1]:8080/a?b=c",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 alice@example.com",
		"4111111111111111 abc",
		"/api/users?email=alice%40example.com&page=2",
		"note alice%40example.com+4111+1111+1111+1111",
		"",
	}
	for i, v := range seeds {
		f.Add(uint8(i), v)
	}
	names := []string{
		"authorization", "proxy-authorization", "x-api-key", "cookie", "set-cookie",
		"x-forwarded-for", "forwarded", "x-real-ip", "referer", "location", "user-agent",
		"x-custom", "x-envoy-original-path", "x-envoy-peer-metadata",
	}
	s := newTestScrubber(f)
	f.Fuzz(func(t *testing.T, sel uint8, v string) {
		name := names[int(sel)%len(names)]
		out := s.Headers(capture.Headers{name: {v}}, nil)
		vals, ok := out[name]
		if name == "x-envoy-peer-metadata" {
			if ok {
				t.Fatal("drop header kept")
			}
			return
		}
		if !ok || len(vals) != 1 {
			t.Fatalf("%s: values %q", name, vals)
		}
		assertClean(t, name, vals[0])
		if !cleanViews(vals[0], textDetectors) {
			t.Fatalf("%s: %q -> %q, dirty once unescaped", name, v, vals[0])
		}
		rep := Report{}
		if again := s.Headers(out, rep); !reflect.DeepEqual(again, out) || len(rep) != 0 {
			t.Fatalf("%s not idempotent:\n in: %q\nout: %q\nagain: %q (%v)", name, v, vals[0], again[name], rep)
		}
	})
}
