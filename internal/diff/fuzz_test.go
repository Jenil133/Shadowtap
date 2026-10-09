package diff

import (
	"slices"
	"strings"
	"testing"
)

// FuzzDiffJSON checks that any pair of documents compares without panicking,
// that a document never differs from itself, and that a difference found one
// way is found the other way too.
func FuzzDiffJSON(f *testing.F) {
	for _, s := range [][2]string{
		{`{"a":1}`, `{"a":2}`},
		{`[1,2,3]`, `[3,2]`},
		{`{"a":{"b":[{"c":null}]}}`, `{"a":{"b":[{"c":"x"}]},"d":true}`},
		{`1e2`, `100`},
		{`"x"`, `{"x":1}`},
		{`{"u":[1,2,2],"k":[{"id":1,"v":1}]}`, `{"u":[2,1,3],"k":[{"id":2},{"id":1,"v":2}]}`},
	} {
		f.Add(s[0], s[1])
	}
	c, err := compile(Config{UnorderedArrays: []string{"/u"}, ArrayKeys: map[string]string{"/k": "id"}})
	if err != nil {
		f.Fatal(err)
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		pa, errA := parseJSON([]byte(a))
		pb, errB := parseJSON([]byte(b))
		if errA != nil || errB != nil {
			diffText(a, b) // must not panic either
			return
		}
		if d, _ := c.diffJSON(pa, pa, nil); len(d) != 0 {
			t.Fatalf("document differs from itself: %+v", d)
		}
		ab, _ := c.diffJSON(pa, pb, nil)
		ba, _ := c.diffJSON(pb, pa, nil)
		if (len(ab) == 0) != (len(ba) == 0) {
			t.Fatalf("asymmetric: %d vs %d differences", len(ab), len(ba))
		}
		for _, d := range ab {
			if d.Where != WhereBody || !strings.HasPrefix(d.Pointer, "/") && d.Pointer != "" {
				t.Fatalf("malformed difference %+v", d)
			}
		}
	})
}

// FuzzDiffText checks the line differ on arbitrary text: no panic, a text
// never differs from itself, and a change is one summarized difference.
func FuzzDiffText(f *testing.F) {
	for _, s := range [][2]string{
		{"a\nb\nc\n", "a\nB\nc\n"},
		{"", "x"},
		{"same", "same"},
		{strings.Repeat("l\n", 50), strings.Repeat("l\n", 49) + "m\n"},
	} {
		f.Add(s[0], s[1])
	}
	f.Fuzz(func(t *testing.T, a, b string) {
		if d := diffText(a, a); d != nil {
			t.Fatalf("text differs from itself: %+v", d)
		}
		ab, ba := diffText(a, b), diffText(b, a)
		if (ab == nil) != (a == b) || (ba == nil) != (a == b) {
			t.Fatalf("differs=%v/%v for equal=%v", ab != nil, ba != nil, a == b)
		}
		if ab != nil && (len(ab) != 1 || !strings.HasPrefix(ab[0].Pointer, "line ") || ab[0].Where != WhereBody) {
			t.Fatalf("text difference %+v", ab)
		}
	})
}

// FuzzPointerPattern checks JSON Pointer handling: segments survive an
// escape round trip, and a pattern always matches the pointer it was parsed
// from.
func FuzzPointerPattern(f *testing.F) {
	for _, s := range []string{"", "/", "/a/b", "/a~1b/c~0d", "/users/*/id", "/debug/**", "a", "/*/**", "//"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		segs := strings.Split(s, "/")
		if back := splitPointer(pointer(segs)); !slices.Equal(back, segs) {
			t.Fatalf("round trip of %q: %q", segs, back)
		}
		p, err := parsePattern(s)
		if err != nil {
			if s == "" || strings.HasPrefix(s, "/") {
				t.Fatalf("valid pattern %q rejected: %v", s, err)
			}
			return
		}
		if !p.match(splitPointer(s)) {
			t.Fatalf("pattern %q does not match its own pointer", s)
		}
		if p.rest && !p.match(append(splitPointer(s), "deeper")) {
			t.Fatalf("pattern %q with ** does not match a deeper pointer", s)
		}
	})
}

// FuzzRouteTemplate checks that collapsing identifiers never panics and is
// idempotent: a template is its own template.
func FuzzRouteTemplate(f *testing.F) {
	for _, s := range []string{"/api/users/u_3f2a1b9c0d4e?x=1", "/x/%31%32/y", "//", "", "/a%2Fb/7", "/x/{id}", "0", "?x=1", "a/b"} {
		f.Add("get", s)
	}
	f.Add("?", "0") // a method is a token, but the path part must stay well-formed whatever it is
	f.Fuzz(func(t *testing.T, method, path string) {
		tmpl := RouteTemplate(method, path)
		prefix := strings.ToUpper(method) + " "
		if !strings.HasPrefix(tmpl, prefix) {
			t.Fatalf("RouteTemplate(%q, %q) = %q", method, path, tmpl)
		}
		// The shape checks apply to the path part: the method is reproduced
		// as given and may itself hold any character.
		route := tmpl[len(prefix):]
		if !strings.HasPrefix(route, "/") || strings.Contains(route, "?") {
			t.Fatalf("RouteTemplate(%q, %q) = %q: path part %q is not an absolute path without a query", method, path, tmpl, route)
		}
		if again := RouteTemplate(method, route); again != tmpl {
			t.Fatalf("not idempotent: %q then %q", tmpl, again)
		}
	})
}
