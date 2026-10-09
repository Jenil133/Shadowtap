package diff

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"math"
	"slices"
	"strconv"
	"unicode/utf8"
)

// Difference kinds.
const (
	KindChanged = "changed" // same type, different value
	KindAdded   = "added"   // only the candidate has it
	KindRemoved = "removed" // only production has it
	KindType    = "type"    // the JSON types differ
)

// maxJSONDepth bounds recursion; deeper documents are compared as text.
const maxJSONDepth = 128

// errTooDeep reports a document nested beyond maxJSONDepth.
var errTooDeep = errors.New("diff: JSON nested too deeply")

// parseJSON decodes one JSON document, keeping numbers as written.
func parseJSON(data []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("diff: trailing data after JSON document")
	}
	if depth(v, 0) > maxJSONDepth {
		return nil, errTooDeep
	}
	return v, nil
}

func depth(v any, d int) int {
	if d > maxJSONDepth {
		return d
	}
	m := d
	switch t := v.(type) {
	case map[string]any:
		for _, c := range t {
			m = max(m, depth(c, d+1))
		}
	case []any:
		for _, c := range t {
			m = max(m, depth(c, d+1))
		}
	}
	return m
}

// jsonDiffer compares two decoded documents.
type jsonDiffer struct {
	cfg    *compiled
	ignore []pattern // global plus route ignores
	out    []Difference
	full   bool // maxDiffs reached
}

// diffJSON returns the differences between prod and cand, in a stable order
// (object keys sorted).
func (c *compiled) diffJSON(prod, cand any, routeIgnore []pattern) (diffs []Difference, truncated bool) {
	d := &jsonDiffer{cfg: c, ignore: append(slices.Clone(c.ignore), routeIgnore...)}
	d.walk(nil, nil, prod, cand)
	return d.out, d.full
}

// walk compares a and b at path. tmpl is path with array indices replaced
// by "*", the key noise statistics are kept under.
func (d *jsonDiffer) walk(path, tmpl []string, a, b any) {
	if d.full || d.ignored(path) {
		return
	}
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok {
			d.add(path, tmpl, KindType, a, b)
			return
		}
		keys := make([]string, 0, len(av)+len(bv))
		for k := range av {
			keys = append(keys, k)
		}
		for k := range bv {
			if _, ok := av[k]; !ok {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		for _, k := range keys {
			p, t := append(slices.Clip(path), k), append(slices.Clip(tmpl), k)
			x, inA := av[k]
			y, inB := bv[k]
			switch {
			case !inB:
				if !d.ignored(p) {
					d.add(p, t, KindRemoved, x, nil)
				}
			case !inA:
				if !d.ignored(p) {
					d.add(p, t, KindAdded, nil, y)
				}
			default:
				d.walk(p, t, x, y)
			}
		}
	case []any:
		bv, ok := b.([]any)
		if !ok {
			d.add(path, tmpl, KindType, a, b)
			return
		}
		switch {
		case d.unordered(path):
			d.multiset(path, tmpl, av, bv)
		case d.keyField(path) != "":
			d.keyed(path, tmpl, av, bv, d.keyField(path))
		default:
			d.indexed(path, tmpl, av, bv)
		}
	case json.Number:
		bv, ok := b.(json.Number)
		if !ok {
			d.add(path, tmpl, KindType, a, b)
			return
		}
		if !d.numbersEqual(av, bv) {
			d.add(path, tmpl, KindChanged, a, b)
		}
	default: // string, bool, nil
		if typeName(a) != typeName(b) {
			d.add(path, tmpl, KindType, a, b)
			return
		}
		if a != b {
			d.add(path, tmpl, KindChanged, a, b)
		}
	}
}

func (d *jsonDiffer) indexed(path, tmpl []string, a, b []any) {
	t := append(slices.Clip(tmpl), "*")
	for i := range max(len(a), len(b)) {
		p := append(slices.Clip(path), strconv.Itoa(i))
		switch {
		case i >= len(b):
			if !d.ignored(p) {
				d.add(p, t, KindRemoved, a[i], nil)
			}
		case i >= len(a):
			if !d.ignored(p) {
				d.add(p, t, KindAdded, nil, b[i])
			}
		default:
			d.walk(p, t, a[i], b[i])
		}
	}
}

// keyed matches elements by the value of field; elements without it fall
// back to their index.
func (d *jsonDiffer) keyed(path, tmpl []string, a, b []any, field string) {
	t := append(slices.Clip(tmpl), "*")
	key := func(v any) (string, bool) {
		m, ok := v.(map[string]any)
		if !ok {
			return "", false
		}
		k, ok := m[field]
		if !ok {
			return "", false
		}
		return compact(k), true
	}
	inB := map[string]int{}
	for j, v := range b {
		if k, ok := key(v); ok {
			if _, dup := inB[k]; !dup {
				inB[k] = j
			}
		}
	}
	matched := make([]bool, len(b))
	for i, v := range a {
		p := append(slices.Clip(path), strconv.Itoa(i))
		k, ok := key(v)
		j, found := inB[k]
		if !ok {
			j, found = i, i < len(b)
		}
		if !found || matched[j] {
			if !d.ignored(p) {
				d.add(p, t, KindRemoved, v, nil)
			}
			continue
		}
		matched[j] = true
		d.walk(p, t, v, b[j])
	}
	for j, v := range b {
		if !matched[j] {
			p := append(slices.Clip(path), strconv.Itoa(j))
			if !d.ignored(p) {
				d.add(p, t, KindAdded, nil, v)
			}
		}
	}
}

// multiset compares arrays ignoring order.
func (d *jsonDiffer) multiset(path, tmpl []string, a, b []any) {
	t := append(slices.Clip(tmpl), "*")
	count := map[string][]int{} // canonical element -> indexes in b not yet matched
	for j, v := range b {
		k := compact(v)
		count[k] = append(count[k], j)
	}
	for i, v := range a {
		k := compact(v)
		if js := count[k]; len(js) > 0 {
			count[k] = js[1:]
			continue
		}
		if p := append(slices.Clip(path), strconv.Itoa(i)); !d.ignored(p) {
			d.add(p, t, KindRemoved, v, nil)
		}
	}
	var left []int
	for _, js := range count {
		left = append(left, js...)
	}
	slices.Sort(left)
	for _, j := range left {
		if p := append(slices.Clip(path), strconv.Itoa(j)); !d.ignored(p) {
			d.add(p, t, KindAdded, nil, b[j])
		}
	}
}

func (d *jsonDiffer) numbersEqual(a, b json.Number) bool {
	if a == b {
		return true
	}
	x, err1 := strconv.ParseFloat(string(a), 64)
	y, err2 := strconv.ParseFloat(string(b), 64)
	if err1 != nil || err2 != nil {
		return false
	}
	if x == y {
		return true // 1.0 and 1, 1e2 and 100
	}
	tol := d.cfg.tolerance
	return tol > 0 && math.Abs(x-y) <= tol*math.Max(math.Abs(x), math.Abs(y))
}

func (d *jsonDiffer) ignored(path []string) bool {
	for _, p := range d.ignore {
		if p.match(path) {
			return true
		}
	}
	return false
}

func (d *jsonDiffer) unordered(path []string) bool {
	for _, p := range d.cfg.unordered {
		if p.match(path) {
			return true
		}
	}
	return false
}

func (d *jsonDiffer) keyField(path []string) string {
	for _, k := range d.cfg.arrayKeys {
		if k.p.match(path) {
			return k.field
		}
	}
	return ""
}

func (d *jsonDiffer) add(path, tmpl []string, kind string, a, b any) {
	if len(d.out) >= d.cfg.maxDiffs {
		d.full = true
		return
	}
	diff := Difference{Where: WhereBody, Pointer: pointer(path), Kind: kind, template: pointer(tmpl)}
	if kind != KindAdded {
		diff.Prod = clip(compact(a))
	}
	if kind != KindRemoved {
		diff.Candidate = clip(compact(b))
	}
	d.out = append(d.out, diff)
}

func pointer(segs []string) string {
	var b []byte
	for _, s := range segs {
		b = append(b, '/')
		b = append(b, escapeSegment(s)...)
	}
	return string(b)
}

func typeName(v any) string {
	switch v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
	case string:
		return "string"
	case json.Number:
		return "number"
	case []any:
		return "array"
	case map[string]any:
		return "object"
	}
	return "unknown"
}

// compact renders v as compact JSON without HTML escaping; object keys come
// out sorted, so equal values render equally.
func compact(v any) string {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return "?"
	}
	return string(bytes.TrimSuffix(buf.Bytes(), []byte("\n")))
}

// clip shortens s to DefaultMaxValueBytes on a rune boundary.
func clip(s string) string {
	if len(s) <= DefaultMaxValueBytes {
		return s
	}
	cut := DefaultMaxValueBytes
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
