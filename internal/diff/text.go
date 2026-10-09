package diff

import (
	"fmt"
	"strings"
)

// maxEditDistance caps the Myers search: bodies further apart are reported
// as entirely different rather than diffed line by line.
const maxEditDistance = 1000

// maxTextLines bounds the lines diffed per side.
const maxTextLines = 20_000

// diffText compares two text bodies line by line and summarizes the
// difference as one Difference: how many lines were removed and added, and
// the first line that differs on each side.
func diffText(prod, cand string) []Difference {
	if prod == cand {
		return nil
	}
	a, b := splitLines(prod), splitLines(cand)
	// Common prefix and suffix are skipped before the search.
	start := 0
	for start < len(a) && start < len(b) && a[start] == b[start] {
		start++
	}
	ea, eb := len(a), len(b)
	for ea > start && eb > start && a[ea-1] == b[eb-1] {
		ea--
		eb--
	}
	removed, added := ea-start, eb-start
	if len(a) <= maxTextLines && len(b) <= maxTextLines {
		if d, ok := myers(a[start:ea], b[start:eb], maxEditDistance); ok {
			// d = removed + added for the shortest edit script.
			common := (removed + added - d) / 2
			removed, added = removed-common, added-common
		}
	}
	diff := Difference{
		Where:   WhereBody,
		Kind:    KindChanged,
		Pointer: fmt.Sprintf("line %d", start+1),
		Detail:  fmt.Sprintf("-%d +%d lines", removed, added),
	}
	diff.template = "line"
	if start < len(a) {
		diff.Prod = clip(a[start])
	}
	if start < len(b) {
		diff.Candidate = clip(b[start])
	}
	return []Difference{diff}
}

func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// myers returns the length of the shortest edit script (insertions plus
// deletions) between a and b, or false when it exceeds limit.
func myers(a, b []string, limit int) (int, bool) {
	n, m := len(a), len(b)
	maxD := min(n+m, limit)
	off := maxD + 1
	v := make([]int, 2*maxD+3)
	for d := 0; d <= maxD; d++ {
		for k := -d; k <= d; k += 2 {
			var x int
			if k == -d || (k != d && v[off+k-1] < v[off+k+1]) {
				x = v[off+k+1] // down: insertion
			} else {
				x = v[off+k-1] + 1 // right: deletion
			}
			y := x - k
			for x < n && y < m && a[x] == b[y] {
				x++
				y++
			}
			v[off+k] = x
			if x >= n && y >= m {
				return d, true
			}
		}
	}
	return 0, false
}
