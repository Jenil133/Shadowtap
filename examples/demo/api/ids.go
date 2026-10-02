package api

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
)

// idHexBytes is how many hash bytes an id keeps: 12 hex digits.
const idHexBytes = 6

// UserID returns the id the API assigns to the user with this email: "u_"
// followed by the first 12 hex digits of the SHA-256 of the trimmed,
// lowercased email.
//
// Ids depend only on request content, so a replayed request recreates the
// same id and clients can predict ids without reading responses.
func UserID(email string) string {
	sum := sha256.Sum256([]byte(normalizeEmail(email)))
	return "u_" + hex.EncodeToString(sum[:idHexBytes])
}

// OrderID returns the id of a user's seq-th order (counting from 1) for these
// items: "o_" followed by the first 12 hex digits of the SHA-256 of the user
// id, the canonical item list and seq, separated by NUL bytes. The canonical
// list is sorted, so the order of the lines does not change the id.
func OrderID(userID string, items []Item, seq int) string {
	h := sha256.New()
	h.Write([]byte(userID))
	h.Write([]byte{0})
	h.Write(canonicalItems(items))
	h.Write([]byte{0})
	h.Write(strconv.AppendInt(nil, int64(seq), 10))
	return "o_" + hex.EncodeToString(h.Sum(nil)[:idHexBytes])
}

// canonicalItems renders items as "sku:qty:price" lines joined by ';', sorted
// by SKU, then quantity, then price.
func canonicalItems(items []Item) []byte {
	sorted := slices.Clone(items)
	slices.SortFunc(sorted, func(a, b Item) int {
		return cmp.Or(
			strings.Compare(a.SKU, b.SKU),
			cmp.Compare(a.Qty, b.Qty),
			cmp.Compare(a.PriceCents, b.PriceCents),
		)
	})
	var b []byte
	for i, it := range sorted {
		if i > 0 {
			b = append(b, ';')
		}
		b = append(b, it.SKU...)
		b = append(b, ':')
		b = strconv.AppendInt(b, int64(it.Qty), 10)
		b = append(b, ':')
		b = strconv.AppendInt(b, it.PriceCents, 10)
	}
	return b
}

func normalizeEmail(email string) string {
	return strings.ToLower(strings.TrimSpace(email))
}
