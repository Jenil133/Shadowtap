package traffic

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

// MinLedgerValueLen is the shortest value a Ledger keeps. Shorter strings,
// such as CVVs and ZIP codes, match too much unrelated text to be useful in
// substring leak checks.
const MinLedgerValueLen = 6

// Ledger is a set of raw personal-data strings exactly as they were sent, for
// leak checks that search stored captures for any of them. It is safe for
// concurrent use; the zero value is an empty ledger and a nil *Ledger ignores
// additions.
type Ledger struct {
	mu     sync.Mutex
	values map[string]struct{}
}

// NewLedger returns an empty ledger.
func NewLedger() *Ledger { return &Ledger{} }

// Add records values, skipping those shorter than MinLedgerValueLen. A value
// spanning several lines is recorded line by line, which keeps the one-per-
// line file format lossless: a leak of the whole value contains every line.
func (l *Ledger) Add(values ...string) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, v := range values {
		for _, line := range strings.FieldsFunc(v, isLineBreak) {
			if utf8.RuneCountInString(line) < MinLedgerValueLen {
				continue
			}
			if l.values == nil {
				l.values = make(map[string]struct{})
			}
			l.values[line] = struct{}{}
		}
	}
}

func isLineBreak(r rune) bool { return r == '\n' || r == '\r' }

// Contains reports whether v was recorded.
func (l *Ledger) Contains(v string) bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.values[v]
	return ok
}

// Len returns the number of distinct values recorded.
func (l *Ledger) Len() int {
	if l == nil {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.values)
}

// Values returns the recorded values in sorted order.
func (l *Ledger) Values() []string {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	values := make([]string, 0, len(l.values))
	for v := range l.values {
		values = append(values, v)
	}
	l.mu.Unlock()
	slices.Sort(values)
	return values
}

// WriteFile writes the sorted values to path, one per line, readable only by
// the owner (the values are personal data, even if fake). The file is
// replaced atomically, so readers never see a partial ledger.
func (l *Ledger) WriteFile(path string) (err error) {
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("traffic: write ledger: %w", err)
	}
	defer func() {
		if err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			err = fmt.Errorf("traffic: write ledger %s: %w", path, err)
		}
	}()
	w := bufio.NewWriter(tmp)
	for _, v := range l.Values() {
		w.WriteString(v)
		w.WriteByte('\n')
	}
	if err := w.Flush(); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// addUser records the personal data of a user the API returns.
func (l *Ledger) addUser(u api.User) {
	l.Add(u.FullName, u.Email, u.Phone, u.SSN, u.DOB)
	if u.Address != nil {
		l.Add(u.Address.Street)
	}
}
