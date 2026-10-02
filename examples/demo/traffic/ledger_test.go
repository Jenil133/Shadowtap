package traffic

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
)

func TestLedgerAdd(t *testing.T) {
	var l Ledger // the zero value is usable
	l.Add("short", "exactly6", "rosa.novak3@example.com", "rosa.novak3@example.com", "", "123", "line-one\nline-two\r\n", "a\nb")
	want := []string{"exactly6", "line-one", "line-two", "rosa.novak3@example.com"}
	if got := l.Values(); !slices.Equal(got, want) {
		t.Errorf("Values() = %q, want %q", got, want)
	}
	if l.Len() != len(want) {
		t.Errorf("Len() = %d, want %d", l.Len(), len(want))
	}
	if !l.Contains("exactly6") || l.Contains("short") || l.Contains("line-one\nline-two") {
		t.Error("Contains disagrees with Values")
	}
}

func TestLedgerNil(t *testing.T) {
	var l *Ledger
	l.Add("rosa.novak3@example.com")
	if l.Len() != 0 || l.Values() != nil || l.Contains("rosa.novak3@example.com") {
		t.Error("nil ledger recorded a value")
	}
}

func TestLedgerConcurrentAdd(t *testing.T) {
	l := NewLedger()
	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 100 {
				l.Add(fmt.Sprintf("value-%d", i), fmt.Sprintf("worker-%d-value-%d", w, i))
				l.Contains("value-0")
			}
		})
	}
	wg.Wait()
	if got, want := l.Len(), 100+8*100; got != want {
		t.Errorf("Len() = %d, want %d", got, want)
	}
}

func TestLedgerWriteFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pii.txt")
	if err := os.WriteFile(path, []byte("stale\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	l := NewLedger()
	l.Add("+1 415 555 0199", "rosa.novak3@example.com", "4111 1111 1111 1111")
	if err := l.WriteFile(path); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := strings.Join(l.Values(), "\n") + "\n"; string(data) != want {
		t.Errorf("file = %q, want %q", data, want)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("file mode = %v, want 0600", perm)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Errorf("temporary files left behind: %v", entries)
	}

	if err := l.WriteFile(filepath.Join(dir, "missing", "pii.txt")); err == nil {
		t.Error("WriteFile into a missing directory succeeded")
	}
}
