package tap

import (
	"fmt"
	"strings"
	"sync"
	"testing"
)

func TestServiceFromAuthority(t *testing.T) {
	tests := []struct {
		authority string
		want      string
	}{
		{"demo-api", "demo-api"},
		{"demo-api:8080", "demo-api"},
		{"demo-api.shop.svc", "demo-api"},
		{"demo-api.shop.svc:80", "demo-api"},
		{"demo-api.shop.svc.cluster.local", "demo-api"},
		{"demo-api.shop.svc.cluster.local:8080", "demo-api"},
		{"demo-api.shop.svc.cluster.local.", "demo-api"},
		{"DEMO-API.Shop.SVC.Cluster.Local:8080", "demo-api"},
		{"demo-api.shop", "demo-api.shop"},
		{"api.example.com:443", "api.example.com"},
		{"10.1.2.3:8080", "10.1.2.3"},
		{"user:pass@demo-api:8080", "demo-api"},
		{"[::1]:8080", ""},
		{"[2001:db8::1]", ""},
		{"[::1", ""},
		{"::1", ""},
		{"", ""},
		{":8080", ""},
		{"under_score", ""},
		{"evil<script>", ""},
		{"spa ce", ""},
		{"ünïcode.example", ""},
		{strings.Repeat("a", 63), strings.Repeat("a", 63)},
		{strings.Repeat("a", 64), ""},
		{strings.Repeat("a", 63) + ".shop.svc.cluster.local", strings.Repeat("a", 63)},
	}
	for _, tt := range tests {
		if got := serviceFromAuthority(tt.authority); got != tt.want {
			t.Errorf("serviceFromAuthority(%q) = %q, want %q", tt.authority, got, tt.want)
		}
	}
}

func TestServiceNameFallbacks(t *testing.T) {
	tests := []struct {
		metadata, authority, want string
	}{
		{"checkout", "demo-api:80", "checkout"},
		{"Checkout_Service", "", "Checkout_Service"}, // metadata comes from trusted config
		{"", "demo-api:80", "demo-api"},
		{"", "bad host", unknownService},
		{"", "", unknownService},
	}
	for _, tt := range tests {
		if got := serviceName(tt.metadata, tt.authority); got != tt.want {
			t.Errorf("serviceName(%q, %q) = %q, want %q", tt.metadata, tt.authority, got, tt.want)
		}
	}
}

func TestServiceLabels(t *testing.T) {
	l := newServiceLabels(3)
	for _, tc := range []struct{ in, want string }{
		{"a", "a"}, {"b", "b"}, {"a", "a"}, {unknownService, unknownService},
		{"c", "c"}, {"d", otherService}, {"b", "b"}, {"e", otherService}, {unknownService, unknownService},
	} {
		if got := l.label(tc.in); got != tc.want {
			t.Errorf("label(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestServiceLabelsConcurrent(t *testing.T) {
	const limit, goroutines, names = 10, 16, 40
	l := newServiceLabels(limit)
	results := make([]map[string]string, goroutines)
	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			got := map[string]string{}
			for i := range names {
				name := fmt.Sprintf("svc-%d", (i+g)%names)
				got[name] = l.label(name)
			}
			results[g] = got
		})
	}
	wg.Wait()
	admitted := map[string]bool{}
	for _, got := range results {
		for name, lbl := range got {
			if lbl != otherService {
				if lbl != name {
					t.Fatalf("label(%q) = %q", name, lbl)
				}
				admitted[name] = true
			}
		}
	}
	if len(admitted) != limit {
		t.Errorf("%d names admitted, want %d", len(admitted), limit)
	}
	// Every goroutine agrees on which names were admitted.
	for g, got := range results {
		for name, lbl := range got {
			if lbl == otherService && admitted[name] {
				t.Errorf("goroutine %d labelled admitted %q as other", g, name)
			}
		}
	}
	for name := range admitted {
		if got := l.label(name); got != name {
			t.Errorf("admitted %q now labelled %q", name, got)
		}
	}
}
