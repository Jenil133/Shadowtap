package scrub

import (
	"fmt"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// benchText is about 1 KB of support-ticket prose with a handful of
// personal values, the shape of a typical free-text field.
func benchText() string {
	t := `Hi team, customer Alice Johnson (alice.johnson@example.com) called on 2026-10-03 about order ` +
		`ORD-48213-7731. She was charged twice for $1,299.99 on her card 4111 1111 1111 1111 and wants a ` +
		`refund to the original payment method. Her callback number is (415) 555-2671; she is available ` +
		`after 3pm Pacific. The request came from 203.0.113.7 using Chrome 120.0.6099.109 on macOS 14.6. ` +
		`Previous tickets: #88123, #88321 (both closed). Account created 2021-04-17, plan "business-annual", ` +
		`seats 25, region us-west-2. Notes from tier 1: verified identity with the last four digits and the ` +
		`billing zip code. No chargeback has been filed yet. Escalating to billing because the duplicate ` +
		`charge did not auto-reverse after 72 hours, which usually indicates a gateway timeout during the ` +
		`capture step. Please confirm the refund and reply to the customer within one business day, cc ` +
		`billing-escalations. Thanks, Sam (support engineer, shift B).`
	return t + strings.Repeat(" ", max(0, 1024-len(t)))
}

// benchJSON is about 10 KB of a paginated API response: user records with
// personal fields mixed with ids, timestamps, flags and nested objects.
func benchJSON() []byte {
	var b strings.Builder
	b.WriteString(`{"data":[`)
	for i := range 22 {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"usr_%06d","email":"user%d@example.com","firstName":"Alex","lastName":"Doe",`+
			`"phone":"+1 415 555 %04d","createdAt":"2026-10-03T12:%02d:00Z","plan":"pro","seats":%d,`+
			`"address":{"street":"%d Main St","city":"Springfield","zip":"9410%d","country":"US"},`+
			`"preferences":{"newsletter":true,"theme":"dark","locale":"en-US","timezone":"America/Los_Angeles"},`+
			`"stats":{"logins":%d,"lastLoginAt":1759449600%03d,"score":0.%d7},"note":"VIP since 2021, prefers email contact"}`,
			i, i, 2600+i, i, 5+i, 100+i, i%10, 40+i, i, i)
	}
	b.WriteString(`],"page":{"next":"eyJvZmZzZXQiOjIwfQ","size":20,"total":1834},"requestId":"7d5c8a2e-0e3f-4c55-9a51-3f1f0f3b1a10"}`)
	return []byte(b.String())
}

func BenchmarkText(b *testing.B) {
	s := newTestScrubber(b)
	in := benchText()
	b.SetBytes(int64(len(in)))
	b.ReportAllocs()
	for b.Loop() {
		s.Text(in, nil)
	}
}

func BenchmarkJSON(b *testing.B) {
	s := newTestScrubber(b)
	in := BodyInput{Data: benchJSON(), ContentType: "application/json"}
	b.SetBytes(int64(len(in.Data)))
	b.ReportAllocs()
	for b.Loop() {
		if res := s.Body(in, nil); res.Omitted != "" {
			b.Fatal(res.Omitted)
		}
	}
}

// BenchmarkExchange measures what the tap pays per captured request.
func BenchmarkExchange(b *testing.B) {
	s := newTestScrubber(b)
	x, _, _ := exchangeFixture(b)
	raw, err := capture.Encode(x)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		x, err := capture.Decode(raw)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		s.Exchange(x)
	}
}
