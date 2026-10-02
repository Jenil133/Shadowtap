package traffic

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"

	"github.com/Jenil133/Shadowtap/examples/demo/api"
)

// Op names the API operation a generated request performs.
type Op string

// Operations, from most to least frequent.
const (
	OpCreateUser  Op = "create_user"  // POST /api/users
	OpCreateOrder Op = "create_order" // POST /api/orders, card number in the body
	OpGetUser     Op = "get_user"     // GET /api/users/{id}
	OpSearchUsers Op = "search_users" // GET /api/users?email=, email in the query
	OpLookupUser  Op = "lookup_user"  // GET /api/users/lookup/{email}, email in the path
	OpGetOrder    Op = "get_order"    // GET /api/orders/{id}
	OpLogin       Op = "login"        // POST /api/login
	OpListUsers   Op = "list_users"   // GET /api/users?limit=, large enough to be gzipped
)

// opWeights is the request mix, in percent.
var opWeights = [...]struct {
	op     Op
	weight int
}{
	{OpCreateUser, 20}, {OpCreateOrder, 18}, {OpGetUser, 14}, {OpSearchUsers, 12},
	{OpLookupUser, 12}, {OpGetOrder, 10}, {OpLogin, 10}, {OpListUsers, 4},
}

// Probabilities of request variations.
const (
	traceparentRate      = 0.90 // W3C traceparent header
	bearerRate           = 0.30 // Authorization: Bearer <JWT-shaped token>
	cookieRate           = 0.30 // Cookie: session=...
	userEmailHeaderRate  = 0.20 // X-User-Email
	ipv6ClientRate       = 0.15 // client address in 2001:db8::/32 rather than 203.0.113.0/24
	proxyHopRate         = 0.40 // second X-Forwarded-For entry from 198.51.100.0/24
	duplicateUserRate    = 0.04 // re-register an existing email: 409
	unknownIDRate        = 0.05 // get a user or order that does not exist: 404
	uncreatedPersonRate  = 0.10 // search or look up someone not registered yet
	mixedCaseSearchRate  = 0.20 // search with a differently cased email
	invalidOrderRate     = 0.05 // order with a zero quantity: 422
	unknownUserOrderRate = 0.03 // order for a user that does not exist: 404
	wrongPasswordRate    = 0.15 // login with a wrong password: 401
)

const (
	userAgent = "shadowtap-demo-traffic/1"
	// jwtHeader is base64url({"alg":"HS256","typ":"JWT"}).
	jwtHeader = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"
	// generatorStream separates the generator's random stream from people's.
	generatorStream = 0x67656e657261746f // "generato"
	// settleWindow is how many of the newest users and orders reads avoid
	// while older ones exist: with concurrent senders, a create generated
	// moments ago may not have reached the API yet.
	settleWindow = 16
)

// acceptEncodings are sampled uniformly; "" sends no Accept-Encoding header.
var acceptEncodings = [...]string{"gzip", "gzip", "gzip, deflate, br", "br;q=1.0, gzip;q=0.8", "identity", ""}

// catalog is what generated orders buy.
var catalog = [...]struct {
	sku   string
	price int64
}{
	{"BOOK-1001", 1299}, {"MUG-2002", 899}, {"TSHIRT-3003", 1999}, {"CABLE-4004", 1499},
	{"LAMP-5005", 3499}, {"PEN-6006", 199}, {"BAG-7007", 4599}, {"SOCKS-8008", 999},
}

// Expectation is what the generator's client-side model predicts for a
// request it generated.
type Expectation struct {
	Op Op
	// Status is what a fresh API answers when requests are served one at a
	// time in generation order. Concurrent senders may see a few differences,
	// such as a lookup overtaking the create it depends on.
	Status int
}

type expectationKey struct{}

// ExpectationOf returns the expectation Next attached to r. It lives in r's
// context, so it survives contexts derived from r.Context().
func ExpectationOf(r *http.Request) (Expectation, bool) {
	e, ok := r.Context().Value(expectationKey{}).(Expectation)
	return e, ok
}

// Generator produces a deterministic, weighted mix of demo API requests. It
// keeps a client-side model of the API so users exist before they are
// referenced and most reads hit, and it records the personal data of every
// request in a Ledger. It is safe for concurrent use. For the same seed,
// people and base URL, the n-th call to Next returns the same request.
//
// The model assumes the API starts empty apart from its seeded users and
// that people are valid for the API, as People's are.
type Generator struct {
	people []Person
	ledger *Ledger

	mu        sync.Mutex
	rng       *rand.Rand
	planned   int             // requests planned so far
	next      int             // people[next:] have never been sent to create-user
	created   []int           // indexes of people the model has registered
	userIDs   map[string]bool // ids of users the model believes exist
	orderSeq  map[string]int  // user id -> orders the API has numbered
	lastOrder map[string]int  // user id -> planned count at their latest numbered order
	orders    []string        // predicted ids of created orders
}

// NewGenerator returns a generator over people that records personal data in
// ledger, which may be nil. The ledger also receives the personal data of
// the API's seeded users, because list responses carry it.
func NewGenerator(seed int64, people []Person, ledger *Ledger) *Generator {
	g := &Generator{
		people:    slices.Clone(people),
		ledger:    ledger,
		rng:       rand.New(rand.NewPCG(uint64(seed), generatorStream)),
		userIDs:   make(map[string]bool),
		orderSeq:  make(map[string]int),
		lastOrder: make(map[string]int),
	}
	for _, u := range api.SeedUsers() {
		g.userIDs[u.ID] = true
		ledger.addUser(u)
	}
	return g
}

// Next returns the next request for the API at baseURL (scheme, host and an
// optional path prefix). Its context carries an Expectation, see
// ExpectationOf, and its personal data is in the ledger before Next returns.
func (g *Generator) Next(baseURL string) (*http.Request, error) {
	base, err := parseBaseURL(baseURL)
	if err != nil {
		return nil, err
	}
	if len(g.people) == 0 {
		return nil, errors.New("traffic: generator has no people")
	}
	g.mu.Lock()
	p := g.plan()
	g.mu.Unlock()

	req, err := p.request(base)
	if err != nil {
		return nil, err
	}
	g.ledger.Add(p.pii...)
	return req, nil
}

func parseBaseURL(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", fmt.Errorf("traffic: base URL: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("traffic: base URL %q must look like http(s)://host[:port][/prefix]", s)
	}
	return strings.TrimSuffix(u.String(), "/"), nil
}

// requestPlan is a request decided under the generator's lock and built
// after it.
type requestPlan struct {
	expect  Expectation
	method  string
	path    string // escaped
	query   string // raw, without '?'
	body    any    // encoded as JSON when not nil
	headers [][2]string
	pii     []string // raw personal data the request carries
}

func (p *requestPlan) request(base string) (*http.Request, error) {
	target := base + p.path
	if p.query != "" {
		target += "?" + p.query
	}
	var body io.Reader
	if p.body != nil {
		data, err := json.Marshal(p.body)
		if err != nil {
			return nil, fmt.Errorf("traffic: encode %s body: %w", p.expect.Op, err)
		}
		body = bytes.NewReader(data)
	}
	ctx := context.WithValue(context.Background(), expectationKey{}, p.expect)
	req, err := http.NewRequestWithContext(ctx, p.method, target, body)
	if err != nil {
		return nil, fmt.Errorf("traffic: build %s request: %w", p.expect.Op, err)
	}
	for _, h := range p.headers {
		req.Header.Set(h[0], h[1])
	}
	return req, nil
}

// plan draws the next request. It must be called with g.mu held; every
// random draw happens here, in a fixed order, which makes Next deterministic.
func (g *Generator) plan() *requestPlan {
	g.planned++
	op := g.pickOp()
	// Until enough settled state exists, operations that read it create it
	// instead, so early requests hit even with concurrent senders.
	if op == OpGetOrder && len(g.orders) <= settleWindow {
		op = OpCreateOrder
	}
	if op != OpCreateUser && op != OpListUsers && g.warmingUp() {
		op = OpCreateUser
	}

	p := &requestPlan{method: http.MethodGet, expect: Expectation{Op: op, Status: http.StatusOK}}
	switch op {
	case OpCreateUser:
		g.planCreateUser(p)
	case OpCreateOrder:
		g.planCreateOrder(p)
	case OpGetUser:
		g.planGetUser(p)
	case OpSearchUsers:
		g.planSearchUsers(p)
	case OpLookupUser:
		g.planLookupUser(p)
	case OpGetOrder:
		g.planGetOrder(p)
	case OpLogin:
		g.planLogin(p)
	case OpListUsers:
		p.path = "/api/users"
		p.query = fmt.Sprintf("limit=%d", 10+g.rng.IntN(41))
	}
	g.planHeaders(p)
	return p
}

func (g *Generator) pickOp() Op {
	total := 0
	for _, w := range opWeights {
		total += w.weight
	}
	n := g.rng.IntN(total)
	for _, w := range opWeights {
		if n < w.weight {
			return w.op
		}
		n -= w.weight
	}
	return opWeights[0].op
}

func (g *Generator) planCreateUser(p *requestPlan) {
	var idx int
	switch {
	case g.next < len(g.people) && !(len(g.created) > settleWindow && g.chance(duplicateUserRate)):
		idx = g.next
		g.next++
	case len(g.created) > 0:
		idx = g.created[g.rng.IntN(settled(len(g.created)))]
	default:
		idx = g.rng.IntN(len(g.people))
	}
	person := g.people[idx]
	p.expect.Status = http.StatusCreated
	if id := api.UserID(person.Email); g.userIDs[id] {
		p.expect.Status = http.StatusConflict
	} else {
		g.userIDs[id] = true
		g.created = append(g.created, idx)
	}
	p.method, p.path = http.MethodPost, "/api/users"
	p.body = api.CreateUserRequest{
		FullName: person.FullName,
		Email:    person.Email,
		Phone:    person.Phone,
		SSN:      person.SSN,
		DOB:      person.DOB,
		Address:  &api.Address{Street: person.Street, City: person.City, Zip: person.Zip},
		Password: person.Password,
	}
	p.pii = append(p.pii, person.FullName, person.Email, person.Phone, person.SSN, person.DOB, person.Street, person.Password)
}

func (g *Generator) planCreateOrder(p *requestPlan) {
	person := g.pickOrderingPerson()
	userID := api.UserID(person.Email)
	items := g.cart()
	switch {
	case g.chance(invalidOrderRate):
		items[0].Qty = 0
		p.expect.Status = http.StatusUnprocessableEntity
	case g.chance(unknownUserOrderRate):
		userID = g.unknownUserID()
		p.expect.Status = http.StatusNotFound
	default:
		g.orderSeq[userID]++
		g.lastOrder[userID] = g.planned
		g.orders = append(g.orders, api.OrderID(userID, items, g.orderSeq[userID]))
		p.expect.Status = http.StatusCreated
	}
	p.method, p.path = http.MethodPost, "/api/orders"
	p.body = api.CreateOrderRequest{
		UserID:  userID,
		Items:   items,
		Payment: api.PaymentInput{CardNumber: person.Card, CVV: person.CVV, Exp: cardExp(person)},
	}
	p.pii = append(p.pii, person.Card)
}

func (g *Generator) planGetUser(p *requestPlan) {
	if g.chance(unknownIDRate) {
		p.path = "/api/users/" + g.unknownUserID()
		p.expect.Status = http.StatusNotFound
		return
	}
	p.path = "/api/users/" + api.UserID(g.pickCreated().Email)
}

func (g *Generator) planSearchUsers(p *requestPlan) {
	email := g.pickPerson(uncreatedPersonRate).Email
	if g.chance(mixedCaseSearchRate) {
		email = g.mixCase(email)
	}
	p.path = "/api/users"
	// '@' is legal in a query, and leaving it unescaped keeps the address
	// verbatim on the wire, where leak checks look for it.
	p.query = "email=" + strings.ReplaceAll(url.QueryEscape(email), "%40", "@")
	p.pii = append(p.pii, email)
}

func (g *Generator) planLookupUser(p *requestPlan) {
	email := g.pickPerson(uncreatedPersonRate).Email
	p.path = "/api/users/lookup/" + url.PathEscape(email)
	if !g.userIDs[api.UserID(email)] {
		p.expect.Status = http.StatusNotFound
	}
	p.pii = append(p.pii, email)
}

func (g *Generator) planGetOrder(p *requestPlan) {
	if g.chance(unknownIDRate) {
		id := "o_" + g.hex(12)
		for slices.Contains(g.orders, id) {
			id = "o_" + g.hex(12)
		}
		p.path = "/api/orders/" + id
		p.expect.Status = http.StatusNotFound
		return
	}
	p.path = "/api/orders/" + g.orders[g.rng.IntN(settled(len(g.orders)))]
}

func (g *Generator) planLogin(p *requestPlan) {
	person := g.pickCreated()
	password := person.Password
	if person.Password == "" || g.chance(wrongPasswordRate) {
		password = randomPassword(g.rng, 12)
		p.expect.Status = http.StatusUnauthorized
	}
	p.method, p.path = http.MethodPost, "/api/login"
	p.body = api.LoginRequest{Email: person.Email, Password: password}
	p.pii = append(p.pii, person.Email, password)
}

func (g *Generator) planHeaders(p *requestPlan) {
	p.headers = append(p.headers, [2]string{"Accept", "application/json"}, [2]string{"User-Agent", userAgent})
	if ae := pick(g.rng, acceptEncodings[:]); ae != "" {
		p.headers = append(p.headers, [2]string{"Accept-Encoding", ae})
	}
	if p.body != nil {
		p.headers = append(p.headers, [2]string{"Content-Type", "application/json"})
	}
	if g.chance(traceparentRate) {
		p.headers = append(p.headers, [2]string{"Traceparent", "00-" + g.nonZeroHex(32) + "-" + g.nonZeroHex(16) + "-01"})
	}
	ips := g.clientIPs()
	p.headers = append(p.headers, [2]string{"X-Forwarded-For", strings.Join(ips, ", ")})
	p.pii = append(p.pii, ips...)
	if g.chance(bearerRate) {
		token := g.bearerToken()
		p.headers = append(p.headers, [2]string{"Authorization", "Bearer " + token})
		p.pii = append(p.pii, token)
	}
	if g.chance(cookieRate) {
		session := g.hex(32)
		p.headers = append(p.headers, [2]string{"Cookie", "session=" + session})
		p.pii = append(p.pii, session)
	}
	if len(g.created) > 0 && g.chance(userEmailHeaderRate) {
		email := g.pickCreated().Email
		p.headers = append(p.headers, [2]string{"X-User-Email", email})
		p.pii = append(p.pii, email)
	}
}

// clientIPs returns a client address and sometimes a proxy hop, all from
// documentation prefixes.
func (g *Generator) clientIPs() []string {
	client := fmt.Sprintf("203.0.113.%d", 1+g.rng.IntN(254))
	if g.chance(ipv6ClientRate) {
		client = fmt.Sprintf("2001:db8:%x:%x::%x", g.rng.IntN(0x10000), g.rng.IntN(0x10000), 1+g.rng.IntN(0xffff))
	}
	ips := []string{client}
	if g.chance(proxyHopRate) {
		ips = append(ips, fmt.Sprintf("198.51.100.%d", 1+g.rng.IntN(254)))
	}
	return ips
}

// bearerToken returns a client-made JWT-shaped token: base64url JSON header
// and claims and a random signature. The API ignores it; scrubbers must not.
func (g *Generator) bearerToken() string {
	sub := api.UserID(g.pickCreated().Email)
	iat := 1_700_000_000 + g.rng.Int64N(100_000_000)
	claims := fmt.Sprintf(`{"sub":%q,"iat":%d,"exp":%d}`, sub, iat, iat+3600)
	sig := make([]byte, 32)
	for i := range sig {
		sig[i] = byte(g.rng.Uint32())
	}
	return jwtHeader + "." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + "." + base64.RawURLEncoding.EncodeToString(sig)
}

// cart returns one to three distinct catalog items.
func (g *Generator) cart() []api.Item {
	n := 1 + g.rng.IntN(3)
	items := make([]api.Item, n)
	for i, j := range g.rng.Perm(len(catalog))[:n] {
		items[i] = api.Item{SKU: catalog[j].sku, Qty: 1 + g.rng.IntN(5), PriceCents: catalog[j].price}
	}
	return items
}

// warmingUp reports whether reads should wait for more registrations: no one
// is registered yet, or everyone registered is still within the settle
// window and unregistered people remain.
func (g *Generator) warmingUp() bool {
	return len(g.created) == 0 || len(g.created) <= settleWindow && g.next < len(g.people)
}

// pickCreated returns a registered person, preferring settled ones.
func (g *Generator) pickCreated() Person {
	return g.people[g.created[g.rng.IntN(settled(len(g.created)))]]
}

// pickOrderingPerson returns a registered person for a new order, preferring
// one without a recent order: the API numbers a user's orders in arrival
// order, so two in flight at once could swap their predicted ids.
func (g *Generator) pickOrderingPerson() Person {
	var person Person
	for range 8 {
		person = g.pickCreated()
		last, ok := g.lastOrder[api.UserID(person.Email)]
		if !ok || g.planned-last > settleWindow {
			break
		}
	}
	return person
}

// settled returns how many of the n oldest items reads choose from: all but
// the newest settleWindow, or all n while there are not enough to spare.
func settled(n int) int {
	if n > settleWindow {
		return n - settleWindow
	}
	return n
}

// pickPerson returns, with probability rate, someone not registered yet (so
// lookups miss), and otherwise someone registered.
func (g *Generator) pickPerson(rate float64) Person {
	if g.next < len(g.people) && g.chance(rate) {
		return g.people[g.next+g.rng.IntN(min(len(g.people)-g.next, 16))]
	}
	return g.pickCreated()
}

func (g *Generator) unknownUserID() string {
	for {
		if id := "u_" + g.hex(12); !g.userIDs[id] {
			return id
		}
	}
}

// mixCase upper-cases a random subset of letters; the API matches emails
// case-insensitively.
func (g *Generator) mixCase(s string) string {
	b := []byte(s)
	for i, c := range b {
		if 'a' <= c && c <= 'z' && g.rng.IntN(2) == 0 {
			b[i] = c - 'a' + 'A'
		}
	}
	return string(b)
}

func (g *Generator) chance(p float64) bool { return g.rng.Float64() < p }

func (g *Generator) hex(n int) string {
	const digits = "0123456789abcdef"
	b := make([]byte, n)
	for i := range b {
		b[i] = digits[g.rng.IntN(len(digits))]
	}
	return string(b)
}

// nonZeroHex returns n hex digits that are not all zero, as trace context ids
// must be.
func (g *Generator) nonZeroHex(n int) string {
	for {
		if s := g.hex(n); strings.Trim(s, "0") != "" {
			return s
		}
	}
}

// cardExp derives a stable MM/YY expiry for a person's card.
func cardExp(p Person) string {
	h := fnv.New32a()
	h.Write([]byte(p.Email))
	h.Write([]byte(p.Card))
	v := h.Sum32()
	return fmt.Sprintf("%02d/%02d", 1+v%12, 27+(v/12)%9)
}
