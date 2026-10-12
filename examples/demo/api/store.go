package api

import (
	"errors"
	"slices"
	"strings"
	"sync"
)

var (
	errEmailTaken   = errors.New("email already registered")
	errUserNotFound = errors.New("user not found")
)

const orderStatusConfirmed = "confirmed"

// userRecord is a stored user. Records are never modified after insertion, so
// readers may share the User (and its Address) without copying.
type userRecord struct {
	user     User
	email    string // normalized; guards lookups against id hash collisions
	password passwordHash
}

// store is the API's in-memory database. Stored users and orders are
// immutable; mu guards the maps and the per-user order counters.
type store struct {
	mu       sync.RWMutex
	users    map[string]*userRecord // by user id
	orders   map[string]*Order      // by order id
	orderSeq map[string]int         // user id -> orders created so far
}

func newStore() *store {
	return &store{
		users:    make(map[string]*userRecord),
		orders:   make(map[string]*Order),
		orderSeq: make(map[string]int),
	}
}

// newUserRecord builds the record for a validated, normalized request.
func newUserRecord(req *CreateUserRequest, password passwordHash) *userRecord {
	return &userRecord{user: newUser(req), email: normalizeEmail(req.Email), password: password}
}

func newUser(req *CreateUserRequest) User {
	id := UserID(req.Email)
	return User{
		ID:       id,
		FullName: req.FullName,
		Email:    req.Email,
		Phone:    req.Phone,
		SSN:      req.SSN,
		DOB:      req.DOB,
		Address:  req.Address,
		Status:   "active",
		Tier:     "standard",
		Preferences: Preferences{
			Locale:   "en-US",
			Timezone: "UTC",
			Currency: "USD",
		},
		Links: Links{Self: "/api/users/" + id},
	}
}

func (s *store) hasUser(id string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.users[id]
	return ok
}

// addUser stores rec unless a user with the same id (that is, the same
// normalized email) exists.
func (s *store) addUser(rec *userRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.users[rec.user.ID]; ok {
		return errEmailTaken
	}
	s.users[rec.user.ID] = rec
	return nil
}

func (s *store) user(id string) (User, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rec, ok := s.users[id]
	if !ok {
		return User{}, false
	}
	return rec.user, true
}

func (s *store) userByEmail(email string) (*userRecord, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.userByEmailLocked(email)
}

func (s *store) userByEmailLocked(email string) (*userRecord, bool) {
	rec, ok := s.users[UserID(email)]
	if !ok || rec.email != normalizeEmail(email) {
		return nil, false
	}
	return rec, true
}

// listUsers returns users filtered by email (case-insensitive, exact) when
// email is not empty, sorted by id (descending when desc is set) and cut to
// limit when limit is positive. total is the number of matches before the
// cut.
func (s *store) listUsers(email string, limit int, desc bool) (users []User, total int) {
	s.mu.RLock()
	if email != "" {
		if rec, ok := s.userByEmailLocked(email); ok {
			users = append(users, rec.user)
		}
	} else {
		users = make([]User, 0, len(s.users))
		for _, rec := range s.users {
			users = append(users, rec.user)
		}
	}
	s.mu.RUnlock()

	slices.SortFunc(users, func(a, b User) int {
		if desc {
			return strings.Compare(b.ID, a.ID)
		}
		return strings.Compare(a.ID, b.ID)
	})
	total = len(users)
	if limit > 0 && limit < total {
		users = users[:limit]
	}
	if users == nil {
		users = []User{} // encode as [] rather than null
	}
	return users, total
}

// addOrder stores an order for a validated, normalized request. The order id
// uses the user's next sequence number, so identical carts get distinct ids.
// addOrder stores an order for a user the caller has already resolved (the
// users service may live in another process); billingEmail is that user's
// email.
func (s *store) addOrder(req *CreateOrderRequest, billingEmail string) (*Order, error) {
	var total int64
	for _, it := range req.Items {
		total += int64(it.Qty) * it.PriceCents
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.orderSeq[req.UserID] + 1
	o := &Order{
		ID:         OrderID(req.UserID, req.Items, seq),
		UserID:     req.UserID,
		Items:      slices.Clone(req.Items),
		TotalCents: total,
		Status:     orderStatusConfirmed,
		Payment: Payment{
			CardNumber: req.Payment.CardNumber,
			Exp:        req.Payment.Exp,
		},
		BillingEmail: billingEmail,
	}
	s.orderSeq[req.UserID] = seq
	s.orders[o.ID] = o
	return o, nil
}

func (s *store) order(id string) (*Order, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	o, ok := s.orders[id]
	return o, ok
}

// seedRequests returns the users every API starts with. Like generated
// traffic they use only reserved or fictional data: example.com and
// example.org addresses, 555-01XX phone numbers and SSNs from the
// 987-65-4320..4329 range reserved for advertising.
func seedRequests() []CreateUserRequest {
	return []CreateUserRequest{
		{
			FullName: "Maria Castillo",
			Email:    "maria.castillo@example.com",
			Phone:    "(415) 555-0100",
			SSN:      "987-65-4320",
			DOB:      "1984-03-14",
			Address:  &Address{Street: "12 Maple Avenue", City: "Riverton", Zip: "84065"},
			Password: "garden-Lantern-42",
		},
		{
			FullName: "James Okafor",
			Email:    "james.okafor@example.org",
			Phone:    "212-555-0101",
			SSN:      "987-65-4321",
			DOB:      "1990-11-02",
			Address:  &Address{Street: "450 Cedar Lane", City: "Fairview", Zip: "97024"},
			Password: "copper-Harbor-77",
		},
		{
			FullName: "Priya Natarajan",
			Email:    "priya.natarajan@example.com",
			Phone:    "+1 312 555 0102",
			SSN:      "987-65-4322",
			DOB:      "1978-07-23",
			Address:  &Address{Street: "9 Willow Court", City: "Lakewood", Zip: "80226"},
			Password: "silver-Meadow-19",
		},
	}
}

// SeedUsers returns the users every new API starts with, as GET responses
// show them. Leak checks use it: list responses carry these users' personal
// data even though no client sent it.
func SeedUsers() []User {
	reqs := seedRequests()
	users := make([]User, len(reqs))
	for i := range reqs {
		users[i] = newUser(&reqs[i])
	}
	return users
}
