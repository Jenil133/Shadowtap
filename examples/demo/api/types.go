package api

// Address is a postal address. Every field is personal data; it travels under
// the sensitive "address" key so scrubbers redact all of its leaves.
type Address struct {
	Street string `json:"street"`
	City   string `json:"city"`
	Zip    string `json:"zip"`
}

// CreateUserRequest is the body of POST /api/users. Only FullName and Email
// are required. Password is optional and never echoed; a user created without
// one cannot log in.
type CreateUserRequest struct {
	FullName string   `json:"full_name"`
	Email    string   `json:"email"`
	Phone    string   `json:"phone,omitempty"`
	SSN      string   `json:"ssn,omitempty"`
	DOB      string   `json:"dob,omitempty"` // YYYY-MM-DD
	Address  *Address `json:"address,omitempty"`
	Password string   `json:"password,omitempty"`
}

// User is a user as the API returns it. It deliberately echoes personal data
// so that response scrubbing is exercised.
type User struct {
	ID          string      `json:"id"`
	FullName    string      `json:"full_name"`
	Email       string      `json:"email"`
	Phone       string      `json:"phone,omitempty"`
	SSN         string      `json:"ssn,omitempty"`
	DOB         string      `json:"dob,omitempty"`
	Address     *Address    `json:"address,omitempty"`
	Status      string      `json:"status"`
	Tier        string      `json:"tier"`
	Preferences Preferences `json:"preferences"`
	Links       Links       `json:"links"`
}

// Preferences are account defaults assigned by the API.
type Preferences struct {
	Locale     string `json:"locale"`
	Timezone   string `json:"timezone"`
	Currency   string `json:"currency"`
	Newsletter bool   `json:"newsletter"`
}

// Links holds paths of related resources.
type Links struct {
	Self string `json:"self"`
}

// UserList is the body of GET /api/users.
type UserList struct {
	Users []User `json:"users"`
	Count int    `json:"count"` // users in this response
	Total int    `json:"total"` // users matching the query before the limit
}

// Item is one order line.
type Item struct {
	SKU        string `json:"sku"`
	Qty        int    `json:"qty"`
	PriceCents int64  `json:"price_cents"`
}

// PaymentInput is the card an order is paid with.
type PaymentInput struct {
	CardNumber string `json:"card_number"`
	CVV        string `json:"cvv"`
	Exp        string `json:"exp"` // MM/YY
}

// CreateOrderRequest is the body of POST /api/orders.
type CreateOrderRequest struct {
	UserID  string       `json:"user_id"`
	Items   []Item       `json:"items"`
	Payment PaymentInput `json:"payment"`
}

// Payment is the payment method stored with an order. The CVV is never kept.
type Payment struct {
	CardNumber string `json:"card_number"`
	Exp        string `json:"exp"`
}

// Order is an order as the API returns it.
type Order struct {
	ID           string  `json:"id"`
	UserID       string  `json:"user_id"`
	Items        []Item  `json:"items"`
	TotalCents   int64   `json:"total_cents"`
	Status       string  `json:"status"`
	Payment      Payment `json:"payment"`
	BillingEmail string  `json:"billing_email"`
}

// LoginRequest is the body of POST /api/login.
type LoginRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

// LoginResponse is returned for valid credentials, together with a session
// cookie.
type LoginResponse struct {
	Token     string `json:"token"` // JWT-shaped bearer token
	TokenType string `json:"token_type"`
	ExpiresIn int    `json:"expires_in"` // seconds
}

// Health is the body of GET /healthz.
type Health struct {
	Status  string `json:"status"`
	Version string `json:"version"`
}

// ErrorResponse is the body of every error response.
type ErrorResponse struct {
	Error string `json:"error"`
}
