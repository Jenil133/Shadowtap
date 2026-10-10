package api

import (
	"errors"
	"net/http"
	"strconv"
	"time"
)

// maxListLimit caps the limit query parameter of GET /api/users.
const maxListLimit = 1000

func (a *API) healthz(w http.ResponseWriter, r *http.Request) {
	a.writeJSON(w, r, http.StatusOK, Health{Status: "ok", Version: a.version})
}

func (a *API) createUser(w http.ResponseWriter, r *http.Request) {
	var req CreateUserRequest
	if !a.decode(w, r, &req) {
		return
	}
	req.normalize()
	if err := req.validate(a.now()); err != nil {
		a.writeError(w, r, http.StatusUnprocessableEntity, err.Error())
		return
	}
	// Check before hashing so duplicates do not pay for PBKDF2; addUser
	// re-checks under the write lock.
	if a.store.hasUser(UserID(req.Email)) {
		a.writeError(w, r, http.StatusConflict, errEmailTaken.Error())
		return
	}
	var pw passwordHash
	if req.Password != "" {
		var err error
		if pw, err = hashPassword(req.Password); err != nil {
			a.log.ErrorContext(r.Context(), "create user", "error", err)
			a.writeError(w, r, http.StatusInternalServerError, "internal server error")
			return
		}
	}
	rec := newUserRecord(&req, pw)
	if err := a.store.addUser(rec); err != nil {
		a.writeError(w, r, http.StatusConflict, err.Error())
		return
	}
	w.Header().Set("Location", rec.user.Links.Self)
	a.writeJSON(w, r, http.StatusCreated, a.presentUser(rec.user))
}

func (a *API) listUsers(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 0
	if s := q.Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n < 1 || n > maxListLimit {
			a.writeError(w, r, http.StatusBadRequest, "limit must be an integer from 1 to 1000")
			return
		}
		limit = n
	}
	users, total := a.store.listUsers(q.Get("email"), limit, a.version == VersionV2)
	generated := a.now().UTC().Format(time.RFC3339Nano)
	if a.version == VersionV2 {
		list := UserListV2{Users: make([]UserV2, len(users)), Count: len(users), Total: total, GeneratedAt: generated}
		for i, u := range users {
			list.Users[i] = userV2(u)
		}
		a.writeJSON(w, r, http.StatusOK, list)
		return
	}
	a.writeJSON(w, r, http.StatusOK, UserList{Users: users, Count: len(users), Total: total, GeneratedAt: generated})
}

func (a *API) getUser(w http.ResponseWriter, r *http.Request) {
	u, ok := a.store.user(r.PathValue("id"))
	if !ok {
		a.writeError(w, r, http.StatusNotFound, errUserNotFound.Error())
		return
	}
	a.writeJSON(w, r, http.StatusOK, a.presentUser(u))
}

func (a *API) lookupUser(w http.ResponseWriter, r *http.Request) {
	rec, ok := a.store.userByEmail(r.PathValue("email"))
	if !ok {
		a.writeError(w, r, http.StatusNotFound, errUserNotFound.Error())
		return
	}
	a.writeJSON(w, r, http.StatusOK, a.presentUser(rec.user))
}

func (a *API) createOrder(w http.ResponseWriter, r *http.Request) {
	if !a.ordersDelay(r) {
		return
	}
	var req CreateOrderRequest
	if !a.decode(w, r, &req) {
		return
	}
	req.normalize()
	if err := req.validate(); err != nil {
		status := http.StatusUnprocessableEntity
		if a.version == VersionV2 {
			status = http.StatusBadRequest // regression: wrong status for a validation error
		}
		a.writeError(w, r, status, err.Error())
		return
	}
	o, err := a.store.addOrder(&req)
	if errors.Is(err, errUserNotFound) {
		a.writeError(w, r, http.StatusNotFound, err.Error())
		return
	}
	if err != nil {
		a.log.ErrorContext(r.Context(), "create order", "error", err)
		a.writeError(w, r, http.StatusInternalServerError, "internal server error")
		return
	}
	w.Header().Set("Location", "/api/orders/"+o.ID)
	a.writeJSON(w, r, http.StatusCreated, o)
}

func (a *API) getOrder(w http.ResponseWriter, r *http.Request) {
	if !a.ordersDelay(r) {
		return
	}
	o, ok := a.store.order(r.PathValue("id"))
	if !ok {
		a.writeError(w, r, http.StatusNotFound, "order not found")
		return
	}
	a.writeJSON(w, r, http.StatusOK, o)
}

func (a *API) login(w http.ResponseWriter, r *http.Request) {
	var req LoginRequest
	if !a.decode(w, r, &req) {
		return
	}
	if err := req.validate(); err != nil {
		a.writeError(w, r, http.StatusUnprocessableEntity, err.Error())
		return
	}
	rec, ok := a.store.userByEmail(req.Email)
	if !ok {
		a.dummy.matches(req.Password) // equalize timing with known accounts
		a.writeError(w, r, http.StatusUnauthorized, "invalid email or password")
		return
	}
	if !rec.password.matches(req.Password) {
		a.writeError(w, r, http.StatusUnauthorized, "invalid email or password")
		return
	}
	token, err := a.signer.sign(rec.user.ID, a.now())
	if err != nil {
		a.log.ErrorContext(r.Context(), "login", "error", err)
		a.writeError(w, r, http.StatusInternalServerError, "internal server error")
		return
	}
	http.SetCookie(w, &http.Cookie{Name: sessionCookie, Value: newSessionID(), Path: "/", HttpOnly: true})
	w.Header().Set("Cache-Control", "no-store")
	a.writeJSON(w, r, http.StatusOK, LoginResponse{
		Token:     token,
		TokenType: "Bearer",
		ExpiresIn: int(tokenTTL / time.Second),
	})
}

// presentUser returns u as this version renders it.
func (a *API) presentUser(u User) any {
	if a.version == VersionV2 {
		return userV2(u)
	}
	return u
}

func userV2(u User) UserV2 {
	return UserV2{User: u, PhoneNumber: u.Phone}
}

// ordersDelay adds VersionV2's /api/orders latency. It reports false when
// the client went away while waiting.
func (a *API) ordersDelay(r *http.Request) bool {
	if a.version != VersionV2 {
		return true
	}
	t := time.NewTimer(OrdersDelayV2)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-r.Context().Done():
		return false
	}
}
