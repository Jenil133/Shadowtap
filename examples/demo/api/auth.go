package api

import (
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

const (
	// passwordIterations is far below current PBKDF2 guidance on purpose: this
	// is an in-memory demo store driven by load generators and race-enabled
	// tests, and every create and login pays this cost.
	passwordIterations = 1024
	passwordKeyBytes   = 32
	passwordSaltBytes  = 16

	tokenTTL      = time.Hour
	tokenIssuer   = "shadowtap-demo-api"
	sessionCookie = "session"
)

// passwordHash is a salted PBKDF2-SHA256 hash. The zero value matches no
// password.
type passwordHash struct {
	salt []byte
	key  []byte
}

func hashPassword(password string) (passwordHash, error) {
	salt := make([]byte, passwordSaltBytes)
	rand.Read(salt) // never fails: crypto/rand crashes the program instead
	key, err := pbkdf2.Key(sha256.New, password, salt, passwordIterations, passwordKeyBytes)
	if err != nil {
		return passwordHash{}, fmt.Errorf("hash password: %w", err)
	}
	return passwordHash{salt: salt, key: key}, nil
}

func (p passwordHash) matches(password string) bool {
	if len(p.key) == 0 {
		return false
	}
	key, err := pbkdf2.Key(sha256.New, password, p.salt, passwordIterations, passwordKeyBytes)
	return err == nil && subtle.ConstantTimeCompare(key, p.key) == 1
}

// tokenSigner issues HS256 JWTs. The key is random per API instance, so
// tokens are only meaningful to the process that issued them.
type tokenSigner struct {
	key []byte
}

func newTokenSigner() tokenSigner {
	key := make([]byte, 32)
	rand.Read(key)
	return tokenSigner{key: key}
}

type tokenClaims struct {
	Issuer    string `json:"iss"`
	Subject   string `json:"sub"`
	IssuedAt  int64  `json:"iat"`
	ExpiresAt int64  `json:"exp"`
}

// jwtHeader is base64url({"alg":"HS256","typ":"JWT"}).
const jwtHeader = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9"

// sign returns header.payload.signature, each part base64url without padding.
func (s tokenSigner) sign(subject string, now time.Time) (string, error) {
	claims, err := json.Marshal(tokenClaims{
		Issuer:    tokenIssuer,
		Subject:   subject,
		IssuedAt:  now.Unix(),
		ExpiresAt: now.Add(tokenTTL).Unix(),
	})
	if err != nil {
		return "", fmt.Errorf("encode token claims: %w", err)
	}
	signingInput := jwtHeader + "." + base64.RawURLEncoding.EncodeToString(claims)
	mac := hmac.New(sha256.New, s.key)
	mac.Write([]byte(signingInput))
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil)), nil
}

// newSessionID returns 128 random bits as base32 text.
func newSessionID() string { return rand.Text() }
