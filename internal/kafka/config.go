// Package kafka connects Shadowtap to Kafka with franz-go: client options
// (TLS, SASL), a non-blocking producer for captures, and a consumer helper.
package kafka

import "github.com/Jenil133/Shadowtap/internal/capture"

// Config is shared by producers and consumers.
type Config struct {
	Brokers  []string `json:"brokers"`
	Topic    string   `json:"topic"`
	ClientID string   `json:"client_id"`

	// Compression is one of zstd, snappy, lz4, gzip, none.
	Compression string `json:"compression"`
	// LingerMS is how long a partition waits for more records before sending.
	LingerMS int `json:"linger_ms"`
	// MaxBufferedRecords bounds the producer buffer; Publish fails fast
	// with ErrBackpressure when it is full.
	MaxBufferedRecords int `json:"max_buffered_records"`
	// MaxBufferedBytes bounds the producer buffer by size (keys, values and
	// headers), so a Kafka stall cannot grow memory past it.
	MaxBufferedBytes int `json:"max_buffered_bytes"`
	// MaxRecordBytes bounds a record batch and therefore the largest encoded
	// capture; bodies are dropped to fit (capture.Exchange.Fit). Must stay
	// below the topic's max.message.bytes.
	MaxRecordBytes int `json:"max_record_bytes"`
	// DeliveryTimeoutMS bounds how long a record may wait for delivery.
	DeliveryTimeoutMS int `json:"delivery_timeout_ms"`

	TLS  TLSConfig  `json:"tls"`
	SASL SASLConfig `json:"sasl"`
}

// TLSConfig enables TLS to the brokers.
type TLSConfig struct {
	Enabled            bool   `json:"enabled"`
	CAFile             string `json:"ca_file,omitempty"`
	CertFile           string `json:"cert_file,omitempty"`
	KeyFile            string `json:"key_file,omitempty"`
	ServerName         string `json:"server_name,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

// SASLConfig enables SASL authentication.
type SASLConfig struct {
	// Mechanism is PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512; empty disables SASL.
	Mechanism string `json:"mechanism,omitempty"`
	Username  string `json:"username,omitempty"`
	// Password is never read from the config file; the config package fills
	// it from SHADOWTAP_KAFKA_SASL_PASSWORD or PasswordFile.
	Password     string `json:"-"`
	PasswordFile string `json:"password_file,omitempty"`
}

// DefaultConfig targets a local broker and the captures topic.
func DefaultConfig() Config {
	return Config{
		Brokers:            []string{"localhost:9092"},
		Topic:              capture.DefaultTopic,
		ClientID:           "shadowtap",
		Compression:        "zstd",
		LingerMS:           20,
		MaxBufferedRecords: 10_000,
		MaxBufferedBytes:   256 << 20,
		MaxRecordBytes:     900_000,
		DeliveryTimeoutMS:  30_000,
	}
}
