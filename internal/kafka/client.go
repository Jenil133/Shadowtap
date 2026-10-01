package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ClientOptions returns the franz-go options every Shadowtap client shares:
// seed brokers, client id, TLS and SASL. Only the connection settings of cfg
// are validated (brokers, SASL, TLS key pair); the TLS files are read here,
// so a missing or malformed certificate is reported as an error.
func ClientOptions(cfg Config) ([]kgo.Opt, error) {
	if err := configError(cfg.connectionProblems()); err != nil {
		return nil, err
	}

	brokers := make([]string, len(cfg.Brokers))
	for i, b := range cfg.Brokers {
		brokers[i] = strings.TrimSpace(b)
	}
	opts := []kgo.Opt{kgo.SeedBrokers(brokers...)}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.TLS.Enabled {
		tc, err := newTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tc))
	}
	if m := saslMechanism(cfg.SASL); m != nil {
		opts = append(opts, kgo.SASL(m))
	}
	return opts, nil
}

// newTLSConfig builds the dialer's TLS config. Without a CA file the system
// roots are used. franz-go fills ServerName from each broker's host when it
// is empty.
func newTLSConfig(c TLSConfig) (*tls.Config, error) {
	tc := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		ServerName:         c.ServerName,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // explicit opt-in for test clusters
	}
	if c.CAFile != "" {
		pem, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: read tls.ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("kafka: tls.ca_file %s: no PEM certificates found", c.CAFile)
		}
		tc.RootCAs = pool
	}
	if c.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafka: load tls client certificate: %w", err)
		}
		tc.Certificates = []tls.Certificate{cert}
	}
	return tc, nil
}

// saslMechanism returns the configured mechanism, or nil when SASL is off.
// The config must already have passed connectionProblems.
func saslMechanism(c SASLConfig) sasl.Mechanism {
	mechanism, _ := normalizeMechanism(c.Mechanism)
	switch mechanism {
	case mechanismPlain:
		return plain.Auth{User: c.Username, Pass: c.Password}.AsMechanism()
	case mechanismSCRAMSHA256:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha256Mechanism()
	case mechanismSCRAMSHA512:
		return scram.Auth{User: c.Username, Pass: c.Password}.AsSha512Mechanism()
	default:
		return nil
	}
}
