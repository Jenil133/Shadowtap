package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/Jenil133/Shadowtap/internal/kafka"
)

// Environment variables the CLI reads for Kafka connections. The SASL
// password is only ever taken from the environment or a file, never from a
// flag, so it does not end up in shell history or the process list.
const (
	envKafkaBrokers      = "SHADOWTAP_KAFKA_BROKERS"
	envKafkaSASLPassword = "SHADOWTAP_KAFKA_SASL_PASSWORD"
)

// connFlags are the Kafka connection flags shared by every subcommand that
// talks to Kafka.
type connFlags struct {
	brokers string

	tls                bool
	caFile             string
	certFile           string
	keyFile            string
	serverName         string
	insecureSkipVerify bool

	saslMechanism    string
	saslUsername     string
	saslPasswordFile string
}

// register adds the flags to fs. getenv supplies the default brokers.
func (c *connFlags) register(fs *flag.FlagSet, getenv func(string) string) {
	defaultBrokers := getenv(envKafkaBrokers)
	if defaultBrokers == "" {
		defaultBrokers = "localhost:9092"
	}
	fs.StringVar(&c.brokers, "brokers", defaultBrokers, "comma-separated Kafka seed brokers (default $"+envKafkaBrokers+")")
	fs.BoolVar(&c.tls, "kafka-tls", false, "connect to Kafka over TLS (implied by any other --kafka-tls-* flag)")
	fs.StringVar(&c.caFile, "kafka-tls-ca", "", "PEM CA bundle for the brokers (default: system roots)")
	fs.StringVar(&c.certFile, "kafka-tls-cert", "", "PEM client certificate for mutual TLS")
	fs.StringVar(&c.keyFile, "kafka-tls-key", "", "PEM client key for mutual TLS")
	fs.StringVar(&c.serverName, "kafka-tls-server-name", "", "TLS server name to verify instead of the broker host")
	fs.BoolVar(&c.insecureSkipVerify, "kafka-tls-insecure-skip-verify", false, "do not verify the broker certificate (testing only)")
	fs.StringVar(&c.saslMechanism, "kafka-sasl-mechanism", "", "SASL mechanism: PLAIN, SCRAM-SHA-256 or SCRAM-SHA-512")
	fs.StringVar(&c.saslUsername, "kafka-sasl-username", "", "SASL username")
	fs.StringVar(&c.saslPasswordFile, "kafka-sasl-password-file", "",
		"file holding the SASL password (default: $"+envKafkaSASLPassword+")")
}

// apply sets the connection settings on cfg.
func (c *connFlags) apply(cfg *kafka.Config, getenv func(string) string) error {
	cfg.Brokers = splitList(c.brokers)
	if c.tls || c.caFile != "" || c.certFile != "" || c.keyFile != "" || c.serverName != "" || c.insecureSkipVerify {
		cfg.TLS = kafka.TLSConfig{
			Enabled:            true,
			CAFile:             c.caFile,
			CertFile:           c.certFile,
			KeyFile:            c.keyFile,
			ServerName:         c.serverName,
			InsecureSkipVerify: c.insecureSkipVerify,
		}
	}
	if c.saslMechanism == "" {
		if c.saslUsername != "" || c.saslPasswordFile != "" {
			return fmt.Errorf("--kafka-sasl-username and --kafka-sasl-password-file need --kafka-sasl-mechanism")
		}
		return nil
	}
	cfg.SASL = kafka.SASLConfig{Mechanism: c.saslMechanism, Username: c.saslUsername}
	switch {
	case c.saslPasswordFile != "":
		b, err := os.ReadFile(c.saslPasswordFile)
		if err != nil {
			return fmt.Errorf("--kafka-sasl-password-file: %w", err)
		}
		cfg.SASL.Password = strings.TrimRight(string(b), "\r\n")
	default:
		cfg.SASL.Password = getenv(envKafkaSASLPassword)
	}
	return nil
}
