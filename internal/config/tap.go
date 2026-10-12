// Package config loads the shadowtap-tap configuration: a strict YAML file,
// built-in defaults for everything it omits, SHADOWTAP_* environment overrides,
// and secrets read from the environment or mounted files.
package config

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/scrub"
	"github.com/Jenil133/Shadowtap/internal/tap"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

// Environment variables read by LoadTap.
const (
	EnvKafkaBrokers      = "SHADOWTAP_KAFKA_BROKERS"
	EnvKafkaTopic        = "SHADOWTAP_KAFKA_TOPIC"
	EnvKafkaSASLPassword = "SHADOWTAP_KAFKA_SASL_PASSWORD"
	EnvSampleRate        = "SHADOWTAP_SAMPLE_RATE"
	EnvScrubKey          = "SHADOWTAP_SCRUB_KEY"
	EnvLogLevel          = "SHADOWTAP_LOG_LEVEL"
	EnvGRPCAddr          = "SHADOWTAP_GRPC_ADDR"
	EnvHTTPAddr          = "SHADOWTAP_HTTP_ADDR"
	EnvPodName           = "POD_NAME"
	EnvOTelEnabled       = "SHADOWTAP_OTEL_ENABLED"
	EnvOTelEndpoint      = "SHADOWTAP_OTEL_ENDPOINT"
)

// Limits on tap settings that would otherwise exhaust memory.
const (
	minBodyBytes = 1 << 10
	maxBodyBytes = 8 << 20
	maxWorkers   = 1024
)

// Tap is the shadowtap-tap configuration file (tap.yaml).
type Tap struct {
	Log      Log             `json:"log"`
	Listen   Listen          `json:"listen"`
	Tap      tap.Config      `json:"tap"`
	Sampling sampling.Config `json:"sampling"`
	Scrub    Scrub           `json:"scrub"`
	Kafka    kafka.Config    `json:"kafka"`
	// Telemetry exports the tap's capture spans and logs over OTLP.
	Telemetry telemetry.Config `json:"telemetry"`
}

// Log configures the process logger.
type Log struct {
	Level  string `json:"level"`  // debug, info, warn or error
	Format string `json:"format"` // json or text
}

// Listen holds the server addresses.
type Listen struct {
	GRPC string `json:"grpc"` // ext_proc and gRPC health
	HTTP string `json:"http"` // /healthz, /readyz, /metrics
}

// Scrub is the scrubber configuration plus where to find its key. The key
// itself is never part of the file.
type Scrub struct {
	scrub.Config
	// KeyFile is a mounted secret holding the HMAC key.
	KeyFile string `json:"key_file,omitempty"`
	// KeyEnv names the environment variable holding the key; it wins over
	// KeyFile when set.
	KeyEnv string `json:"key_env,omitempty"`
}

// DefaultTap returns the configuration used for every field the file omits.
func DefaultTap() Tap {
	k := kafka.DefaultConfig()
	k.ClientID = "shadowtap-tap"
	tel := telemetry.DefaultConfig()
	tel.ServiceName = "shadowtap-tap"
	tel.Insecure = true // plaintext to an in-cluster Collector; set insecure: false for TLS
	return Tap{
		Log:      Log{Level: "info", Format: "json"},
		Listen:   Listen{GRPC: ":9002", HTTP: ":9090"},
		Tap:      tap.DefaultConfig(),
		Sampling: sampling.DefaultConfig(),
		Scrub: Scrub{
			Config: scrub.Config{MaxDecodedBytes: scrub.DefaultMaxDecodedBytes},
			KeyEnv: EnvScrubKey,
		},
		Kafka:     k,
		Telemetry: tel,
	}
}

// LoadTap reads the file at path (if any) over the defaults, applies
// environment overrides and resolves secrets. It does not validate; call
// Validate before using the result.
func LoadTap(path string, getenv func(string) string) (Tap, error) {
	cfg := DefaultTap()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("config: %w", err)
		}
		if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
			return cfg, fmt.Errorf("config: %s: %w", path, err)
		}
	}
	if err := cfg.applyEnv(getenv); err != nil {
		return cfg, err
	}
	if err := cfg.loadSecrets(getenv); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Tap) applyEnv(getenv func(string) string) error {
	if v := strings.TrimSpace(getenv(EnvKafkaBrokers)); v != "" {
		c.Kafka.Brokers = splitList(v)
	}
	if v := strings.TrimSpace(getenv(EnvKafkaTopic)); v != "" {
		c.Kafka.Topic = v
	}
	if v := strings.TrimSpace(getenv(EnvSampleRate)); v != "" {
		r, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return fmt.Errorf("config: %s: %w", EnvSampleRate, err)
		}
		c.Sampling.DefaultRate = r
	}
	if v := strings.TrimSpace(getenv(EnvLogLevel)); v != "" {
		c.Log.Level = v
	}
	if v := strings.TrimSpace(getenv(EnvGRPCAddr)); v != "" {
		c.Listen.GRPC = v
	}
	if v := strings.TrimSpace(getenv(EnvHTTPAddr)); v != "" {
		c.Listen.HTTP = v
	}
	if c.Tap.Name == "" {
		c.Tap.Name = strings.TrimSpace(getenv(EnvPodName))
	}
	if v := strings.TrimSpace(getenv(EnvOTelEnabled)); v != "" {
		enabled, err := strconv.ParseBool(v)
		if err != nil {
			return fmt.Errorf("config: %s: %w", EnvOTelEnabled, err)
		}
		c.Telemetry.Enabled = enabled
	}
	if v := strings.TrimSpace(getenv(EnvOTelEndpoint)); v != "" {
		c.Telemetry.Endpoint = v
	}
	return nil
}

func (c *Tap) loadSecrets(getenv func(string) string) error {
	if c.Scrub.KeyEnv != "" {
		if v := getenv(c.Scrub.KeyEnv); v != "" {
			c.Scrub.Key = []byte(v)
		}
	}
	if c.Scrub.Key == nil && c.Scrub.KeyFile != "" {
		key, err := readSecret(c.Scrub.KeyFile)
		if err != nil {
			return fmt.Errorf("config: scrub key: %w", err)
		}
		c.Scrub.Key = key
	}
	if v := getenv(EnvKafkaSASLPassword); v != "" {
		c.Kafka.SASL.Password = v
	} else if c.Kafka.SASL.PasswordFile != "" {
		pw, err := readSecret(c.Kafka.SASL.PasswordFile)
		if err != nil {
			return fmt.Errorf("config: kafka sasl password: %w", err)
		}
		c.Kafka.SASL.Password = string(pw)
	}
	return nil
}

// readSecret reads a mounted secret, dropping the trailing newline that
// editors and `echo` add.
func readSecret(path string) ([]byte, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []byte(strings.TrimRight(string(b), "\r\n")), nil
}

// Validate reports every problem with the configuration at once.
func (c Tap) Validate() error {
	var errs []error
	if _, err := parseLevel(c.Log.Level); err != nil {
		errs = append(errs, err)
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		errs = append(errs, fmt.Errorf("log.format %q must be json or text", c.Log.Format))
	}
	if c.Listen.GRPC == "" || c.Listen.HTTP == "" {
		errs = append(errs, errors.New("listen.grpc and listen.http are required"))
	}
	if c.Tap.MaxBodyBytes < minBodyBytes || c.Tap.MaxBodyBytes > maxBodyBytes {
		errs = append(errs, fmt.Errorf("tap.max_body_bytes %d not in [%d, %d]", c.Tap.MaxBodyBytes, minBodyBytes, maxBodyBytes))
	}
	if c.Tap.QueueSize < 1 {
		errs = append(errs, fmt.Errorf("tap.queue_size %d must be positive", c.Tap.QueueSize))
	}
	if c.Tap.MaxQueueBytes != 0 && c.Tap.MaxQueueBytes < c.Tap.MinQueueBytes() {
		// One exchange with both bodies at the limit, plus its per-job
		// allowance, must fit.
		errs = append(errs, fmt.Errorf("tap.max_queue_bytes %d must be 0 or at least 2 x max_body_bytes + %d (%d)",
			c.Tap.MaxQueueBytes, tap.JobOverheadBytes, c.Tap.MinQueueBytes()))
	}
	if c.Tap.Workers < 1 || c.Tap.Workers > maxWorkers {
		errs = append(errs, fmt.Errorf("tap.workers %d not in [1, %d]", c.Tap.Workers, maxWorkers))
	}
	if c.Tap.ServiceMetadataKey == "" {
		errs = append(errs, errors.New("tap.service_metadata_key is required"))
	}
	if err := c.Sampling.Validate(); err != nil {
		errs = append(errs, err)
	}
	if len(c.Scrub.Key) < scrub.MinKeyBytes {
		errs = append(errs, fmt.Errorf("scrub key must be at least %d bytes: set %s or scrub.key_file", scrub.MinKeyBytes, c.Scrub.KeyEnv))
	}
	if c.Scrub.MaxDecodedBytes < 0 {
		errs = append(errs, fmt.Errorf("scrub.max_decoded_bytes %d is negative", c.Scrub.MaxDecodedBytes))
	}
	if err := c.Kafka.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := c.Telemetry.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

// YAML renders the configuration with secrets omitted.
func (c Tap) YAML() ([]byte, error) {
	c.Telemetry = c.Telemetry.Redacted() // header values may hold credentials
	return yaml.Marshal(c)
}

// Logger builds the process logger.
func (l Log) Logger(w io.Writer) (*slog.Logger, error) {
	level, err := parseLevel(l.Level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: level}
	if l.Format == "text" {
		return slog.New(slog.NewTextHandler(w, opts)), nil
	}
	return slog.New(slog.NewJSONHandler(w, opts)), nil
}

func parseLevel(s string) (slog.Level, error) {
	var level slog.Level
	if err := level.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("log.level %q must be debug, info, warn or error", s)
	}
	return level, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
