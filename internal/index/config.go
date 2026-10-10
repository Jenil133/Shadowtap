// Package index is the Kafka → Elasticsearch indexer of Shadowtap. It
// consumes the captures, replay diffs and OpenTelemetry spans and logs that
// land in Kafka, maps them to Elasticsearch documents with ECS and
// OpenTelemetry field names keyed by trace id, and writes them in bulk with
// at-least-once delivery: offsets are committed only after Elasticsearch has
// acknowledged every document of a batch, and documents carry deterministic
// ids so a redelivery is a harmless conflict rather than a duplicate.
//
// Documents land in four data streams: spans in traces-shadowtap-default,
// logs in logs-shadowtap-default, captures in shadowtap-captures and diffs in
// shadowtap-diffs. Every document has trace.id (keyword), service.name and
// kafka.{topic,partition,offset}; the index templates embedded in this
// package describe the rest and Bootstrap applies them idempotently.
package index

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"sigs.k8s.io/yaml"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/kafka"
	"github.com/Jenil133/Shadowtap/internal/telemetry"
)

// ServiceName is the service.name the indexer reports in its own telemetry.
const ServiceName = "shadowtap-indexer"

// Environment variables read by Load.
const (
	EnvKafkaBrokers      = "SHADOWTAP_KAFKA_BROKERS"
	EnvKafkaSASLPassword = "SHADOWTAP_KAFKA_SASL_PASSWORD"
	EnvESURL             = "SHADOWTAP_ES_URL"
	EnvESUsername        = "SHADOWTAP_ES_USERNAME"
	EnvESPassword        = "SHADOWTAP_ES_PASSWORD"
	EnvESAPIKey          = "SHADOWTAP_ES_API_KEY"
	EnvLogLevel          = "SHADOWTAP_LOG_LEVEL"
	EnvHTTPAddr          = "SHADOWTAP_HTTP_ADDR"
	EnvPodName           = "POD_NAME"
)

// Kafka defaults.
const (
	DefaultGroup      = "shadowtap-indexer"
	DefaultSpansTopic = "shadowtap.otel.spans.v1"
	DefaultLogsTopic  = "shadowtap.otel.logs.v1"
	DefaultDLQTopic   = "shadowtap.indexer.dlq.v1"
)

// Default data stream names. Each must match its index template's pattern:
// traces-shadowtap-*, logs-shadowtap-*, shadowtap-captures* and
// shadowtap-diffs*.
const (
	DefaultSpansIndex    = "traces-shadowtap-default"
	DefaultLogsIndex     = "logs-shadowtap-default"
	DefaultCapturesIndex = "shadowtap-captures"
	DefaultDiffsIndex    = "shadowtap-diffs"
)

// Index name patterns the embedded templates match.
const (
	spansIndexPrefix    = "traces-shadowtap-"
	logsIndexPrefix     = "logs-shadowtap-"
	capturesIndexPrefix = "shadowtap-captures"
	diffsIndexPrefix    = "shadowtap-diffs"
)

// Limits that keep a misconfiguration from exhausting memory or stalling
// the consumer group.
const (
	maxBatchRecords  = 50_000
	maxBatchBytes    = 256 << 20
	maxTopicLength   = 249
	maxIndexLength   = 255
	maxRetryAttempts = 100
)

// Config is the shadowtap-indexer configuration file (indexer.yaml).
type Config struct {
	Log    Log    `json:"log"`
	Listen Listen `json:"listen"`
	// Name identifies this instance in DLQ records and logs; it defaults to
	// POD_NAME, then the hostname.
	Name          string      `json:"name,omitempty"`
	Kafka         KafkaConfig `json:"kafka"`
	Elasticsearch ESConfig    `json:"elasticsearch"`
	Indices       Indices     `json:"indices"`
	Batch         BatchConfig `json:"batch"`
	Retry         RetryConfig `json:"retry"`
	// Telemetry configures the indexer's own traces and logs. The service
	// name defaults to ServiceName.
	Telemetry telemetry.Config `json:"telemetry"`
	// ShutdownTimeoutMS bounds the final batch, its commit and leaving the
	// group once the indexer is told to stop.
	ShutdownTimeoutMS int `json:"shutdown_timeout_ms"`
}

// Log configures the process logger.
type Log struct {
	Level  string `json:"level"`  // debug, info, warn or error
	Format string `json:"format"` // json or text
}

// Listen holds the server addresses.
type Listen struct {
	HTTP string `json:"http"` // /healthz, /readyz, /metrics
}

// KafkaConfig is the consumer group and the DLQ producer. TLS and SASL are
// shared with the other Shadowtap processes (kafka.ClientOptions).
type KafkaConfig struct {
	Brokers  []string `json:"brokers"`
	ClientID string   `json:"client_id"`
	Group    string   `json:"group"`
	Topics   Topics   `json:"topics"`
	// StartOffset is where a partition without a committed offset starts:
	// earliest or latest.
	StartOffset string `json:"start_offset"`
	// FetchMaxBytes bounds one fetch response per broker.
	FetchMaxBytes      int `json:"fetch_max_bytes"`
	SessionTimeoutMS   int `json:"session_timeout_ms"`
	RebalanceTimeoutMS int `json:"rebalance_timeout_ms"`
	// MaxRecordBytes bounds a DLQ record; the failed document or value is
	// dropped from a record that would exceed it.
	MaxRecordBytes int              `json:"max_record_bytes"`
	TLS            kafka.TLSConfig  `json:"tls"`
	SASL           kafka.SASLConfig `json:"sasl"`
}

// Topics names the topics consumed and the dead-letter topic produced.
type Topics struct {
	Captures string `json:"captures"`
	Diffs    string `json:"diffs"`
	Spans    string `json:"spans"`
	Logs     string `json:"logs"`
	DLQ      string `json:"dlq"`
}

// ESConfig connects to Elasticsearch. Credentials are never read from the
// file: the password comes from SHADOWTAP_ES_PASSWORD or PasswordFile, the
// API key from SHADOWTAP_ES_API_KEY or APIKeyFile. An API key wins over basic
// auth when both are present.
type ESConfig struct {
	URLs         []string `json:"urls"`
	Username     string   `json:"username,omitempty"`
	Password     string   `json:"-"`
	PasswordFile string   `json:"password_file,omitempty"`
	APIKey       string   `json:"-"`
	APIKeyFile   string   `json:"api_key_file,omitempty"`
	TLS          ESTLS    `json:"tls"`
	// RequestTimeoutMS bounds one request, bulk or bootstrap.
	RequestTimeoutMS int `json:"request_timeout_ms"`
	// Compress gzips request bodies; bulk NDJSON compresses well.
	Compress bool `json:"compress"`
	// Bootstrap applies the ILM policy, templates and data streams at
	// startup.
	Bootstrap bool `json:"bootstrap"`
}

// ESTLS configures TLS to Elasticsearch; it applies to https URLs.
type ESTLS struct {
	CAFile             string `json:"ca_file,omitempty"`
	CertFile           string `json:"cert_file,omitempty"`
	KeyFile            string `json:"key_file,omitempty"`
	ServerName         string `json:"server_name,omitempty"`
	InsecureSkipVerify bool   `json:"insecure_skip_verify,omitempty"`
}

// Indices names the data streams documents are written to.
type Indices struct {
	Spans    string `json:"spans"`
	Logs     string `json:"logs"`
	Captures string `json:"captures"`
	Diffs    string `json:"diffs"`
}

// All returns every data stream name, in a fixed order: spans, logs,
// captures, diffs.
func (i Indices) All() []string {
	return []string{i.Spans, i.Logs, i.Captures, i.Diffs}
}

// BatchConfig bounds one bulk request. A batch is sent when it reaches
// MaxRecords records or MaxBytes of record data, or MaxIntervalMS after its
// first record arrived.
type BatchConfig struct {
	MaxRecords    int `json:"max_records"`
	MaxBytes      int `json:"max_bytes"`
	MaxIntervalMS int `json:"max_interval_ms"`
}

// RetryConfig controls retries of documents Elasticsearch rejected with a
// transient status (429, 503). A document still failing after MaxAttempts
// goes to the DLQ. Request-level failures (Elasticsearch unreachable or
// answering the whole request with an error) are retried with the same
// backoff for as long as the indexer runs, since nothing is wrong with the
// documents.
type RetryConfig struct {
	MaxAttempts      int `json:"max_attempts"`
	InitialBackoffMS int `json:"initial_backoff_ms"`
	MaxBackoffMS     int `json:"max_backoff_ms"`
}

// DefaultConfig returns the configuration used for every field the file
// omits.
func DefaultConfig() Config {
	return Config{
		Log:    Log{Level: "info", Format: "json"},
		Listen: Listen{HTTP: ":9091"},
		Kafka: KafkaConfig{
			Brokers:  []string{"localhost:9092"},
			ClientID: "shadowtap-indexer",
			Group:    DefaultGroup,
			Topics: Topics{
				Captures: capture.DefaultTopic,
				Diffs:    diff.DefaultTopic,
				Spans:    DefaultSpansTopic,
				Logs:     DefaultLogsTopic,
				DLQ:      DefaultDLQTopic,
			},
			StartOffset:        "earliest",
			FetchMaxBytes:      16 << 20,
			SessionTimeoutMS:   45_000,
			RebalanceTimeoutMS: 60_000,
			MaxRecordBytes:     900_000,
		},
		Elasticsearch: ESConfig{
			URLs:             []string{"http://localhost:9200"},
			RequestTimeoutMS: 30_000,
			Compress:         true,
			Bootstrap:        true,
		},
		Indices: DefaultIndices(),
		Batch: BatchConfig{
			MaxRecords:    1000,
			MaxBytes:      5 << 20,
			MaxIntervalMS: 1000,
		},
		Retry: RetryConfig{
			MaxAttempts:      5,
			InitialBackoffMS: 200,
			MaxBackoffMS:     10_000,
		},
		Telemetry:         defaultTelemetry(),
		ShutdownTimeoutMS: 20_000,
	}
}

// defaultTelemetry is the telemetry package's default under the indexer's
// own service name.
func defaultTelemetry() telemetry.Config {
	t := telemetry.DefaultConfig()
	t.ServiceName = ServiceName
	return t
}

// DefaultIndices returns the default data stream names.
func DefaultIndices() Indices {
	return Indices{
		Spans:    DefaultSpansIndex,
		Logs:     DefaultLogsIndex,
		Captures: DefaultCapturesIndex,
		Diffs:    DefaultDiffsIndex,
	}
}

// Load reads the file at path (if any) over the defaults, applies
// environment overrides and resolves secrets. It does not validate; call
// Validate before using the result.
func Load(path string, getenv func(string) string) (Config, error) {
	cfg := DefaultConfig()
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("config: %w", err)
		}
		if err := yaml.UnmarshalStrict(data, &cfg); err != nil {
			return cfg, fmt.Errorf("config: %s: %w", path, err)
		}
	}
	cfg.applyEnv(getenv)
	if err := cfg.loadSecrets(getenv); err != nil {
		return cfg, err
	}
	return cfg, nil
}

func (c *Config) applyEnv(getenv func(string) string) {
	if v := strings.TrimSpace(getenv(EnvKafkaBrokers)); v != "" {
		c.Kafka.Brokers = splitList(v)
	}
	if v := strings.TrimSpace(getenv(EnvESURL)); v != "" {
		c.Elasticsearch.URLs = splitList(v)
	}
	if v := strings.TrimSpace(getenv(EnvESUsername)); v != "" {
		c.Elasticsearch.Username = v
	}
	if v := strings.TrimSpace(getenv(EnvLogLevel)); v != "" {
		c.Log.Level = v
	}
	if v := strings.TrimSpace(getenv(EnvHTTPAddr)); v != "" {
		c.Listen.HTTP = v
	}
	if c.Name == "" {
		c.Name = strings.TrimSpace(getenv(EnvPodName))
	}
}

func (c *Config) loadSecrets(getenv func(string) string) error {
	secret := func(env, file, what string) (string, error) {
		if v := getenv(env); v != "" {
			return v, nil
		}
		if file == "" {
			return "", nil
		}
		b, err := readSecret(file)
		if err != nil {
			return "", fmt.Errorf("config: %s: %w", what, err)
		}
		return string(b), nil
	}
	var err error
	if c.Elasticsearch.Password, err = secret(EnvESPassword, c.Elasticsearch.PasswordFile, "elasticsearch password"); err != nil {
		return err
	}
	if c.Elasticsearch.APIKey, err = secret(EnvESAPIKey, c.Elasticsearch.APIKeyFile, "elasticsearch api key"); err != nil {
		return err
	}
	if c.Kafka.SASL.Password, err = secret(EnvKafkaSASLPassword, c.Kafka.SASL.PasswordFile, "kafka sasl password"); err != nil {
		return err
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
func (c Config) Validate() error {
	var errs []error
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Errorf(format, args...))
	}
	if _, err := parseLevel(c.Log.Level); err != nil {
		errs = append(errs, err)
	}
	if c.Log.Format != "json" && c.Log.Format != "text" {
		fail("log.format %q must be json or text", c.Log.Format)
	}
	if c.Listen.HTTP == "" {
		fail("listen.http is required")
	}

	// kafka.Config.Validate covers brokers, SASL, the TLS key pair and the
	// DLQ topic name (plus producer settings taken from the defaults).
	if err := c.Kafka.clientConfig(c.Kafka.Topics.DLQ).Validate(); err != nil {
		errs = append(errs, err)
	}
	if c.Kafka.Group == "" {
		fail("kafka.group is required")
	}
	topics := map[string]string{
		"kafka.topics.captures": c.Kafka.Topics.Captures,
		"kafka.topics.diffs":    c.Kafka.Topics.Diffs,
		"kafka.topics.spans":    c.Kafka.Topics.Spans,
		"kafka.topics.logs":     c.Kafka.Topics.Logs,
	}
	seen := map[string]string{c.Kafka.Topics.DLQ: "kafka.topics.dlq"}
	for _, field := range []string{"kafka.topics.captures", "kafka.topics.diffs", "kafka.topics.spans", "kafka.topics.logs"} {
		topic := topics[field]
		if err := topicProblem(topic); err != nil {
			fail("%s: %v", field, err)
			continue
		}
		if other, dup := seen[topic]; dup {
			fail("%s and %s are both %q", other, field, topic)
		}
		seen[topic] = field
	}
	switch strings.ToLower(c.Kafka.StartOffset) {
	case "earliest", "latest":
	default:
		fail("kafka.start_offset %q must be earliest or latest", c.Kafka.StartOffset)
	}
	if c.Kafka.FetchMaxBytes <= 0 {
		fail("kafka.fetch_max_bytes %d must be positive", c.Kafka.FetchMaxBytes)
	}
	if c.Kafka.SessionTimeoutMS <= 0 {
		fail("kafka.session_timeout_ms %d must be positive", c.Kafka.SessionTimeoutMS)
	}
	if c.Kafka.RebalanceTimeoutMS <= 0 {
		fail("kafka.rebalance_timeout_ms %d must be positive", c.Kafka.RebalanceTimeoutMS)
	}

	if len(c.Elasticsearch.URLs) == 0 {
		fail("elasticsearch.urls is empty")
	}
	for i, raw := range c.Elasticsearch.URLs {
		u, err := url.Parse(strings.TrimSpace(raw))
		switch {
		case err != nil:
			fail("elasticsearch.urls[%d]: %v", i, err)
		case (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
			fail("elasticsearch.urls[%d] %q must be an http(s) URL with a host", i, raw)
		}
	}
	if c.Elasticsearch.Username != "" && c.Elasticsearch.Password == "" {
		fail("elasticsearch password is required with username %q (set %s or elasticsearch.password_file)", c.Elasticsearch.Username, EnvESPassword)
	}
	if c.Elasticsearch.Username == "" && c.Elasticsearch.Password != "" {
		fail("elasticsearch.username is required with a password")
	}
	if (c.Elasticsearch.TLS.CertFile == "") != (c.Elasticsearch.TLS.KeyFile == "") {
		fail("elasticsearch.tls.cert_file and elasticsearch.tls.key_file must be set together")
	}
	if c.Elasticsearch.RequestTimeoutMS <= 0 {
		fail("elasticsearch.request_timeout_ms %d must be positive", c.Elasticsearch.RequestTimeoutMS)
	}

	for _, idx := range []struct{ field, name, prefix string }{
		{"indices.spans", c.Indices.Spans, spansIndexPrefix},
		{"indices.logs", c.Indices.Logs, logsIndexPrefix},
		{"indices.captures", c.Indices.Captures, capturesIndexPrefix},
		{"indices.diffs", c.Indices.Diffs, diffsIndexPrefix},
	} {
		if err := indexProblem(idx.name); err != nil {
			fail("%s: %v", idx.field, err)
			continue
		}
		if !strings.HasPrefix(idx.name, idx.prefix) {
			// The embedded index templates only apply to names of this shape.
			fail("%s %q must start with %q to match its index template", idx.field, idx.name, idx.prefix)
		}
	}

	if c.Batch.MaxRecords < 1 || c.Batch.MaxRecords > maxBatchRecords {
		fail("batch.max_records %d not in [1, %d]", c.Batch.MaxRecords, maxBatchRecords)
	}
	if c.Batch.MaxBytes < 1 || c.Batch.MaxBytes > maxBatchBytes {
		fail("batch.max_bytes %d not in [1, %d]", c.Batch.MaxBytes, maxBatchBytes)
	}
	if c.Batch.MaxIntervalMS < 1 {
		fail("batch.max_interval_ms %d must be positive", c.Batch.MaxIntervalMS)
	}
	if c.Retry.MaxAttempts < 1 || c.Retry.MaxAttempts > maxRetryAttempts {
		fail("retry.max_attempts %d not in [1, %d]", c.Retry.MaxAttempts, maxRetryAttempts)
	}
	if c.Retry.InitialBackoffMS < 1 {
		fail("retry.initial_backoff_ms %d must be positive", c.Retry.InitialBackoffMS)
	}
	if c.Retry.MaxBackoffMS < c.Retry.InitialBackoffMS {
		fail("retry.max_backoff_ms %d is below retry.initial_backoff_ms %d", c.Retry.MaxBackoffMS, c.Retry.InitialBackoffMS)
	}
	if c.ShutdownTimeoutMS < 1 {
		fail("shutdown_timeout_ms %d must be positive", c.ShutdownTimeoutMS)
	}
	if err := c.Telemetry.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("config: %w", err)
	}
	return nil
}

// clientConfig returns the shared Kafka settings as a kafka.Config for
// kafka.ClientOptions and kafka.NewWriter, with topic as the writer's topic.
func (k KafkaConfig) clientConfig(topic string) kafka.Config {
	c := kafka.DefaultConfig()
	c.Brokers = k.Brokers
	c.ClientID = k.ClientID
	c.Topic = topic
	c.TLS = k.TLS
	c.SASL = k.SASL
	if k.MaxRecordBytes > 0 {
		c.MaxRecordBytes = k.MaxRecordBytes
	}
	return c
}

// topicProblem applies Kafka's topic naming rules.
func topicProblem(topic string) error {
	switch {
	case topic == "":
		return errors.New("topic is empty")
	case len(topic) > maxTopicLength:
		return fmt.Errorf("topic is longer than %d characters", maxTopicLength)
	case topic == "." || topic == "..":
		return fmt.Errorf("topic %q is not allowed", topic)
	}
	for _, r := range topic {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-') {
			return fmt.Errorf("topic %q may only contain ASCII letters, digits, '.', '_' and '-'", topic)
		}
	}
	return nil
}

// indexProblem applies Elasticsearch's index naming rules.
func indexProblem(name string) error {
	switch {
	case name == "":
		return errors.New("index name is empty")
	case len(name) > maxIndexLength:
		return fmt.Errorf("index name is longer than %d bytes", maxIndexLength)
	case name == "." || name == "..":
		return fmt.Errorf("index name %q is not allowed", name)
	case strings.ContainsAny(name[:1], "-_+"):
		return fmt.Errorf("index name %q must not start with '-', '_' or '+'", name)
	case name != strings.ToLower(name):
		return fmt.Errorf("index name %q must be lowercase", name)
	case strings.ContainsAny(name, `\/*?"<>| ,#:`):
		return fmt.Errorf("index name %q contains a character Elasticsearch rejects", name)
	}
	return nil
}

// YAML renders the configuration with secrets omitted: passwords and API
// keys are never serialized, and telemetry header values are masked.
func (c Config) YAML() ([]byte, error) {
	c.Telemetry = c.Telemetry.Redacted()
	return yaml.Marshal(c)
}

// Handler builds the process log handler, which telemetry.Setup wraps so
// records also reach the OTLP exporter.
func (l Log) Handler(w io.Writer) (slog.Handler, error) {
	level, err := parseLevel(l.Level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: level}
	if l.Format == "text" {
		return slog.NewTextHandler(w, opts), nil
	}
	return slog.NewJSONHandler(w, opts), nil
}

// Logger builds the process logger without telemetry.
func (l Log) Logger(w io.Writer) (*slog.Logger, error) {
	h, err := l.Handler(w)
	if err != nil {
		return nil, err
	}
	return slog.New(h), nil
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
