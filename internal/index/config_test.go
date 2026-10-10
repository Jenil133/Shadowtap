package index

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/diff"
	"github.com/Jenil133/Shadowtap/internal/kafka"
)

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func kafkaSASL(mechanism, user, password string) kafka.SASLConfig {
	return kafka.SASLConfig{Mechanism: mechanism, Username: user, Password: password}
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDefaultConfigIsValid(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate(DefaultConfig()) = %v", err)
	}
	if cfg.Kafka.Group != DefaultGroup || cfg.Kafka.Topics.Captures != capture.DefaultTopic || cfg.Kafka.Topics.Diffs != diff.DefaultTopic ||
		cfg.Kafka.Topics.Spans != DefaultSpansTopic || cfg.Kafka.Topics.Logs != DefaultLogsTopic || cfg.Kafka.Topics.DLQ != DefaultDLQTopic {
		t.Errorf("kafka defaults = %+v", cfg.Kafka)
	}
	if cfg.Indices != DefaultIndices() || cfg.Indices.Spans != "traces-shadowtap-default" || cfg.Indices.Logs != "logs-shadowtap-default" {
		t.Errorf("indices = %+v", cfg.Indices)
	}
	if cfg.Telemetry.ServiceName != ServiceName || cfg.Telemetry.Enabled || cfg.Telemetry.SampleRatio != 1 {
		t.Errorf("telemetry defaults = %+v", cfg.Telemetry)
	}
	if !cfg.Elasticsearch.Bootstrap || !cfg.Elasticsearch.Compress {
		t.Errorf("elasticsearch defaults = %+v", cfg.Elasticsearch)
	}
}

func TestLoadFileEnvAndSecrets(t *testing.T) {
	pw := writeFile(t, "es-password", "from-file\n")
	saslPw := writeFile(t, "sasl-password", "sasl-from-file\r\n")
	path := writeFile(t, "indexer.yaml", `
log:
  level: debug
  format: text
kafka:
  brokers: [file-broker:9092]
  group: custom-group
  topics:
    spans: custom.spans
  sasl:
    mechanism: SCRAM-SHA-256
    username: indexer
    password_file: `+saslPw+`
elasticsearch:
  urls: [https://file-es:9200]
  username: file-user
  password_file: `+pw+`
  tls:
    insecure_skip_verify: true
indices:
  spans: traces-shadowtap-staging
batch:
  max_records: 10
telemetry:
  enabled: true
  endpoint: collector:4317
  insecure: true
  headers:
    authorization: Bearer token
`)
	t.Run("file over defaults", func(t *testing.T) {
		cfg, err := Load(path, env(nil))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Log.Level != "debug" || cfg.Log.Format != "text" || cfg.Kafka.Brokers[0] != "file-broker:9092" || cfg.Kafka.Group != "custom-group" ||
			cfg.Kafka.Topics.Spans != "custom.spans" || cfg.Kafka.Topics.Logs != DefaultLogsTopic || cfg.Elasticsearch.URLs[0] != "https://file-es:9200" ||
			cfg.Elasticsearch.Username != "file-user" || cfg.Elasticsearch.Password != "from-file" || cfg.Kafka.SASL.Password != "sasl-from-file" ||
			!cfg.Elasticsearch.TLS.InsecureSkipVerify || cfg.Indices.Spans != "traces-shadowtap-staging" || cfg.Batch.MaxRecords != 10 ||
			cfg.Batch.MaxBytes != DefaultConfig().Batch.MaxBytes || !cfg.Telemetry.Enabled || cfg.Telemetry.Endpoint != "collector:4317" || cfg.Telemetry.ServiceName != ServiceName {
			t.Errorf("loaded %+v", cfg)
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate: %v", err)
		}
	})
	t.Run("environment over file", func(t *testing.T) {
		cfg, err := Load(path, env(map[string]string{
			EnvKafkaBrokers:      "b1:9092, b2:9092,",
			EnvKafkaSASLPassword: "sasl-from-env",
			EnvESURL:             "http://es1:9200,http://es2:9200",
			EnvESUsername:        "env-user",
			EnvESPassword:        "env-password",
			EnvESAPIKey:          "env-key",
			EnvLogLevel:          "warn",
			EnvHTTPAddr:          ":8081",
			EnvPodName:           "indexer-0",
		}))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Join(cfg.Kafka.Brokers, ",") != "b1:9092,b2:9092" || cfg.Kafka.SASL.Password != "sasl-from-env" ||
			strings.Join(cfg.Elasticsearch.URLs, ",") != "http://es1:9200,http://es2:9200" || cfg.Elasticsearch.Username != "env-user" ||
			cfg.Elasticsearch.Password != "env-password" || cfg.Elasticsearch.APIKey != "env-key" || cfg.Log.Level != "warn" ||
			cfg.Listen.HTTP != ":8081" || cfg.Name != "indexer-0" {
			t.Errorf("loaded %+v", cfg)
		}
	})
	t.Run("name from the file wins over the pod name", func(t *testing.T) {
		named := writeFile(t, "named.yaml", "name: custom\n")
		cfg, err := Load(named, env(map[string]string{EnvPodName: "indexer-0"}))
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Name != "custom" {
			t.Errorf("name = %q", cfg.Name)
		}
	})
}

func TestLoadErrors(t *testing.T) {
	if _, err := Load("/nonexistent/indexer.yaml", env(nil)); err == nil {
		t.Error("missing file accepted")
	}
	unknown := writeFile(t, "unknown.yaml", "kafka:\n  bootstrap_servers: [x]\n")
	if _, err := Load(unknown, env(nil)); err == nil || !strings.Contains(err.Error(), "bootstrap_servers") {
		t.Errorf("unknown field: %v", err)
	}
	// Secrets never come from the file.
	secret := writeFile(t, "secret.yaml", "elasticsearch:\n  password: nope\n")
	if _, err := Load(secret, env(nil)); err == nil {
		t.Error("password in the file accepted")
	}
	missingPw := writeFile(t, "pw.yaml", "elasticsearch:\n  password_file: /nonexistent/pw\n")
	if _, err := Load(missingPw, env(nil)); err == nil || !strings.Contains(err.Error(), "elasticsearch password") {
		t.Errorf("missing password file: %v", err)
	}
	missingKey := writeFile(t, "key.yaml", "elasticsearch:\n  api_key_file: /nonexistent/key\n")
	if _, err := Load(missingKey, env(nil)); err == nil || !strings.Contains(err.Error(), "api key") {
		t.Errorf("missing api key file: %v", err)
	}
	missingSASL := writeFile(t, "sasl.yaml", "kafka:\n  sasl:\n    password_file: /nonexistent/pw\n")
	if _, err := Load(missingSASL, env(nil)); err == nil || !strings.Contains(err.Error(), "sasl") {
		t.Errorf("missing sasl password file: %v", err)
	}
	// Environment secrets win over files, so a missing file is fine then.
	cfg, err := Load(missingPw, env(map[string]string{EnvESPassword: "env"}))
	if err != nil || cfg.Elasticsearch.Password != "env" {
		t.Errorf("env password with a missing file: %v, %q", err, cfg.Elasticsearch.Password)
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"bad log level", func(c *Config) { c.Log.Level = "loud" }, "log.level"},
		{"bad log format", func(c *Config) { c.Log.Format = "xml" }, "log.format"},
		{"no listen address", func(c *Config) { c.Listen.HTTP = "" }, "listen.http"},
		{"no brokers", func(c *Config) { c.Kafka.Brokers = nil }, "brokers is empty"},
		{"no group", func(c *Config) { c.Kafka.Group = "" }, "kafka.group"},
		{"bad topic", func(c *Config) { c.Kafka.Topics.Spans = "bad topic!" }, "kafka.topics.spans"},
		{"empty topic", func(c *Config) { c.Kafka.Topics.Logs = "" }, "kafka.topics.logs"},
		{"duplicate topics", func(c *Config) { c.Kafka.Topics.Diffs = c.Kafka.Topics.Captures }, "both"},
		{"dlq is a consumed topic", func(c *Config) { c.Kafka.Topics.DLQ = c.Kafka.Topics.Logs }, "kafka.topics.dlq and kafka.topics.logs"},
		{"bad start offset", func(c *Config) { c.Kafka.StartOffset = "yesterday" }, "kafka.start_offset"},
		{"fetch bytes", func(c *Config) { c.Kafka.FetchMaxBytes = 0 }, "kafka.fetch_max_bytes"},
		{"session timeout", func(c *Config) { c.Kafka.SessionTimeoutMS = -1 }, "kafka.session_timeout_ms"},
		{"rebalance timeout", func(c *Config) { c.Kafka.RebalanceTimeoutMS = 0 }, "kafka.rebalance_timeout_ms"},
		{"sasl without password", func(c *Config) { c.Kafka.SASL.Mechanism = "PLAIN"; c.Kafka.SASL.Username = "u" }, "sasl password"},
		{"tls key without cert", func(c *Config) { c.Kafka.TLS.KeyFile = "k.pem" }, "tls.cert_file and tls.key_file"},
		{"no es urls", func(c *Config) { c.Elasticsearch.URLs = nil }, "elasticsearch.urls is empty"},
		{"es url without scheme", func(c *Config) { c.Elasticsearch.URLs = []string{"localhost:9200"} }, "elasticsearch.urls[0]"},
		{"es url with bad scheme", func(c *Config) { c.Elasticsearch.URLs = []string{"ftp://es:9200"} }, "elasticsearch.urls[0]"},
		{"es username without password", func(c *Config) { c.Elasticsearch.Username = "elastic" }, "elasticsearch password is required"},
		{"es password without username", func(c *Config) { c.Elasticsearch.Password = "x" }, "elasticsearch.username is required"},
		{"es tls pair", func(c *Config) { c.Elasticsearch.TLS.CertFile = "c.pem" }, "elasticsearch.tls.cert_file"},
		{"es timeout", func(c *Config) { c.Elasticsearch.RequestTimeoutMS = 0 }, "elasticsearch.request_timeout_ms"},
		{"empty index", func(c *Config) { c.Indices.Spans = "" }, "indices.spans"},
		{"uppercase index", func(c *Config) { c.Indices.Logs = "Logs-shadowtap-x" }, "indices.logs"},
		{"index with bad character", func(c *Config) { c.Indices.Captures = "shadowtap-captures/x" }, "indices.captures"},
		{"index not matching its template", func(c *Config) { c.Indices.Diffs = "diffs" }, "must start with"},
		{"batch records", func(c *Config) { c.Batch.MaxRecords = 0 }, "batch.max_records"},
		{"batch records too many", func(c *Config) { c.Batch.MaxRecords = maxBatchRecords + 1 }, "batch.max_records"},
		{"batch bytes", func(c *Config) { c.Batch.MaxBytes = 0 }, "batch.max_bytes"},
		{"batch interval", func(c *Config) { c.Batch.MaxIntervalMS = 0 }, "batch.max_interval_ms"},
		{"retry attempts", func(c *Config) { c.Retry.MaxAttempts = 0 }, "retry.max_attempts"},
		{"retry backoff", func(c *Config) { c.Retry.InitialBackoffMS = 0 }, "retry.initial_backoff_ms"},
		{"retry max below initial", func(c *Config) { c.Retry.MaxBackoffMS = c.Retry.InitialBackoffMS - 1 }, "retry.max_backoff_ms"},
		{"shutdown timeout", func(c *Config) { c.ShutdownTimeoutMS = 0 }, "shutdown_timeout_ms"},
		{"telemetry", func(c *Config) { c.Telemetry.SampleRatio = 2 }, "telemetry"},
		{"telemetry endpoint with scheme", func(c *Config) { c.Telemetry.Endpoint = "http://collector:4317" }, "endpoint"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			err := cfg.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Validate = %v, want it to mention %q", err, tt.want)
			}
		})
	}
	t.Run("several problems at once", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Log.Level, cfg.Kafka.Group, cfg.Batch.MaxRecords = "loud", "", 0
		err := cfg.Validate()
		if err == nil {
			t.Fatal("invalid config accepted")
		}
		for _, want := range []string{"log.level", "kafka.group", "batch.max_records"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
	})
	t.Run("valid variations", func(t *testing.T) {
		cfg := DefaultConfig()
		cfg.Kafka.StartOffset = "latest"
		cfg.Elasticsearch.Username, cfg.Elasticsearch.Password = "elastic", "pw"
		cfg.Elasticsearch.APIKey = "key"
		cfg.Indices = Indices{Spans: "traces-shadowtap-prod", Logs: "logs-shadowtap-prod", Captures: "shadowtap-captures-prod", Diffs: "shadowtap-diffs-prod"}
		cfg.Kafka.SASL = kafkaSASL("SCRAM-SHA-512", "u", "p")
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate = %v", err)
		}
	})
}

func TestYAMLOmitsSecrets(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Elasticsearch.Username, cfg.Elasticsearch.Password = "elastic", "es-secret"
	cfg.Elasticsearch.APIKey = "api-secret"
	cfg.Kafka.SASL = kafkaSASL("PLAIN", "indexer", "sasl-secret")
	cfg.Telemetry.Headers = map[string]string{"authorization": "Bearer otel-secret"}
	out, err := cfg.YAML()
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, secret := range []string{"es-secret", "api-secret", "sasl-secret", "otel-secret"} {
		if strings.Contains(s, secret) {
			t.Errorf("YAML contains %q:\n%s", secret, s)
		}
	}
	for _, want := range []string{"username: elastic", "mechanism: PLAIN", "authorization: '***'", "group: shadowtap-indexer", "spans: traces-shadowtap-default", "service_name: shadowtap-indexer"} {
		if !strings.Contains(s, want) {
			t.Errorf("YAML lacks %q:\n%s", want, s)
		}
	}
	// Rendering does not alter the configuration itself.
	if cfg.Telemetry.Headers["authorization"] != "Bearer otel-secret" {
		t.Error("YAML redacted the live configuration")
	}
}

func TestLogHandler(t *testing.T) {
	var buf syncBuffer
	for _, format := range []string{"json", "text"} {
		l := Log{Level: "warn", Format: format}
		logger, err := l.Logger(&buf)
		if err != nil {
			t.Fatal(err)
		}
		logger.Info("hidden")
		logger.Warn("shown", "format", format)
	}
	s := buf.String()
	if strings.Contains(s, "hidden") || strings.Count(s, "shown") != 2 || !strings.Contains(s, `"format":"json"`) || !strings.Contains(s, "format=text") {
		t.Errorf("log output:\n%s", s)
	}
	if _, err := (Log{Level: "loud"}).Handler(&buf); err == nil {
		t.Error("bad level accepted")
	}
}

func TestIndicesAll(t *testing.T) {
	all := DefaultIndices().All()
	if strings.Join(all, ",") != "traces-shadowtap-default,logs-shadowtap-default,shadowtap-captures,shadowtap-diffs" {
		t.Errorf("All = %v", all)
	}
}
