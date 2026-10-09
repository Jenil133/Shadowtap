package config

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Jenil133/Shadowtap/internal/scrub"
)

const testKey = "0123456789abcdef0123456789abcdef"

func env(kv map[string]string) func(string) string {
	return func(k string) string { return kv[k] }
}

func writeFile(t *testing.T, name, content string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestDefaultsAreValidWithKey(t *testing.T) {
	cfg, err := LoadTap("", env(map[string]string{EnvScrubKey: testKey}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("defaults invalid: %v", err)
	}
	if cfg.Listen.GRPC != ":9002" || cfg.Kafka.ClientID != "shadowtap-tap" || cfg.Sampling.DefaultRate != 0.01 {
		t.Errorf("unexpected defaults: %+v", cfg)
	}
}

func TestFileOverridesDefaults(t *testing.T) {
	path := writeFile(t, "tap.yaml", `
log: {level: debug, format: text}
tap:
  max_body_bytes: 65536
sampling:
  default_rate: 0.5
  rules:
    - name: orders
      methods: [POST]
      path_prefix: /api/orders
      rate: 1
scrub:
  sensitive_keys: [loyalty_id]
kafka:
  brokers: [a:9092, b:9092]
  compression: snappy
`)
	cfg, err := LoadTap(path, env(map[string]string{EnvScrubKey: testKey}))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.Log.Level != "debug" || cfg.Tap.MaxBodyBytes != 65536 || cfg.Sampling.DefaultRate != 0.5 {
		t.Errorf("file values not applied: %+v", cfg)
	}
	if len(cfg.Sampling.Rules) != 1 || cfg.Sampling.Rules[0].Methods[0] != "POST" {
		t.Errorf("rules = %+v", cfg.Sampling.Rules)
	}
	if got := strings.Join(cfg.Kafka.Brokers, ","); got != "a:9092,b:9092" {
		t.Errorf("brokers = %s", got)
	}
	// Unset fields keep their defaults.
	if cfg.Tap.QueueSize != 4096 || cfg.Kafka.Topic != "shadowtap.captures.v1" || len(cfg.Sampling.ExcludePaths) == 0 {
		t.Errorf("defaults lost: %+v", cfg)
	}
	if cfg.Scrub.SensitiveKeys[0] != "loyalty_id" {
		t.Errorf("scrub keys = %v", cfg.Scrub.SensitiveKeys)
	}
}

func TestStrictDecoding(t *testing.T) {
	for _, doc := range []string{
		"kafka:\n  brokerz: [x]\n",
		"unknown: 1\n",
		"tap:\n  max_body_bytes: lots\n",
	} {
		if _, err := LoadTap(writeFile(t, "tap.yaml", doc), env(nil)); err == nil {
			t.Errorf("accepted %q", doc)
		}
	}
	if _, err := LoadTap(filepath.Join(t.TempDir(), "missing.yaml"), env(nil)); err == nil {
		t.Error("missing file accepted")
	}
}

func TestEnvOverrides(t *testing.T) {
	path := writeFile(t, "tap.yaml", "kafka:\n  brokers: [file:9092]\nlisten:\n  grpc: ':1'\n")
	cfg, err := LoadTap(path, env(map[string]string{
		EnvKafkaBrokers: " x:1, y:2 ,,",
		EnvKafkaTopic:   "custom",
		EnvSampleRate:   "0.25",
		EnvLogLevel:     "warn",
		EnvGRPCAddr:     ":7000",
		EnvHTTPAddr:     ":7001",
		EnvPodName:      "tap-abc",
		EnvScrubKey:     testKey,
	}))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(cfg.Kafka.Brokers, ",") != "x:1,y:2" || cfg.Kafka.Topic != "custom" {
		t.Errorf("kafka = %+v", cfg.Kafka)
	}
	if cfg.Sampling.DefaultRate != 0.25 || cfg.Log.Level != "warn" {
		t.Errorf("rate/level = %v/%s", cfg.Sampling.DefaultRate, cfg.Log.Level)
	}
	if cfg.Listen.GRPC != ":7000" || cfg.Listen.HTTP != ":7001" || cfg.Tap.Name != "tap-abc" {
		t.Errorf("listen/name = %+v %q", cfg.Listen, cfg.Tap.Name)
	}
	if _, err := LoadTap("", env(map[string]string{EnvSampleRate: "half"})); err == nil {
		t.Error("bad sample rate accepted")
	}
}

func TestPodNameDoesNotOverrideFile(t *testing.T) {
	cfg, err := LoadTap(writeFile(t, "tap.yaml", "tap:\n  name: explicit\n"), env(map[string]string{EnvPodName: "pod"}))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Tap.Name != "explicit" {
		t.Errorf("name = %q", cfg.Tap.Name)
	}
}

func TestSecrets(t *testing.T) {
	keyFile := writeFile(t, "key", testKey+"\n")
	pwFile := writeFile(t, "pw", "s3cret\r\n")
	path := writeFile(t, "tap.yaml", "scrub:\n  key_file: "+keyFile+"\nkafka:\n  sasl: {mechanism: PLAIN, username: u, password_file: "+pwFile+"}\n")

	cfg, err := LoadTap(path, env(nil))
	if err != nil {
		t.Fatal(err)
	}
	if string(cfg.Scrub.Key) != testKey || cfg.Kafka.SASL.Password != "s3cret" {
		t.Errorf("secrets from files: key=%d bytes pw=%q", len(cfg.Scrub.Key), cfg.Kafka.SASL.Password)
	}

	cfg, err = LoadTap(path, env(map[string]string{EnvScrubKey: "env-key-0123456789", EnvKafkaSASLPassword: "envpw"}))
	if err != nil {
		t.Fatal(err)
	}
	if string(cfg.Scrub.Key) != "env-key-0123456789" || cfg.Kafka.SASL.Password != "envpw" {
		t.Error("env secrets should win over files")
	}

	// Custom key variable name.
	cfg, err = LoadTap(writeFile(t, "tap.yaml", "scrub:\n  key_env: MY_KEY\n"), env(map[string]string{"MY_KEY": testKey}))
	if err != nil || string(cfg.Scrub.Key) != testKey {
		t.Errorf("key_env not honoured: %v", err)
	}

	if _, err := LoadTap(writeFile(t, "tap.yaml", "scrub:\n  key_file: /nonexistent/key\n"), env(nil)); err == nil {
		t.Error("missing key file accepted")
	}
}

func TestSecretsNeverRendered(t *testing.T) {
	cfg, err := LoadTap("", env(map[string]string{EnvScrubKey: testKey, EnvKafkaSASLPassword: "hunter2hunter2"}))
	if err != nil {
		t.Fatal(err)
	}
	out, err := cfg.YAML()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(out, []byte(testKey)) || bytes.Contains(out, []byte("hunter2hunter2")) {
		t.Fatalf("secret rendered:\n%s", out)
	}
	if !bytes.Contains(out, []byte("default_rate")) {
		t.Errorf("rendered config incomplete:\n%s", out)
	}
	// The rendered YAML must load back.
	path := writeFile(t, "rendered.yaml", string(out))
	if _, err := LoadTap(path, env(map[string]string{EnvScrubKey: testKey})); err != nil {
		t.Errorf("rendered config does not reload: %v", err)
	}
}

func TestValidate(t *testing.T) {
	base := func() Tap {
		c := DefaultTap()
		c.Scrub.Key = []byte(testKey)
		return c
	}
	tests := []struct {
		name   string
		mutate func(*Tap)
		want   string
	}{
		{"no key", func(c *Tap) { c.Scrub.Key = nil }, "scrub key"},
		{"short key", func(c *Tap) { c.Scrub.Key = []byte("short") }, "scrub key"},
		// The boundary itself: one byte under the minimum is rejected (the
		// exact minimum is accepted below).
		{"key one byte short", func(c *Tap) { c.Scrub.Key = bytes.Repeat([]byte("k"), scrub.MinKeyBytes-1) }, "scrub key"},
		{"level", func(c *Tap) { c.Log.Level = "loud" }, "log.level"},
		{"format", func(c *Tap) { c.Log.Format = "xml" }, "log.format"},
		{"listen", func(c *Tap) { c.Listen.HTTP = "" }, "listen"},
		{"body small", func(c *Tap) { c.Tap.MaxBodyBytes = 10 }, "max_body_bytes"},
		{"body large", func(c *Tap) { c.Tap.MaxBodyBytes = 1 << 30 }, "max_body_bytes"},
		{"queue", func(c *Tap) { c.Tap.QueueSize = 0 }, "queue_size"},
		{"queue bytes", func(c *Tap) { c.Tap.MaxQueueBytes = 1000 }, "max_queue_bytes"},
		// Exactly two full bodies leaves no room for the per-job allowance:
		// such an exchange would always be dropped.
		{"queue bytes no overhead", func(c *Tap) { c.Tap.MaxQueueBytes = 2 * c.Tap.MaxBodyBytes }, "max_queue_bytes"},
		{"workers", func(c *Tap) { c.Tap.Workers = 0 }, "workers"},
		{"metadata key", func(c *Tap) { c.Tap.ServiceMetadataKey = "" }, "service_metadata_key"},
		{"rate", func(c *Tap) { c.Sampling.DefaultRate = 2 }, "default_rate"},
		{"decoded", func(c *Tap) { c.Scrub.MaxDecodedBytes = -1 }, "max_decoded_bytes"},
		{"brokers", func(c *Tap) { c.Kafka.Brokers = nil }, "broker"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := base()
			tt.mutate(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want mention of %q", err, tt.want)
			}
		})
	}
	// The shortest accepted key is exactly scrub.MinKeyBytes.
	exact := base()
	exact.Scrub.Key = bytes.Repeat([]byte("k"), scrub.MinKeyBytes)
	if err := exact.Validate(); err != nil {
		t.Errorf("a %d-byte key was rejected: %v", scrub.MinKeyBytes, err)
	}
	// All problems are reported together.
	c := base()
	c.Log.Format = "xml"
	c.Tap.QueueSize = 0
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "log.format") || !strings.Contains(err.Error(), "queue_size") {
		t.Errorf("errors not joined: %v", err)
	}
}

func TestLogger(t *testing.T) {
	var buf bytes.Buffer
	l, err := Log{Level: "warn", Format: "json"}.Logger(&buf)
	if err != nil {
		t.Fatal(err)
	}
	l.Info("hidden")
	l.Warn("shown")
	if strings.Contains(buf.String(), "hidden") || !strings.Contains(buf.String(), `"msg":"shown"`) {
		t.Errorf("json logger output: %s", buf.String())
	}
	buf.Reset()
	l, _ = Log{Level: "debug", Format: "text"}.Logger(&buf)
	l.Debug("dbg")
	if !strings.Contains(buf.String(), "msg=dbg") {
		t.Errorf("text logger output: %s", buf.String())
	}
	if _, err := (Log{Level: "nope"}).Logger(&buf); err == nil {
		t.Error("bad level accepted")
	}
}
