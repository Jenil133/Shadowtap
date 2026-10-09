package telemetry

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

func TestValidate(t *testing.T) {
	tests := []struct {
		name   string
		modify func(*Config)
		// wantErr lists substrings the error must contain; empty means valid.
		wantErr []string
	}{
		{name: "defaults", modify: func(*Config) {}},
		{name: "enabled defaults", modify: func(c *Config) { c.Enabled = true }},
		{name: "enabled without service name", modify: func(c *Config) { c.Enabled, c.ServiceName = true, " " }, wantErr: []string{"service_name is required"}},
		{name: "disabled without service name", modify: func(c *Config) { c.ServiceName = "" }},
		{name: "endpoint host and port", modify: func(c *Config) { c.Endpoint = "otel-collector.observability:4317" }},
		{name: "endpoint ipv6", modify: func(c *Config) { c.Endpoint = "[::1]:4317" }},
		{name: "endpoint with scheme", modify: func(c *Config) { c.Endpoint = "http://otel:4317" }, wantErr: []string{"without a scheme"}},
		{name: "endpoint without port", modify: func(c *Config) { c.Endpoint = "otel" }, wantErr: []string{`endpoint "otel" must be host:port`}},
		{name: "endpoint without host", modify: func(c *Config) { c.Endpoint = ":4317" }, wantErr: []string{"must be host:port"}},
		{name: "endpoint named port", modify: func(c *Config) { c.Endpoint = "otel:grpc" }, wantErr: []string{"invalid port"}},
		{name: "endpoint port out of range", modify: func(c *Config) { c.Endpoint = "otel:70000" }, wantErr: []string{"invalid port"}},
		{name: "headers", modify: func(c *Config) { c.Headers = map[string]string{"authorization": "Bearer t", "x-api-key": "k"} }},
		{name: "header empty key", modify: func(c *Config) { c.Headers = map[string]string{"": "v"} }, wantErr: []string{"empty key"}},
		{name: "header key with space", modify: func(c *Config) { c.Headers = map[string]string{"x key": "v"} }, wantErr: []string{`key "x key"`}},
		{name: "header reserved prefix", modify: func(c *Config) { c.Headers = map[string]string{"Grpc-Timeout": "1S"} }, wantErr: []string{"reserved grpc- prefix"}},
		{name: "header control character", modify: func(c *Config) { c.Headers = map[string]string{"x-api-key": "a\nb"} }, wantErr: []string{"printable ASCII"}},
		{name: "sample ratio zero", modify: func(c *Config) { c.SampleRatio = 0 }},
		{name: "sample ratio negative", modify: func(c *Config) { c.SampleRatio = -0.1 }, wantErr: []string{"sample_ratio -0.1"}},
		{name: "sample ratio above one", modify: func(c *Config) { c.SampleRatio = 1.5 }, wantErr: []string{"sample_ratio 1.5"}},
		{name: "sample ratio nan", modify: func(c *Config) { c.SampleRatio = math.NaN() }, wantErr: []string{"sample_ratio"}},
		{name: "timeout zero means default", modify: func(c *Config) { c.TimeoutMS = 0 }},
		{name: "timeout negative", modify: func(c *Config) { c.TimeoutMS = -1 }, wantErr: []string{"timeout_ms -1"}},
		{name: "timeout at one hour", modify: func(c *Config) { c.TimeoutMS = maxTimeoutMS }},
		{name: "timeout above one hour", modify: func(c *Config) { c.TimeoutMS = maxTimeoutMS + 1 }, wantErr: []string{"timeout_ms 3600001 exceeds"}},
		{name: "header keys differing only in case", modify: func(c *Config) { c.Headers = map[string]string{"X-Api-Key": "a", "x-api-key": "b"} }, wantErr: []string{`keys "X-Api-Key" and "x-api-key" differ only in case`}},
		{name: "disabled block is still checked", modify: func(c *Config) { c.Enabled, c.SampleRatio = false, 2 }, wantErr: []string{"sample_ratio 2"}},
		{
			name: "every problem is reported",
			modify: func(c *Config) {
				c.Enabled = true
				c.ServiceName = ""
				c.Endpoint = "otel"
				c.Headers = map[string]string{"b ad": "v", "": "v"}
				c.SampleRatio = 2
				c.TimeoutMS = -5
			},
			wantErr: []string{"service_name", `endpoint "otel"`, "empty key", `key "b ad"`, "sample_ratio 2", "timeout_ms -5"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg)
			err := cfg.Validate()
			if len(tt.wantErr) == 0 {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tt.wantErr)
			}
			if !strings.HasPrefix(err.Error(), "telemetry: invalid config: ") {
				t.Errorf("error %q lacks the package prefix", err)
			}
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not contain %q", err, want)
				}
			}
		})
	}
}

func TestConfigJSONTags(t *testing.T) {
	b, err := json.Marshal(DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	var keys map[string]any
	if err := json.Unmarshal(b, &keys); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"enabled", "service_name", "sample_ratio", "timeout_ms", "metrics"} {
		if _, ok := keys[want]; !ok {
			t.Errorf("default config JSON %s lacks %q", b, want)
		}
	}
	for _, absent := range []string{"endpoint", "insecure", "headers", "service_version", "service_instance"} {
		if _, ok := keys[absent]; ok {
			t.Errorf("default config JSON %s renders empty %q", b, absent)
		}
	}
}

func TestConfigYAML(t *testing.T) {
	const doc = `
enabled: true
endpoint: otel-collector:4317
insecure: true
headers:
  x-api-key: secret
service_name: shadowtap-tap
service_version: 1.2.3
service_instance: tap-0
sample_ratio: 0.25
timeout_ms: 2500
metrics: true
`
	cfg := DefaultConfig()
	if err := yaml.UnmarshalStrict([]byte(doc), &cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	want := Config{
		Enabled:         true,
		Endpoint:        "otel-collector:4317",
		Insecure:        true,
		Headers:         map[string]string{"x-api-key": "secret"},
		ServiceName:     "shadowtap-tap",
		ServiceVersion:  "1.2.3",
		ServiceInstance: "tap-0",
		SampleRatio:     0.25,
		TimeoutMS:       2500,
		Metrics:         true,
	}
	if got, want := jsonString(t, cfg), jsonString(t, want); got != want {
		t.Fatalf("decoded %s, want %s", got, want)
	}
	if cfg.Timeout() != 2500*time.Millisecond {
		t.Errorf("Timeout() = %v, want 2.5s", cfg.Timeout())
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("Validate() = %v", err)
	}

	var strict Config
	if err := yaml.UnmarshalStrict([]byte("timeout: 10s\n"), &strict); err == nil {
		t.Error("strict unmarshal accepted the unknown field timeout")
	}
}

func TestRedacted(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Headers = map[string]string{"authorization": "Bearer secret", "x-api-key": "k"}
	red := cfg.Redacted()
	for k, v := range red.Headers {
		if v != "***" {
			t.Errorf("Redacted header %s = %q", k, v)
		}
	}
	if cfg.Headers["authorization"] != "Bearer secret" {
		t.Error("Redacted modified the original headers")
	}
	if got := DefaultConfig().Redacted(); got.Headers != nil {
		t.Errorf("Redacted of a config without headers = %+v", got)
	}
}

func jsonString(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
