package kafka

import (
	"strings"
	"testing"
)

func TestValidate(t *testing.T) {
	withSASL := func(mechanism, user, pass string) func(*Config) {
		return func(c *Config) {
			c.SASL = SASLConfig{Mechanism: mechanism, Username: user, Password: pass}
		}
	}
	tests := []struct {
		name   string
		modify func(*Config)
		// wantErr lists substrings the error must contain; empty means valid.
		wantErr []string
	}{
		{name: "defaults", modify: func(*Config) {}},
		{name: "no brokers", modify: func(c *Config) { c.Brokers = nil }, wantErr: []string{"brokers is empty"}},
		{name: "blank broker", modify: func(c *Config) { c.Brokers = []string{"kafka:9092", "  "} }, wantErr: []string{"brokers[1] is blank"}},
		{name: "empty topic", modify: func(c *Config) { c.Topic = "" }, wantErr: []string{"topic is empty"}},
		{name: "topic with space", modify: func(c *Config) { c.Topic = "shadowtap captures" }, wantErr: []string{"topic"}},
		{name: "topic too long", modify: func(c *Config) { c.Topic = strings.Repeat("t", 250) }, wantErr: []string{"topic"}},
		{name: "topic dot", modify: func(c *Config) { c.Topic = "." }, wantErr: []string{"topic"}},
		{name: "topic max length", modify: func(c *Config) { c.Topic = strings.Repeat("t", 249) }},
		{name: "compression empty", modify: func(c *Config) { c.Compression = "" }, wantErr: []string{"compression"}},
		{name: "compression unknown", modify: func(c *Config) { c.Compression = "brotli" }, wantErr: []string{`compression "brotli"`}},
		{name: "compression snappy", modify: func(c *Config) { c.Compression = "snappy" }},
		{name: "compression lz4", modify: func(c *Config) { c.Compression = "lz4" }},
		{name: "compression gzip", modify: func(c *Config) { c.Compression = "gzip" }},
		{name: "compression none", modify: func(c *Config) { c.Compression = "none" }},
		{name: "compression upper case", modify: func(c *Config) { c.Compression = "ZSTD" }},
		{name: "linger negative", modify: func(c *Config) { c.LingerMS = -1 }, wantErr: []string{"linger_ms -1"}},
		{name: "linger zero", modify: func(c *Config) { c.LingerMS = 0 }},
		{name: "linger above client limit", modify: func(c *Config) { c.LingerMS = 60_001 }, wantErr: []string{"linger_ms 60001"}},
		{name: "max buffered negative", modify: func(c *Config) { c.MaxBufferedRecords = -1 }, wantErr: []string{"max_buffered_records"}},
		{name: "max buffered zero means default", modify: func(c *Config) { c.MaxBufferedRecords = 0 }},
		{name: "max record zero", modify: func(c *Config) { c.MaxRecordBytes = 0 }, wantErr: []string{"max_record_bytes 0"}},
		{name: "max record negative", modify: func(c *Config) { c.MaxRecordBytes = -5 }, wantErr: []string{"max_record_bytes -5"}},
		{name: "max record at limit", modify: func(c *Config) { c.MaxRecordBytes = 16 << 20 }},
		{name: "max record above limit", modify: func(c *Config) { c.MaxRecordBytes = 16<<20 + 1 }, wantErr: []string{"max_record_bytes"}},
		{name: "delivery timeout negative", modify: func(c *Config) { c.DeliveryTimeoutMS = -1 }, wantErr: []string{"delivery_timeout_ms -1"}},
		{name: "delivery timeout zero disables it", modify: func(c *Config) { c.DeliveryTimeoutMS = 0 }},
		{name: "delivery timeout short", modify: func(c *Config) { c.DeliveryTimeoutMS = 500 }},
		{name: "delivery timeout within linger", modify: func(c *Config) { c.LingerMS, c.DeliveryTimeoutMS = 50, 50 }, wantErr: []string{"delivery_timeout_ms 50 must be above linger_ms 50"}},
		{name: "no delivery timeout with linger", modify: func(c *Config) { c.LingerMS, c.DeliveryTimeoutMS = 50, 0 }},
		{name: "sasl plain", modify: withSASL("PLAIN", "tap", "secret")},
		{name: "sasl scram-sha-256 lower case", modify: withSASL("scram-sha-256", "tap", "secret")},
		{name: "sasl scram-sha-512", modify: withSASL("SCRAM-SHA-512", "tap", "secret")},
		{name: "sasl unsupported", modify: withSASL("GSSAPI", "tap", "secret"), wantErr: []string{`sasl.mechanism "GSSAPI"`}},
		{name: "sasl without username", modify: withSASL("PLAIN", "", "secret"), wantErr: []string{"sasl.username"}},
		{name: "sasl without password", modify: withSASL("SCRAM-SHA-256", "tap", ""), wantErr: []string{"sasl password"}},
		{name: "credentials without mechanism are ignored", modify: withSASL("", "tap", "")},
		{name: "tls cert without key", modify: func(c *Config) { c.TLS.CertFile = "client.pem" }, wantErr: []string{"tls.cert_file and tls.key_file"}},
		{name: "tls key without cert", modify: func(c *Config) { c.TLS.KeyFile = "client-key.pem" }, wantErr: []string{"tls.cert_file and tls.key_file"}},
		{name: "tls key pair", modify: func(c *Config) {
			c.TLS = TLSConfig{Enabled: true, CertFile: "client.pem", KeyFile: "client-key.pem"}
		}},
		{
			name: "every problem is reported",
			modify: func(c *Config) {
				c.Brokers = nil
				c.Topic = ""
				c.Compression = "brotli"
				c.MaxRecordBytes = 0
			},
			wantErr: []string{"brokers is empty", "topic is empty", "compression", "max_record_bytes"},
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
			for _, want := range tt.wantErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Validate() = %q, want it to contain %q", err, want)
				}
			}
		})
	}
}
