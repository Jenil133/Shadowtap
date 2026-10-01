package kafka

import (
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// SASL mechanism names as Kafka spells them.
const (
	mechanismPlain       = "PLAIN"
	mechanismSCRAMSHA256 = "SCRAM-SHA-256"
	mechanismSCRAMSHA512 = "SCRAM-SHA-512"
)

const (
	// maxRecordBytesLimit caps MaxRecordBytes. Bigger records would need
	// broker, topic and consumer limits raised everywhere they are read.
	maxRecordBytesLimit = 16 << 20

	// maxLingerMS is the longest linger franz-go accepts; checking it here
	// turns a client construction failure into a config error.
	maxLingerMS = 60_000

	// maxTopicLength is Kafka's limit on topic names.
	maxTopicLength = 249
)

// Validate reports every problem with c in a single error. It does not read
// the TLS files; ClientOptions does that. Zero is valid for four settings:
// LingerMS sends without waiting, MaxBufferedRecords and MaxBufferedBytes use
// their defaults and DeliveryTimeoutMS disables the delivery timeout.
func (c Config) Validate() error {
	problems := c.connectionProblems()
	problems = append(problems, topicProblems(c.Topic)...)
	if _, ok := compressionCodec(c.Compression); !ok {
		problems = append(problems, fmt.Sprintf("compression %q is not one of zstd, snappy, lz4, gzip, none", c.Compression))
	}
	switch {
	case c.LingerMS < 0:
		problems = append(problems, fmt.Sprintf("linger_ms %d is negative", c.LingerMS))
	case c.LingerMS > maxLingerMS:
		problems = append(problems, fmt.Sprintf("linger_ms %d is above %d", c.LingerMS, maxLingerMS))
	}
	if c.MaxBufferedRecords < 0 {
		problems = append(problems, fmt.Sprintf("max_buffered_records %d is negative", c.MaxBufferedRecords))
	}
	if c.MaxRecordBytes <= 0 || c.MaxRecordBytes > maxRecordBytesLimit {
		problems = append(problems, fmt.Sprintf("max_record_bytes %d is not between 1 and %d", c.MaxRecordBytes, maxRecordBytesLimit))
	}
	switch {
	case c.MaxBufferedBytes < 0:
		problems = append(problems, fmt.Sprintf("max_buffered_bytes %d is negative", c.MaxBufferedBytes))
	case c.MaxBufferedBytes > 0 && c.MaxBufferedBytes < c.MaxRecordBytes:
		// The buffer could never hold a record of the maximum size.
		problems = append(problems, fmt.Sprintf("max_buffered_bytes %d is below max_record_bytes %d", c.MaxBufferedBytes, c.MaxRecordBytes))
	}
	switch {
	case c.DeliveryTimeoutMS < 0:
		problems = append(problems, fmt.Sprintf("delivery_timeout_ms %d is negative", c.DeliveryTimeoutMS))
	case c.DeliveryTimeoutMS > 0 && c.DeliveryTimeoutMS <= c.LingerMS:
		// Every record would expire while it lingers.
		problems = append(problems, fmt.Sprintf("delivery_timeout_ms %d must be above linger_ms %d (or 0 for no timeout)", c.DeliveryTimeoutMS, c.LingerMS))
	}
	return configError(problems)
}

// connectionProblems checks the settings every client needs, producer or
// consumer: brokers, SASL and the TLS key pair.
func (c Config) connectionProblems() []string {
	var problems []string
	if len(c.Brokers) == 0 {
		problems = append(problems, "brokers is empty")
	}
	for i, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			problems = append(problems, fmt.Sprintf("brokers[%d] is blank", i))
		}
	}

	switch mechanism, ok := normalizeMechanism(c.SASL.Mechanism); {
	case !ok:
		problems = append(problems, fmt.Sprintf("sasl.mechanism %q is not one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", c.SASL.Mechanism))
	case mechanism != "":
		if c.SASL.Username == "" {
			problems = append(problems, fmt.Sprintf("sasl.username is required for %s", mechanism))
		}
		if c.SASL.Password == "" {
			problems = append(problems, fmt.Sprintf("sasl password is required for %s (sasl.password_file or SHADOWTAP_KAFKA_SASL_PASSWORD)", mechanism))
		}
	}

	if (c.TLS.CertFile == "") != (c.TLS.KeyFile == "") {
		problems = append(problems, "tls.cert_file and tls.key_file must be set together")
	}
	return problems
}

// topicProblems applies Kafka's topic naming rules so a typo fails at startup
// instead of on every produce.
func topicProblems(topic string) []string {
	switch {
	case topic == "":
		return []string{"topic is empty"}
	case len(topic) > maxTopicLength:
		return []string{fmt.Sprintf("topic is longer than %d characters", maxTopicLength)}
	case topic == "." || topic == "..":
		return []string{fmt.Sprintf("topic %q is not allowed", topic)}
	}
	for _, r := range topic {
		if !isTopicRune(r) {
			return []string{fmt.Sprintf("topic %q may only contain ASCII letters, digits, '.', '_' and '-'", topic)}
		}
	}
	return nil
}

func isTopicRune(r rune) bool {
	return r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '_' || r == '-'
}

func configError(problems []string) error {
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("kafka: invalid config: %s", strings.Join(problems, "; "))
}

// normalizeMechanism returns the canonical SASL mechanism name, "" when SASL
// is disabled, and false for anything unsupported.
func normalizeMechanism(m string) (string, bool) {
	switch u := strings.ToUpper(strings.TrimSpace(m)); u {
	case "", mechanismPlain, mechanismSCRAMSHA256, mechanismSCRAMSHA512:
		return u, true
	default:
		return "", false
	}
}

// compressionCodec maps a Config.Compression value to its franz-go codec.
// Names are matched case-insensitively.
func compressionCodec(name string) (kgo.CompressionCodec, bool) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "zstd":
		return kgo.ZstdCompression(), true
	case "snappy":
		return kgo.SnappyCompression(), true
	case "lz4":
		return kgo.Lz4Compression(), true
	case "gzip":
		return kgo.GzipCompression(), true
	case "none":
		return kgo.NoCompression(), true
	default:
		return kgo.CompressionCodec{}, false
	}
}
