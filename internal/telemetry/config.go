package telemetry

import (
	"fmt"
	"maps"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Environment variables Setup reads for resource attributes. The Kubernetes
// manifests set them from the downward API.
const (
	EnvPodName      = "POD_NAME"
	EnvPodNamespace = "POD_NAMESPACE"
	EnvNodeName     = "NODE_NAME"
)

// DefaultServiceName is the service.name used when a component does not set
// its own.
const DefaultServiceName = "shadowtap"

// maxTimeoutMS caps timeout_ms. Nothing legitimate waits longer than an hour
// for one export, and far larger values overflow time.Duration.
const maxTimeoutMS = int(time.Hour / time.Millisecond)

// Config controls what a process exports over OTLP and how it identifies
// itself. Endpoint, Insecure, Headers and TimeoutMS are only applied when
// set; otherwise the standard OTEL_EXPORTER_OTLP_* environment variables, and
// then the exporter defaults, apply.
type Config struct {
	// Enabled turns on export. When false, Setup returns no-op providers and
	// starts nothing.
	Enabled bool `json:"enabled"`
	// Endpoint is the host:port of the OTLP gRPC collector, without a scheme.
	// Empty defers to OTEL_EXPORTER_OTLP_ENDPOINT, then localhost:4317.
	Endpoint string `json:"endpoint,omitempty"`
	// Insecure disables TLS to the collector. False defers to the environment
	// (OTEL_EXPORTER_OTLP_INSECURE, or the scheme of an OTEL_EXPORTER_OTLP_*
	// endpoint variable), whose default is TLS.
	Insecure bool `json:"insecure,omitempty"`
	// Headers are sent as gRPC metadata on every export request, typically
	// for authentication. Keys must be valid gRPC metadata keys.
	Headers map[string]string `json:"headers,omitempty"`
	// ServiceName is the service.name resource attribute; required when
	// enabled.
	ServiceName string `json:"service_name"`
	// ServiceVersion is the service.version resource attribute.
	ServiceVersion string `json:"service_version,omitempty"`
	// ServiceInstance is the service.instance.id resource attribute. Empty
	// uses POD_NAME, then the hostname.
	ServiceInstance string `json:"service_instance,omitempty"`
	// SampleRatio is the fraction of root traces to sample, 0 to 1. Spans
	// with a parent follow the parent's decision.
	SampleRatio float64 `json:"sample_ratio"`
	// TimeoutMS bounds each export request, up to an hour. Zero defers to
	// OTEL_EXPORTER_OTLP_TIMEOUT, then the exporter default of 10 s.
	TimeoutMS int `json:"timeout_ms"`
	// Metrics also exports metrics over OTLP. Off by default because
	// Prometheus scrapes the admin server.
	Metrics bool `json:"metrics"`
}

// DefaultConfig returns a disabled configuration that samples every root
// trace once enabled.
func DefaultConfig() Config {
	return Config{
		ServiceName: DefaultServiceName,
		SampleRatio: 1,
	}
}

// Timeout returns TimeoutMS as a duration.
func (c Config) Timeout() time.Duration {
	return time.Duration(c.TimeoutMS) * time.Millisecond
}

// Redacted returns a copy of c with header values masked, for rendering the
// configuration without leaking credentials.
func (c Config) Redacted() Config {
	if len(c.Headers) == 0 {
		return c
	}
	c.Headers = maps.Clone(c.Headers)
	for k := range c.Headers {
		c.Headers[k] = "***"
	}
	return c
}

// Validate reports every problem with c in a single error. Field values are
// checked whether or not export is enabled, so a disabled block with a typo
// fails before someone enables it; the service name is only required when
// enabled.
func (c Config) Validate() error {
	var problems []string
	if c.Enabled && strings.TrimSpace(c.ServiceName) == "" {
		problems = append(problems, "service_name is required when enabled")
	}
	if c.Endpoint != "" {
		if err := checkEndpoint(c.Endpoint); err != nil {
			problems = append(problems, err.Error())
		}
	}
	// gRPC lowercases metadata keys, so two keys differing only in case
	// would be sent as one key with two values.
	seen := make(map[string]string, len(c.Headers))
	for _, k := range slices.Sorted(maps.Keys(c.Headers)) {
		if err := checkHeader(k, c.Headers[k]); err != nil {
			problems = append(problems, err.Error())
			continue
		}
		lower := strings.ToLower(k)
		if prev, dup := seen[lower]; dup {
			problems = append(problems, fmt.Sprintf("headers: keys %q and %q differ only in case", prev, k))
			continue
		}
		seen[lower] = k
	}
	if math.IsNaN(c.SampleRatio) || c.SampleRatio < 0 || c.SampleRatio > 1 {
		problems = append(problems, fmt.Sprintf("sample_ratio %v is not between 0 and 1", c.SampleRatio))
	}
	switch {
	case c.TimeoutMS < 0:
		problems = append(problems, fmt.Sprintf("timeout_ms %d is negative", c.TimeoutMS))
	case c.TimeoutMS > maxTimeoutMS:
		problems = append(problems, fmt.Sprintf("timeout_ms %d exceeds the maximum of %d (1h)", c.TimeoutMS, maxTimeoutMS))
	}
	if len(problems) == 0 {
		return nil
	}
	return fmt.Errorf("telemetry: invalid config: %s", strings.Join(problems, "; "))
}

// checkEndpoint accepts host:port only. A scheme would be passed to gRPC as
// part of the target and fail at the first export, so it is rejected here.
func checkEndpoint(endpoint string) error {
	if strings.Contains(endpoint, "://") {
		return fmt.Errorf("endpoint %q must be host:port without a scheme (set insecure for plaintext)", endpoint)
	}
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil || host == "" {
		return fmt.Errorf("endpoint %q must be host:port", endpoint)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("endpoint %q has an invalid port", endpoint)
	}
	return nil
}

// checkHeader applies the gRPC metadata rules so a bad header fails at
// startup instead of on every export: keys are letters, digits, '-', '_' and
// '.', never the reserved grpc- prefix, and values are printable ASCII.
func checkHeader(key, value string) error {
	if key == "" {
		return fmt.Errorf("headers: empty key")
	}
	for i := 0; i < len(key); i++ {
		if !isMetadataKeyByte(key[i]) {
			return fmt.Errorf("headers: key %q may only contain letters, digits, '-', '_' and '.'", key)
		}
	}
	if strings.HasPrefix(strings.ToLower(key), "grpc-") {
		return fmt.Errorf("headers: key %q uses the reserved grpc- prefix", key)
	}
	for i := 0; i < len(value); i++ {
		if value[i] < 0x20 || value[i] > 0x7e {
			return fmt.Errorf("headers: value of %q must be printable ASCII", key)
		}
	}
	return nil
}

func isMetadataKeyByte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.'
}
