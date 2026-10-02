// Package tap implements the Envoy External Processing (ext_proc) server that
// receives sidecar traffic, samples it, and hands finished exchanges to a
// scrubber and a sink. It supports ext_proc observability mode (Envoy never
// waits) and classic inline mode (one no-op reply per message), and never
// mutates traffic.
package tap

import (
	"context"
	"runtime"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
)

// DefaultMaxQueueBytes is the default MaxQueueBytes.
const DefaultMaxQueueBytes = 128 << 20

// DefaultServiceMetadataKey is the gRPC metadata key the EnvoyFilter uses
// (grpc_service.initial_metadata) to tell the tap which service it fronts.
const DefaultServiceMetadataKey = "x-shadowtap-service"

// Config controls the tap.
type Config struct {
	// Name identifies this tap instance in captures (usually the pod name).
	Name string `json:"name"`
	// ServiceMetadataKey is the gRPC metadata key carrying the service name.
	ServiceMetadataKey string `json:"service_metadata_key"`
	// MaxBodyBytes is how much of each body (per direction) is kept.
	MaxBodyBytes int `json:"max_body_bytes"`
	// QueueSize bounds finished exchanges waiting for a worker.
	QueueSize int `json:"queue_size"`
	// MaxQueueBytes bounds the body bytes held by queued exchanges, so a
	// burst of large bodies cannot exhaust memory; 0 means the default.
	MaxQueueBytes int `json:"max_queue_bytes"`
	// Workers scrub and publish exchanges.
	Workers int `json:"workers"`
}

// DefaultConfig returns production defaults.
func DefaultConfig() Config {
	return Config{
		ServiceMetadataKey: DefaultServiceMetadataKey,
		MaxBodyBytes:       128 << 10,
		QueueSize:          4096,
		MaxQueueBytes:      DefaultMaxQueueBytes,
		Workers:            runtime.GOMAXPROCS(0),
	}
}

// Sampler decides whether a request is captured. *sampling.Sampler
// implements it.
type Sampler interface {
	Decide(sampling.Request) sampling.Decision
}

// Scrubber removes personal data from a finished exchange in place before it
// is published. *scrub.Scrubber implements it. On entry, bodies hold the raw
// bytes the tap kept (Body.SetData) with ContentType, Size and Truncated set;
// on return every field is safe to store and x.Redactions holds the counts.
type Scrubber interface {
	Exchange(x *capture.Exchange)
}

// Sink receives scrubbed exchanges. Publish must not block; it returns an
// error when the exchange cannot be accepted (for example because a buffer is
// full).
type Sink interface {
	Publish(ctx context.Context, x *capture.Exchange) error
}
