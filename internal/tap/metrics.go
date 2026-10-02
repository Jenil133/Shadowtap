package tap

import (
	"fmt"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
)

// Values of the decision label of decisions_total.
const (
	decisionSampled     = sampling.ReasonSampled
	decisionUnsampled   = sampling.ReasonUnsampled
	decisionExcluded    = sampling.ReasonExcluded
	decisionRateLimited = sampling.ReasonRateLimited
)

// Values of the outcome label of exchanges_total.
const (
	outcomePublished  = "published"
	outcomeQueueFull  = "queue_full"
	outcomeIncomplete = "incomplete"
	outcomeInvalid    = "invalid"
	outcomeSinkError  = "sink_error"
	outcomeShutdown   = "shutdown"
	outcomePanic      = "panic"
)

// msgKind is the type of an ext_proc message, used for replies and as the
// type label of message_seconds.
type msgKind uint8

const (
	kindRequestHeaders msgKind = iota
	kindRequestBody
	kindRequestTrailers
	kindResponseHeaders
	kindResponseBody
	kindResponseTrailers
	numKinds
)

func (k msgKind) String() string {
	switch k {
	case kindRequestHeaders:
		return "request_headers"
	case kindRequestBody:
		return "request_body"
	case kindRequestTrailers:
		return "request_trailers"
	case kindResponseHeaders:
		return "response_headers"
	case kindResponseBody:
		return "response_body"
	case kindResponseTrailers:
		return "response_trailers"
	}
	return "unknown"
}

// metrics holds the tap's Prometheus collectors. They exist even when no
// registerer is given, so the rest of the package never checks for nil.
type metrics struct {
	streamsActive  prometheus.Gauge
	streams        *prometheus.CounterVec
	decisions      *prometheus.CounterVec
	exchanges      *prometheus.CounterVec
	redactions     *prometheus.CounterVec
	bodyOmitted    *prometheus.CounterVec
	queueDepth     prometheus.GaugeFunc
	messageSeconds *prometheus.HistogramVec
	processSeconds prometheus.Histogram

	// message holds the message_seconds child for each kind so the inline
	// path skips the label lookup.
	message [numKinds]prometheus.Observer
}

func newMetrics(queueLen func() int) *metrics {
	const ns, sub = "shadowtap", "tap"
	m := &metrics{
		streamsActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "streams_active",
			Help: "ext_proc streams currently open.",
		}),
		streams: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "streams_total",
			Help: "ext_proc streams that sent at least one message, by mode (observe or inline).",
		}, []string{"mode"}),
		decisions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "decisions_total",
			Help: "Sampling decisions by service and decision (sampled, unsampled, excluded, rate_limited).",
		}, []string{"service", "decision"}),
		exchanges: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "exchanges_total",
			Help: "Sampled exchanges by service and outcome (published, queue_full, incomplete, invalid, sink_error, shutdown, panic).",
		}, []string{"service", "outcome"}),
		redactions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "redactions_total",
			Help: "Values redacted by the scrubber, by kind.",
		}, []string{"kind"}),
		bodyOmitted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "body_omitted_total",
			Help: "Request and response bodies whose data the scrubber dropped, by reason.",
		}, []string{"reason"}),
		queueDepth: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "queue_depth",
			Help: "Finished exchanges waiting for a worker.",
		}, func() float64 { return float64(queueLen()) }),
		messageSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "message_seconds",
			Help: "Inline mode: time Envoy waits on the tap per message, from receiving it to sending the reply, by message type.",
			// Sub-millisecond resolution up to Envoy's typical 200ms message timeout.
			Buckets: []float64{.00001, .000025, .00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .2, .5},
		}, []string{"type"}),
		processSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "process_seconds",
			Help:    "Worker time per exchange: scrubbing and handing it to the sink.",
			Buckets: []float64{.00005, .0001, .00025, .0005, .001, .0025, .005, .01, .025, .05, .1, .25, .5, 1},
		}),
	}
	for k := range numKinds {
		m.message[k] = m.messageSeconds.WithLabelValues(k.String())
	}
	for _, mode := range []string{capture.ModeObserve, capture.ModeInline} {
		m.streams.WithLabelValues(mode)
	}
	return m
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.streamsActive, m.streams, m.decisions, m.exchanges, m.redactions,
		m.bodyOmitted, m.queueDepth, m.messageSeconds, m.processSeconds,
	}
}

// register registers every collector on reg, or none of them: on failure the
// ones already registered are removed again so a retry with another
// registerer is clean.
func (m *metrics) register(reg prometheus.Registerer) error {
	if reg == nil {
		return nil
	}
	var done []prometheus.Collector
	for _, c := range m.collectors() {
		if err := reg.Register(c); err != nil {
			for _, d := range done {
				reg.Unregister(d)
			}
			return fmt.Errorf("tap: register metrics: %w", err)
		}
		done = append(done, c)
	}
	return nil
}

// exchange counts one sampled exchange's outcome.
func (m *metrics) exchange(service, outcome string) {
	m.exchanges.WithLabelValues(service, outcome).Inc()
}

// scrubbed counts what the scrubber did to x.
func (m *metrics) scrubbed(x *capture.Exchange) {
	for kind, n := range x.Redactions {
		if n > 0 {
			m.redactions.WithLabelValues(kind).Add(float64(n))
		}
	}
	for _, b := range [...]*capture.Body{&x.Request.Body, &x.Response.Body} {
		if b.Omitted != "" {
			m.bodyOmitted.WithLabelValues(b.Omitted).Inc()
		}
	}
}

// decisionLabel maps a sampling decision onto the fixed decision label values,
// so a custom Sampler cannot add label values.
func decisionLabel(d sampling.Decision) string {
	if d.Sampled {
		return decisionSampled
	}
	switch d.Reason {
	case sampling.ReasonExcluded:
		return decisionExcluded
	case sampling.ReasonRateLimited:
		return decisionRateLimited
	}
	return decisionUnsampled
}
