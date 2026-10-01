package kafka

import "github.com/prometheus/client_golang/prometheus"

// Values of the result label on shadowtap_kafka_records_total.
const (
	resultOK           = "ok"
	resultError        = "error"
	resultBackpressure = "backpressure"
	resultTooLarge     = "too_large"
)

// producerMetrics belongs to one Producer. It registers as a single collector
// so registration either fully succeeds or leaves the registry untouched.
type producerMetrics struct {
	records        *prometheus.CounterVec
	ok             prometheus.Counter
	failed         prometheus.Counter
	backpressure   prometheus.Counter
	tooLarge       prometheus.Counter
	produceSeconds prometheus.Histogram
	recordBytes    prometheus.Histogram
	buffered       prometheus.GaugeFunc
}

func newProducerMetrics(buffered func() float64) *producerMetrics {
	m := &producerMetrics{
		records: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "shadowtap",
			Subsystem: "kafka",
			Name:      "records_total",
			Help: "Captures handed to the producer by outcome: ok (acknowledged by Kafka), error (not stored), " +
				"backpressure (dropped because the producer buffer was full), too_large (over max_record_bytes even without bodies, " +
				"or rejected by the broker as too large).",
		}, []string{"result"}),
		produceSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "shadowtap",
			Subsystem: "kafka",
			Name:      "produce_seconds",
			Help:      "Time from Publish to the broker acknowledging the record, for acknowledged records.",
			Buckets:   []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}),
		recordBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: "shadowtap",
			Subsystem: "kafka",
			Name:      "record_bytes",
			Help:      "Encoded size of capture records handed to the Kafka client.",
			Buckets:   prometheus.ExponentialBuckets(256, 4, 9), // 256 B .. 16 MiB
		}),
		buffered: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Namespace: "shadowtap",
			Subsystem: "kafka",
			Name:      "buffered_records",
			Help:      "Records buffered in the Kafka client and not yet acknowledged or failed.",
		}, buffered),
	}
	// Resolving the children up front keeps label lookups off the hot path
	// and exports every result as zero from the start.
	m.ok = m.records.WithLabelValues(resultOK)
	m.failed = m.records.WithLabelValues(resultError)
	m.backpressure = m.records.WithLabelValues(resultBackpressure)
	m.tooLarge = m.records.WithLabelValues(resultTooLarge)
	return m
}

func (m *producerMetrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{m.records, m.produceSeconds, m.recordBytes, m.buffered}
}

// Describe implements prometheus.Collector.
func (m *producerMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range m.collectors() {
		c.Describe(ch)
	}
}

// Collect implements prometheus.Collector.
func (m *producerMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, c := range m.collectors() {
		c.Collect(ch)
	}
}
