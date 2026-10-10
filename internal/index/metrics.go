package index

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Values of the result label of documents_total.
const (
	resultCreated   = "created"
	resultDuplicate = "duplicate" // 409: an earlier delivery already indexed it
	resultDLQ       = "dlq"
)

// Values of the result label of bulk_requests_total.
const (
	bulkOK      = "ok"
	bulkPartial = "partial" // some items failed
	bulkFailed  = "error"   // the request itself failed
)

// Values of the event label of rebalances_total.
const (
	eventAssigned = "assigned"
	eventRevoked  = "revoked"
	eventLost     = "lost"
)

// metrics holds the indexer's Prometheus collectors. It registers as a
// single collector so registration either fully succeeds or leaves the
// registry untouched.
type metrics struct {
	consumed       *prometheus.CounterVec
	documents      *prometheus.CounterVec
	itemFailures   *prometheus.CounterVec
	dlq            *prometheus.CounterVec
	bulkRequests   *prometheus.CounterVec
	bulkSeconds    prometheus.Histogram
	bulkBytes      prometheus.Histogram
	batchRecords   prometheus.Histogram
	batchDocuments prometheus.Histogram
	commits        *prometheus.CounterVec
	lag            *prometheus.GaugeVec
	rebalances     *prometheus.CounterVec
	fetchErrors    prometheus.Counter
	requestErrors  *prometheus.CounterVec
	bootstraps     *prometheus.CounterVec
}

func newMetrics() *metrics {
	const ns, sub = "shadowtap", "indexer"
	m := &metrics{
		consumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "records_consumed_total",
			Help: "Kafka records polled, by topic.",
		}, []string{"topic"}),
		documents: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "documents_total",
			Help: "Documents by source topic, index and result: created, duplicate (already indexed by an earlier delivery) or dlq (given up and sent to the dead-letter topic).",
		}, []string{"topic", "index", "result"}),
		itemFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "document_failures_total",
			Help: "Bulk items Elasticsearch rejected, by index and HTTP status; retried items count once per attempt.",
		}, []string{"index", "status"}),
		dlq: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "dlq_records_total",
			Help: "Records written to the dead-letter topic, by source topic and stage (decode or index).",
		}, []string{"topic", "stage"}),
		bulkRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "bulk_requests_total",
			Help: "Bulk requests by result: ok, partial (some items failed) or error (the request failed).",
		}, []string{"result"}),
		bulkSeconds: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "bulk_seconds",
			Help:    "Latency of bulk requests that returned a response.",
			Buckets: []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
		}),
		bulkBytes: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "bulk_bytes",
			Help:    "Uncompressed size of bulk request bodies.",
			Buckets: prometheus.ExponentialBuckets(4096, 4, 9), // 4 KiB .. 256 MiB
		}),
		batchRecords: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "batch_records",
			Help:    "Kafka records per batch.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 9), // 1 .. 65536
		}),
		batchDocuments: prometheus.NewHistogram(prometheus.HistogramOpts{
			Namespace: ns, Subsystem: sub, Name: "batch_documents",
			Help:    "Documents per batch; an OTLP record holds many spans or logs.",
			Buckets: prometheus.ExponentialBuckets(1, 4, 9),
		}),
		commits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "commits_total",
			Help: "Offset commits by result (ok or error).",
		}, []string{"result"}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: ns, Subsystem: sub, Name: "consumer_lag",
			Help: "Records between the last fetched offset and the partition's high watermark, from the latest fetch.",
		}, []string{"topic", "partition"}),
		rebalances: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "rebalances_total",
			Help: "Consumer group callbacks by event: assigned, revoked or lost.",
		}, []string{"event"}),
		fetchErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "fetch_errors_total",
			Help: "Fetch errors reported by the Kafka client, other than the indexer's own poll deadlines.",
		}),
		requestErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "elasticsearch_request_errors_total",
			Help: "Bulk requests that failed as a whole, by kind: transport (no response) or the HTTP status.",
		}, []string{"kind"}),
		bootstraps: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: ns, Subsystem: sub, Name: "bootstrap_total",
			Help: "Attempts to apply the ILM policy, templates and data streams, by result (ok or error).",
		}, []string{"result"}),
	}
	for _, r := range []string{bulkOK, bulkPartial, bulkFailed} {
		m.bulkRequests.WithLabelValues(r)
	}
	for _, r := range []string{"ok", "error"} {
		m.commits.WithLabelValues(r)
	}
	return m
}

func (m *metrics) collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.consumed, m.documents, m.itemFailures, m.dlq, m.bulkRequests, m.bulkSeconds, m.bulkBytes,
		m.batchRecords, m.batchDocuments, m.commits, m.lag, m.rebalances, m.fetchErrors, m.requestErrors, m.bootstraps,
	}
}

// Describe implements prometheus.Collector.
func (m *metrics) Describe(ch chan<- *prometheus.Desc) {
	for _, c := range m.collectors() {
		c.Describe(ch)
	}
}

// Collect implements prometheus.Collector.
func (m *metrics) Collect(ch chan<- prometheus.Metric) {
	for _, c := range m.collectors() {
		c.Collect(ch)
	}
}

// setLag records the lag of one partition after a fetch.
func (m *metrics) setLag(topic string, partition int32, lag int64) {
	m.lag.WithLabelValues(topic, strconv.Itoa(int(partition))).Set(float64(max(lag, 0)))
}

// forgetLag drops the lag series of partitions this member no longer owns.
func (m *metrics) forgetLag(partitions map[string][]int32) {
	for topic, ps := range partitions {
		for _, p := range ps {
			m.lag.DeleteLabelValues(topic, strconv.Itoa(int(p)))
		}
	}
}
