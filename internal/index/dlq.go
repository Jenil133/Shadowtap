package index

import (
	"encoding/base64"
	"encoding/json"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

// DLQSchemaVersion is written to every dead-letter record.
const DLQSchemaVersion = 1

// Dead-letter record header names.
const (
	HeaderSourceTopic     = "source-topic"
	HeaderSourcePartition = "source-partition"
	HeaderSourceOffset    = "source-offset"
	HeaderStage           = "stage"
	HeaderContentType     = "content-type"
)

// Dead-letter stages: where a record was given up on.
const (
	// StageDecode means the record could not be turned into documents.
	StageDecode = "decode"
	// StageIndex means Elasticsearch rejected a document for good.
	StageIndex = "index"
)

// recordFraming covers the record and batch framing (lengths, varints, the
// batch header) around a record's key, value and headers, so a dead-letter
// record built to the budget fits the producer's batch limit.
const recordFraming = 1024

// DLQRecord is the JSON value of a record on the dead-letter topic. It
// locates the original record, says why it was not indexed and keeps what
// could not be indexed: the document, or for an undecodable record the raw
// value.
type DLQRecord struct {
	SchemaVersion int `json:"schema_version"`
	// Indexer names the instance that gave up.
	Indexer  string    `json:"indexer,omitempty"`
	FailedAt time.Time `json:"failed_at"`

	// Topic, Partition, Offset and Timestamp locate the original record.
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp,omitzero"`
	// Key is the record key as text, or KeyBase64 when it is not UTF-8.
	Key       string            `json:"key,omitempty"`
	KeyBase64 string            `json:"key_base64,omitempty"`
	Headers   map[string]string `json:"headers,omitempty"`
	// TraceID is the trace the record belongs to, when its headers say.
	TraceID string `json:"trace_id,omitempty"`

	// Stage is StageDecode or StageIndex.
	Stage string `json:"stage"`
	Error string `json:"error"`
	// Status and ErrorType are the bulk item status and error type for the
	// index stage.
	Status    int    `json:"status,omitempty"`
	ErrorType string `json:"error_type,omitempty"`
	Attempts  int    `json:"attempts,omitempty"`

	// Index and DocumentID identify the document that failed; Document is
	// its body. A decode failure that still produced a partial document
	// keeps it here too.
	Index      string          `json:"index,omitempty"`
	DocumentID string          `json:"document_id,omitempty"`
	Document   json.RawMessage `json:"document,omitempty"`
	// Value is the original record value (base64 in JSON) when it could not
	// be decoded.
	Value []byte `json:"value,omitempty"`
	// Truncated reports that Document and Value were dropped because the
	// record would have exceeded the DLQ record size limit.
	Truncated bool `json:"truncated,omitempty"`
}

// failure is a record, or a document of one, the indexer gave up on.
type failure struct {
	rec      *kgo.Record
	src      Source
	stage    string
	err      error
	status   int
	errType  string
	attempts int
	index    string
	docID    string
	body     []byte // document body, when one was built
	value    bool   // keep the raw record value (decode failures)
}

// dlqRecord renders the failure as a dead-letter record value of at most
// budget bytes (0 for no limit), dropping the payload when it does not fit.
func (f failure) dlqRecord(indexer string, now time.Time, budget int) ([]byte, error) {
	r := DLQRecord{
		SchemaVersion: DLQSchemaVersion,
		Indexer:       indexer,
		FailedAt:      now.UTC(),
		Topic:         f.src.Topic,
		Partition:     f.src.Partition,
		Offset:        f.src.Offset,
		Timestamp:     f.src.Timestamp.UTC(),
		Headers:       f.src.Headers,
		TraceID:       f.src.traceID(),
		Stage:         f.stage,
		Error:         f.err.Error(),
		Status:        f.status,
		ErrorType:     f.errType,
		Attempts:      f.attempts,
		Index:         f.index,
		DocumentID:    f.docID,
	}
	if key := f.rec.Key; len(key) > 0 {
		if utf8.Valid(key) {
			r.Key = string(key)
		} else {
			r.KeyBase64 = base64.StdEncoding.EncodeToString(key)
		}
	}
	if len(f.body) > 0 {
		r.Document = f.body
	}
	if f.value {
		r.Value = f.rec.Value
	}
	b, err := encodeJSON(r)
	if err != nil {
		return nil, err
	}
	if budget > 0 && len(b) > budget {
		r.Document, r.Value, r.Truncated = nil, nil, true
		if b, err = encodeJSON(r); err != nil {
			return nil, err
		}
	}
	return b, nil
}

// headers returns the record headers that let consumers route dead letters
// without decoding them.
func (f failure) headers() map[string]string {
	return map[string]string{
		HeaderContentType:     "application/json",
		HeaderSourceTopic:     f.src.Topic,
		HeaderSourcePartition: strconv.Itoa(int(f.src.Partition)),
		HeaderSourceOffset:    strconv.FormatInt(f.src.Offset, 10),
		HeaderStage:           f.stage,
	}
}

// valueBudget is how large the dead-letter value may be so that the whole
// record, key and headers included, stays within maxRecordBytes. Zero
// means no limit.
func (f failure) valueBudget(maxRecordBytes int) int {
	if maxRecordBytes <= 0 {
		return 0
	}
	overhead := len(f.rec.Key) + recordFraming
	for k, v := range f.headers() {
		overhead += len(k) + len(v)
	}
	return max(maxRecordBytes-overhead, 1)
}
