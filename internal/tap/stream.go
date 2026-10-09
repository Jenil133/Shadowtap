package tap

import (
	"context"
	"errors"
	"io"
	"strconv"
	"strings"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	filterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/Jenil133/Shadowtap/internal/capture"
	"github.com/Jenil133/Shadowtap/internal/sampling"
	"github.com/Jenil133/Shadowtap/internal/tracectx"
)

// Process serves one ext_proc stream, which Envoy opens per HTTP request.
//
// The request headers are sampled first. An unsampled stream is closed at
// once in observability mode (Envoy then sends nothing more); in inline mode
// the reply carries a mode_override that tells Envoy to skip the rest, and any
// message Envoy sends anyway is still answered. A sampled request keeps its
// headers, trailers and up to MaxBodyBytes of each body. The exchange is
// finalized exactly once — when the response ends, or when the stream ends
// after the response headers — and queued without blocking; a stream that ends
// before the response headers is dropped as incomplete.
//
// Inline messages are answered before any capture work; observability mode
// never sends. A stream that ends normally (EOF, cancellation, deadline)
// returns nil; any other receive or send error is returned after finalizing.
func (s *Server) Process(rpc extprocv3.ExternalProcessor_ProcessServer) (err error) {
	s.metrics.streamsActive.Inc()
	defer s.metrics.streamsActive.Dec()
	st := &stream{s: s, rpc: rpc}
	defer func() {
		// A bug here must cost one stream, not every stream on the tap.
		if r := recover(); r != nil {
			s.logPanic(rpc.Context(), "processing an ext_proc stream", r)
			if st.capturing() {
				s.metrics.exchange(st.label, outcomePanic)
			}
			err = status.Error(codes.Internal, "tap: internal error")
		}
	}()
	for {
		req, rerr := rpc.Recv()
		if rerr != nil {
			return st.end(rerr)
		}
		stop, herr := st.handle(req)
		if herr != nil {
			return st.end(herr)
		}
		if stop {
			return nil
		}
	}
}

// phase is where a stream is in its HTTP exchange.
type phase uint8

const (
	phaseStart    phase = iota // waiting for the request headers
	phaseRequest               // sampled; capturing the request
	phaseResponse              // response headers seen; capturing the response
	phaseIdle                  // nothing (more) to capture
)

// stream is the state of one ext_proc stream. Only the goroutine running
// Process touches it.
type stream struct {
	s       *Server
	rpc     extprocv3.ExternalProcessor_ProcessServer
	counted bool // streams_total incremented
	phase   phase
	label   string // service metrics label
	x       *capture.Exchange
	req     direction
	resp    direction

	// Body send modes from protocol_config, which Envoy puts on the first
	// message of a stream; unknown with older Envoy.
	modesKnown        bool
	reqMode, respMode filterv3.ProcessingMode_BodySendMode
}

func (st *stream) capturing() bool {
	return st.phase == phaseRequest || st.phase == phaseResponse
}

// handle processes one message and reports whether the tap is done with the
// stream, in which case Process closes it.
func (st *stream) handle(req *extprocv3.ProcessingRequest) (stop bool, err error) {
	inline := !req.GetObservabilityMode()
	var recvAt time.Time // start of the inline wait measured by message_seconds
	if inline {
		recvAt = time.Now()
	}
	if !st.counted {
		st.counted = true
		st.s.metrics.streams.WithLabelValues(modeName(inline)).Inc()
		if pc := req.GetProtocolConfig(); pc != nil {
			st.modesKnown = true
			st.reqMode, st.respMode = pc.GetRequestBodyMode(), pc.GetResponseBodyMode()
		}
	}
	if st.phase == phaseStart {
		if _, ok := req.GetRequest().(*extprocv3.ProcessingRequest_RequestHeaders); !ok {
			// Without request headers there is nothing to capture; an
			// observability stream can be closed straight away.
			st.phase = phaseIdle
			if !inline {
				return true, nil
			}
		}
	}

	switch m := req.GetRequest().(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return st.requestHeaders(m.RequestHeaders, inline, recvAt)
	case *extprocv3.ProcessingRequest_RequestBody:
		err = st.reply(inline, kindRequestBody, recvAt, nil)
		st.requestBody(m.RequestBody)
	case *extprocv3.ProcessingRequest_RequestTrailers:
		err = st.reply(inline, kindRequestTrailers, recvAt, nil)
		st.requestTrailers(m.RequestTrailers)
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		var at time.Time
		if st.phase == phaseRequest {
			at = st.s.now()
		}
		err = st.reply(inline, kindResponseHeaders, recvAt, nil)
		stop = st.responseHeaders(m.ResponseHeaders, at)
	case *extprocv3.ProcessingRequest_ResponseBody:
		err = st.reply(inline, kindResponseBody, recvAt, nil)
		stop = st.responseBody(m.ResponseBody)
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		err = st.reply(inline, kindResponseTrailers, recvAt, nil)
		stop = st.responseTrailers(m.ResponseTrailers)
	default:
		// There is no matching reply for a message we do not know, so an
		// inline stream can only be closed.
		if inline {
			return true, status.Errorf(codes.InvalidArgument, "tap: unsupported processing request %T", m)
		}
	}
	return stop, err
}

// reply answers an inline message with the matching no-op response and
// records how long Envoy waited for it. Observability mode never replies.
func (st *stream) reply(inline bool, k msgKind, recvAt time.Time, override *filterv3.ProcessingMode) error {
	if !inline {
		return nil
	}
	err := st.rpc.Send(noopReply(k, override))
	st.s.metrics.message[k].Observe(time.Since(recvAt).Seconds())
	return err
}

func (st *stream) requestHeaders(h *extprocv3.HttpHeaders, inline bool, recvAt time.Time) (bool, error) {
	if st.phase != phaseStart {
		// Envoy sends request headers once per stream; only the first counts.
		return false, st.reply(inline, kindRequestHeaders, recvAt, nil)
	}
	startedAt := st.s.now()
	hdrs := h.GetHeaders().GetHeaders()
	p := pseudoHeaders(hdrs)
	service := serviceName(st.metadataService(), p.authority)
	d := st.s.sampler.Decide(sampling.Request{
		Service: service,
		Method:  p.method,
		Path:    p.path,
		Header:  headerList(hdrs).get,
	})
	var override *filterv3.ProcessingMode
	if !d.Sampled {
		override = skipRest()
	}
	err := st.reply(inline, kindRequestHeaders, recvAt, override)

	st.label = st.s.services.label(service)
	st.s.metrics.decisions.WithLabelValues(st.label, decisionLabel(d)).Inc()
	if !d.Sampled {
		st.phase = phaseIdle
		// Closing an observability stream tells Envoy no more messages are
		// needed.
		return !inline, err
	}
	st.begin(h, p, service, d, inline, startedAt)
	return false, err
}

// begin starts capturing a sampled request.
func (st *stream) begin(h *extprocv3.HttpHeaders, p pseudo, service string, d sampling.Decision, inline bool, startedAt time.Time) {
	headers := toHeaders(h.GetHeaders().GetHeaders())
	x := &capture.Exchange{
		SchemaVersion: capture.SchemaVersion,
		ID:            capture.NewID(),
		Service:       service,
		RequestID:     headers.Get("x-request-id"),
		Source:        capture.Source{Tap: st.s.cfg.Name, Mode: modeName(inline)},
		Sampling:      capture.Sampling{Rate: d.Rate, Rule: d.Rule, KeySource: d.KeySource},
		Timing:        capture.Timing{StartedAt: startedAt},
		Request: capture.Request{
			Method:    p.method,
			Scheme:    p.scheme,
			Authority: p.authority,
			Path:      p.path,
			Headers:   headers,
		},
	}
	if tc, ok := tracectx.FromHeaders(headers.Get); ok {
		x.TraceID, x.ParentSpanID = tc.TraceID, tc.SpanID
	}
	st.x = x
	st.phase = phaseRequest
	st.req.hint = contentLength(headers)
	if h.GetEndOfStream() {
		st.req.close(true)
	}
}

func (st *stream) requestBody(b *extprocv3.HttpBody) {
	if st.phase != phaseRequest {
		return
	}
	st.req.write(b.GetBody(), st.s.cfg.MaxBodyBytes)
	if b.GetEndOfStream() || b.GetEndOfStreamWithoutMessage() {
		st.req.close(true)
	}
}

// requestTrailers records the request trailers, which close the direction.
func (st *stream) requestTrailers(t *extprocv3.HttpTrailers) {
	if st.phase != phaseRequest {
		return
	}
	st.x.Request.Trailers = toHeaders(t.GetTrailers().GetHeaders())
	st.req.close(st.trailersEndBody(&st.req, st.reqMode))
}

// trailersEndBody reports whether trailers prove the body before them was
// seen in full. Envoy keeps end_of_stream off the last body message when
// trailers follow, so trailers normally mark the end of the body. Inline
// BUFFERED_PARTIAL is the exception: a body Envoy cut at its buffer limit is
// followed by the trailers just the same, so it only counts as complete when
// its content-length matches what arrived. Marking a complete body truncated
// costs little (the scrubber cuts it back); the reverse lets a PII value split
// at the cut slip past the detectors.
func (st *stream) trailersEndBody(d *direction, mode filterv3.ProcessingMode_BodySendMode) bool {
	switch {
	case st.x.Source.Mode != capture.ModeInline || d.seen == 0:
		// Observability mode only streams bodies, and an empty body has
		// nothing to cut.
		return true
	case st.modesKnown && mode != filterv3.ProcessingMode_BUFFERED_PARTIAL:
		// STREAMED and GRPC send everything; BUFFERED fails the request
		// rather than send a partial body.
		return true
	}
	return d.hint > 0 && int64(d.hint) == d.seen
}

// responseHeaders records the response start and reports whether the
// exchange was finalized (a response without body or trailers).
func (st *stream) responseHeaders(h *extprocv3.HttpHeaders, at time.Time) bool {
	if st.phase != phaseRequest {
		return false
	}
	st.req.close(false) // a request body still open now was not seen in full
	hdrs := h.GetHeaders().GetHeaders()
	headers := toHeaders(hdrs)
	st.x.Response.Status = pseudoHeaders(hdrs).statusCode()
	st.x.Response.Headers = headers
	st.x.Timing.ResponseStartedAt = at
	st.resp.hint = contentLength(headers)
	st.phase = phaseResponse
	if h.GetEndOfStream() {
		st.resp.close(true)
		st.finalize()
		return true
	}
	return false
}

func (st *stream) responseBody(b *extprocv3.HttpBody) bool {
	if st.phase != phaseResponse {
		return false
	}
	st.resp.write(b.GetBody(), st.s.cfg.MaxBodyBytes)
	if b.GetEndOfStream() || b.GetEndOfStreamWithoutMessage() {
		st.resp.close(true)
		st.finalize()
		return true
	}
	return false
}

func (st *stream) responseTrailers(t *extprocv3.HttpTrailers) bool {
	if st.phase != phaseResponse {
		return false
	}
	st.x.Response.Trailers = toHeaders(t.GetTrailers().GetHeaders())
	st.resp.close(st.trailersEndBody(&st.resp, st.respMode))
	st.finalize()
	return true
}

// finalize completes the exchange and queues it. The phase becomes idle, so
// it runs at most once per stream.
func (st *stream) finalize() {
	x := st.x
	x.Timing.CompletedAt = st.s.now()
	x.Timing.DurationMicros = x.Timing.ResponseStartedAt.Sub(x.Timing.StartedAt).Microseconds()
	st.req.close(false)
	st.resp.close(false)
	x.Request.Body = st.req.body(x.Request.Headers.Get("content-type"))
	x.Response.Body = st.resp.body(x.Response.Headers.Get("content-type"))
	j := job{x: x, reqBody: st.req.data, respBody: st.resp.data, service: st.label}
	st.x, st.req, st.resp, st.phase = nil, direction{}, direction{}, phaseIdle
	st.s.submit(j)
}

// end handles the end of the stream, err being why it ended, and returns what
// Process should return.
func (st *stream) end(err error) error {
	switch st.phase {
	case phaseResponse:
		st.finalize()
	case phaseRequest:
		st.s.metrics.exchange(st.label, outcomeIncomplete)
		st.x, st.req, st.phase = nil, direction{}, phaseIdle
	}
	if streamEnded(err) {
		return nil
	}
	return err
}

// metadataService returns the service name the EnvoyFilter passes as gRPC
// initial metadata, or "".
func (st *stream) metadataService() string {
	for _, v := range metadata.ValueFromIncomingContext(st.rpc.Context(), st.s.cfg.ServiceMetadataKey) {
		if v = strings.TrimSpace(v); v != "" {
			return v
		}
	}
	return ""
}

// direction accumulates one body. Chunks are copied, so no protobuf buffer
// outlives its message.
type direction struct {
	data []byte
	seen int64 // every body byte observed, kept or not
	hint int   // content-length (0 when absent): sizes the first allocation
	// and confirms a BUFFERED_PARTIAL body that trailers end
	closed   bool
	complete bool // closed by end_of_stream or trailers rather than cut off
}

// write keeps chunk while the direction holds less than limit bytes.
func (d *direction) write(chunk []byte, limit int) {
	if d.closed {
		return
	}
	d.seen += int64(len(chunk))
	n := min(limit-len(d.data), len(chunk))
	if n <= 0 {
		return
	}
	if need := len(d.data) + n; need > cap(d.data) {
		// Grow by hand: append could overshoot the limit by up to 2x.
		buf := make([]byte, len(d.data), min(limit, max(need, 2*cap(d.data), d.hint)))
		copy(buf, d.data)
		d.data = buf
	}
	d.data = append(d.data, chunk[:n]...)
}

// close ends the direction; complete says whether its end was observed.
func (d *direction) close(complete bool) {
	if !d.closed {
		d.closed, d.complete = true, complete
	}
}

// body returns the body's metadata. The kept bytes travel separately (see
// job) and become Body data in a worker.
func (d *direction) body(contentType string) capture.Body {
	return capture.Body{
		ContentType: contentType,
		Size:        d.seen,
		Truncated:   !d.complete || d.seen > int64(len(d.data)),
	}
}

// pseudo holds the pseudo-headers the tap records.
type pseudo struct {
	method, scheme, authority, path, status string
}

// pseudoHeaders extracts :method, :scheme, :authority, :path and :status;
// others such as :protocol are ignored. The first occurrence wins.
func pseudoHeaders(hs []*corev3.HeaderValue) pseudo {
	var p pseudo
	for _, h := range hs {
		k := h.GetKey()
		if !strings.HasPrefix(k, ":") {
			continue
		}
		var dst *string
		switch strings.ToLower(k) {
		case ":method":
			dst = &p.method
		case ":scheme":
			dst = &p.scheme
		case ":authority":
			dst = &p.authority
		case ":path":
			dst = &p.path
		case ":status":
			dst = &p.status
		default:
			continue
		}
		if *dst == "" {
			*dst = headerValue(h)
		}
	}
	return p
}

// statusCode parses :status; a missing or malformed value gives 0, which
// fails validation later.
func (p pseudo) statusCode() int {
	n, err := strconv.Atoi(p.status)
	if err != nil {
		return 0
	}
	return n
}

// headerValue returns a header's value: current Envoy sends raw_value and
// leaves value empty, older Envoy sends value.
func headerValue(h *corev3.HeaderValue) string {
	if raw := h.GetRawValue(); len(raw) > 0 {
		return string(raw)
	}
	return h.GetValue()
}

// headerList looks headers up in Envoy's list without building a map: the
// sampler reads only a few headers, and most requests are not sampled.
type headerList []*corev3.HeaderValue

func (l headerList) get(name string) string {
	for _, h := range l {
		if strings.EqualFold(h.GetKey(), name) {
			return headerValue(h)
		}
	}
	return ""
}

// toHeaders converts Envoy headers to capture headers: lowercase names,
// values in arrival order, pseudo-headers left out.
func toHeaders(hs []*corev3.HeaderValue) capture.Headers {
	h := make(capture.Headers, len(hs))
	for _, hv := range hs {
		k := hv.GetKey()
		if k == "" || k[0] == ':' {
			continue
		}
		k = strings.ToLower(k)
		h[k] = append(h[k], headerValue(hv))
	}
	return h
}

// contentLength returns the content-length header as a size hint, or 0.
func contentLength(h capture.Headers) int {
	n, err := strconv.Atoi(h.Get("content-length"))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

func modeName(inline bool) string {
	if inline {
		return capture.ModeInline
	}
	return capture.ModeObserve
}

// streamEnded reports whether err only means the stream is over: Envoy
// half-closed it, reset it, or it timed out.
func streamEnded(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	}
	return false
}

// noopReply is the inline reply to a message of kind k: the matching
// response type with an empty CommonResponse, which changes nothing.
func noopReply(k msgKind, override *filterv3.ProcessingMode) *extprocv3.ProcessingResponse {
	r := &extprocv3.ProcessingResponse{ModeOverride: override}
	switch k {
	case kindRequestHeaders:
		r.Response = &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}},
		}
	case kindResponseHeaders:
		r.Response = &extprocv3.ProcessingResponse_ResponseHeaders{
			ResponseHeaders: &extprocv3.HeadersResponse{Response: &extprocv3.CommonResponse{}},
		}
	case kindRequestBody:
		r.Response = &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{}},
		}
	case kindResponseBody:
		r.Response = &extprocv3.ProcessingResponse_ResponseBody{
			ResponseBody: &extprocv3.BodyResponse{Response: &extprocv3.CommonResponse{}},
		}
	case kindRequestTrailers:
		r.Response = &extprocv3.ProcessingResponse_RequestTrailers{RequestTrailers: &extprocv3.TrailersResponse{}}
	case kindResponseTrailers:
		r.Response = &extprocv3.ProcessingResponse_ResponseTrailers{ResponseTrailers: &extprocv3.TrailersResponse{}}
	}
	return r
}

// skipRest is the mode_override for an unsampled inline request: Envoy stops
// sending bodies, response headers and trailers, so the rest of the request
// costs no round trips. It only takes effect when the filter sets
// allow_mode_override.
func skipRest() *filterv3.ProcessingMode {
	return &filterv3.ProcessingMode{
		RequestBodyMode:     filterv3.ProcessingMode_NONE,
		ResponseHeaderMode:  filterv3.ProcessingMode_SKIP,
		ResponseBodyMode:    filterv3.ProcessingMode_NONE,
		RequestTrailerMode:  filterv3.ProcessingMode_SKIP,
		ResponseTrailerMode: filterv3.ProcessingMode_SKIP,
	}
}
