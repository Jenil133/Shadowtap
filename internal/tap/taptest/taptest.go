// Package taptest simulates Envoy's ext_proc HTTP filter for tests.
//
// Run turns one HTTP request/response pair into the ProcessingRequest
// sequence Envoy sends for it and drives that sequence over a client stream:
//
//   - Observe mode is ext_proc observability mode with STREAMED bodies. Every
//     message has observability_mode set, nothing waits for a reply, and any
//     reply is an error because Envoy would ignore it. A server may close the
//     stream early to say it needs no more messages.
//   - Inline mode is classic ext_proc with BUFFERED_PARTIAL bodies and
//     allow_mode_override. Every message waits for exactly one reply of the
//     matching type, and a mode_override in the request headers reply changes
//     what is sent next, as it would in Envoy.
//
// Dial serves an ExternalProcessorServer over an in-memory connection.
package taptest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	filterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// Defaults for Options.
const (
	// DefaultChunkSize is the STREAMED body chunk size in observe mode.
	DefaultChunkSize = 16 << 10
	// DefaultBufferLimit is the BUFFERED_PARTIAL limit in inline mode, Envoy's
	// default per-connection buffer limit.
	DefaultBufferLimit = 1 << 20
)

// Exchange is one HTTP request and its response as Envoy sees them.
// Pseudo-headers (:method, :scheme, :authority, :path and :status) are
// generated from the fields that are set and sent first; a zero Status sends
// no :status. Header and trailer entries follow in order, names lowercased as
// Envoy does; an entry may itself be a pseudo-header such as :protocol.
type Exchange struct {
	Method, Scheme, Authority, Path string
	RequestHeaders                  [][2]string
	RequestBody                     []byte
	RequestTrailers                 [][2]string
	Status                          int
	ResponseHeaders                 [][2]string
	ResponseBody                    []byte
	ResponseTrailers                [][2]string
}

// Options configures the simulated filter.
type Options struct {
	// Mode is capture.ModeObserve (the default when empty) or
	// capture.ModeInline.
	Mode string
	// ChunkSize is the size of STREAMED body chunks; DefaultChunkSize when 0.
	ChunkSize int
	// BufferLimit is the BUFFERED_PARTIAL limit; DefaultBufferLimit when 0.
	// Envoy sends at most this many body bytes in one message and the rest of
	// the body passes through unseen.
	BufferLimit int
	// LegacyHeaderValues sends header values in value instead of raw_value,
	// as Envoy did before raw_value existed. Values must then be valid UTF-8.
	LegacyHeaderValues bool
	// SendTrailers sets both trailer modes to SEND. Without it trailers are
	// not sent, but their presence still keeps end_of_stream off the last body
	// message, as in Envoy.
	SendTrailers bool
	// Metadata is sent as gRPC metadata on the stream, like the EnvoyFilter's
	// initial_metadata (for example x-shadowtap-service).
	Metadata map[string]string
	// CancelAfter, when positive, cancels the stream after this many messages
	// have been sent, simulating a reset. In inline mode the reply to the last
	// message is awaited first; in observe mode the cancellation races with
	// the messages still in flight, exactly like a real reset.
	CancelAfter int
}

// Result describes a run.
type Result struct {
	// Sent is the number of messages sent.
	Sent int
	// Replies are the responses received, in order.
	Replies []*extprocv3.ProcessingResponse
	// ModeOverride is the mode_override of the request headers reply (inline).
	ModeOverride *filterv3.ProcessingMode
	// ResponseModeOverride is the mode_override of the response headers reply
	// (inline). Envoy applies it to the rest of the response.
	ResponseModeOverride *filterv3.ProcessingMode
	// ServerClosedEarly reports that the server ended the stream before every
	// message was sent (or, inline, instead of replying).
	ServerClosedEarly bool
}

// Run sends ex through client as Envoy would in opt.Mode and checks the
// server's behaviour: inline replies must match each message's type and
// observe mode must get no reply. After the last message Run half-closes the
// stream and waits for the server to end it, so once Run returns the server
// has finished with the stream (unless CancelAfter cut it short). Run returns
// what happened so far along with any error.
func Run(ctx context.Context, client extprocv3.ExternalProcessorClient, ex Exchange, opt Options) (Result, error) {
	opt, err := opt.normalize()
	if err != nil {
		return Result{}, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if len(opt.Metadata) > 0 {
		kv := make([]string, 0, 2*len(opt.Metadata))
		for _, k := range slices.Sorted(maps.Keys(opt.Metadata)) {
			kv = append(kv, k, opt.Metadata[k])
		}
		ctx = metadata.AppendToOutgoingContext(ctx, kv...)
	}
	stream, err := client.Process(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("taptest: open stream: %w", err)
	}
	r := &runner{stream: stream, cancel: cancel, ex: ex, opt: opt}
	if opt.Mode == capture.ModeInline {
		err = r.inline()
	} else {
		err = r.observe()
	}
	return r.res, err
}

func (o Options) normalize() (Options, error) {
	switch o.Mode {
	case "":
		o.Mode = capture.ModeObserve
	case capture.ModeObserve, capture.ModeInline:
	default:
		return o, fmt.Errorf("taptest: unknown mode %q", o.Mode)
	}
	if o.ChunkSize <= 0 {
		o.ChunkSize = DefaultChunkSize
	}
	if o.BufferLimit <= 0 {
		o.BufferLimit = DefaultBufferLimit
	}
	return o, nil
}

type runner struct {
	stream     extprocv3.ExternalProcessor_ProcessClient
	cancel     context.CancelFunc
	ex         Exchange
	opt        Options
	res        Result
	configSent bool // protocol_config goes on the first message only
	cancelled  bool
}

// filterMode is what the simulated filter sends after the request headers.
type filterMode struct {
	responseHeaders  bool
	requestBody      filterv3.ProcessingMode_BodySendMode
	responseBody     filterv3.ProcessingMode_BodySendMode
	requestTrailers  bool
	responseTrailers bool
}

// override applies a mode_override the way Envoy does: body modes are
// replaced, and a header or trailer mode of DEFAULT keeps the configured
// behaviour. The request header mode is ignored because request headers have
// already been sent.
func (m filterMode) override(o *filterv3.ProcessingMode) filterMode {
	m.responseHeaders = sendMode(o.GetResponseHeaderMode(), m.responseHeaders)
	m.requestBody = o.GetRequestBodyMode()
	m.responseBody = o.GetResponseBodyMode()
	m.requestTrailers = sendMode(o.GetRequestTrailerMode(), m.requestTrailers)
	m.responseTrailers = sendMode(o.GetResponseTrailerMode(), m.responseTrailers)
	return m
}

func sendMode(mode filterv3.ProcessingMode_HeaderSendMode, configured bool) bool {
	switch mode {
	case filterv3.ProcessingMode_SEND:
		return true
	case filterv3.ProcessingMode_SKIP:
		return false
	}
	return configured
}

// Messages returns the messages Envoy sends for ex in opt.Mode when the
// server does not override the processing mode, so a test can drive an
// ExternalProcessorServer directly (for example through a fake stream)
// instead of over gRPC. Metadata and CancelAfter are ignored.
func Messages(ex Exchange, opt Options) ([]*extprocv3.ProcessingRequest, error) {
	opt, err := opt.normalize()
	if err != nil {
		return nil, err
	}
	r := &runner{ex: ex, opt: opt}
	msgs := []*extprocv3.ProcessingRequest{r.requestHeaders()}
	rest, err := r.afterRequestHeaders(r.configuredMode())
	if err != nil {
		return nil, err
	}
	msgs = append(msgs, rest...)
	if opt.Mode == capture.ModeObserve {
		for _, m := range msgs {
			m.ObservabilityMode = true
		}
	}
	return msgs, nil
}

// configuredMode is the simulated filter's processing_mode: bodies STREAMED in
// observe mode and BUFFERED_PARTIAL inline, trailers per SendTrailers.
func (r *runner) configuredMode() filterMode {
	body := filterv3.ProcessingMode_STREAMED
	if r.opt.Mode == capture.ModeInline {
		body = filterv3.ProcessingMode_BUFFERED_PARTIAL
	}
	return filterMode{
		responseHeaders:  true,
		requestBody:      body,
		responseBody:     body,
		requestTrailers:  r.opt.SendTrailers,
		responseTrailers: r.opt.SendTrailers,
	}
}

// inline sends each message and waits for its reply.
func (r *runner) inline() error {
	mode := r.configuredMode()
	reply, stop, err := r.roundTrip(r.requestHeaders())
	if err != nil || stop {
		return err
	}
	if o := reply.GetModeOverride(); o != nil {
		r.res.ModeOverride = o
		mode = mode.override(o)
	}
	msgs, err := r.requestSide(mode)
	if err != nil {
		return err
	}
	for _, m := range msgs {
		if _, stop, err := r.roundTrip(m); err != nil || stop {
			return err
		}
	}
	if mode.responseHeaders {
		reply, stop, err := r.roundTrip(r.responseHeaders())
		if err != nil || stop {
			return err
		}
		if o := reply.GetModeOverride(); o != nil {
			// Envoy merges an override with the filter's configuration, not
			// with an earlier override.
			r.res.ResponseModeOverride = o
			mode = r.configuredMode().override(o)
		}
	}
	if msgs, err = r.responseSide(mode); err != nil {
		return err
	}
	for _, m := range msgs {
		if _, stop, err := r.roundTrip(m); err != nil || stop {
			return err
		}
	}
	return r.closeAndWait()
}

// observe sends every message without waiting, while a goroutine watches for
// the server ending the stream (or, wrongly, replying).
func (r *runner) observe() error {
	msgs, err := Messages(r.ex, r.opt)
	if err != nil {
		return err
	}

	var (
		replies []*extprocv3.ProcessingResponse
		recvErr error
	)
	ended := make(chan struct{})
	go func() {
		defer close(ended)
		for {
			reply, err := r.stream.Recv()
			if err != nil {
				recvErr = err
				return
			}
			replies = append(replies, reply)
		}
	}()

	sendErr := r.sendAll(msgs, ended)
	if sendErr == nil && !r.cancelled {
		if err := r.stream.CloseSend(); err != nil {
			sendErr = fmt.Errorf("taptest: close send: %w", err)
		}
	}
	if sendErr != nil {
		r.cancel() // unblock the receiving goroutine
	}
	<-ended
	r.res.Replies = replies
	switch {
	case sendErr != nil:
		return sendErr
	case len(replies) > 0:
		return fmt.Errorf("taptest: server sent %d replies in observability mode (first: %s); Envoy ignores them",
			len(replies), replyKind(replies[0]))
	case r.cancelled:
		return nil
	case !errors.Is(recvErr, io.EOF):
		return fmt.Errorf("taptest: server ended the stream: %w", recvErr)
	}
	return nil
}

func (r *runner) sendAll(msgs []*extprocv3.ProcessingRequest, ended <-chan struct{}) error {
	for _, m := range msgs {
		select {
		case <-ended: // Envoy stops sending once the server closes the stream
			r.res.ServerClosedEarly = true
			return nil
		default:
		}
		if err := r.stream.Send(m); err != nil {
			if errors.Is(err, io.EOF) { // the server has ended the stream
				r.res.ServerClosedEarly = true
				return nil
			}
			return fmt.Errorf("taptest: send %s: %w", messageKind(m), err)
		}
		r.res.Sent++
		if r.cancelDue() {
			return nil
		}
	}
	return nil
}

// roundTrip sends one inline message and waits for its reply. stop reports
// that the run is over: the server ended the stream or CancelAfter fired.
func (r *runner) roundTrip(m *extprocv3.ProcessingRequest) (reply *extprocv3.ProcessingResponse, stop bool, err error) {
	kind := messageKind(m)
	if err := r.stream.Send(m); err != nil {
		if errors.Is(err, io.EOF) {
			r.res.ServerClosedEarly = true
			return nil, true, r.waitEnd()
		}
		return nil, true, fmt.Errorf("taptest: send %s: %w", kind, err)
	}
	r.res.Sent++
	reply, err = r.stream.Recv()
	switch {
	case errors.Is(err, io.EOF):
		// Closing the stream instead of replying is allowed: Envoy stops
		// calling the server for the rest of the request.
		r.res.ServerClosedEarly = true
		return nil, true, nil
	case err != nil:
		return nil, true, fmt.Errorf("taptest: waiting for the %s reply: %w", kind, err)
	}
	r.res.Replies = append(r.res.Replies, reply)
	if !matches(m, reply) {
		return reply, true, fmt.Errorf("taptest: %s answered with %s", kind, replyKind(reply))
	}
	return reply, r.cancelDue(), nil
}

// closeAndWait half-closes the stream, as Envoy does when the request is
// done, and waits for the server to end it.
func (r *runner) closeAndWait() error {
	if err := r.stream.CloseSend(); err != nil {
		return fmt.Errorf("taptest: close send: %w", err)
	}
	return r.waitEnd()
}

// waitEnd waits for the server to end the stream with an OK status.
func (r *runner) waitEnd() error {
	reply, err := r.stream.Recv()
	switch {
	case errors.Is(err, io.EOF):
		return nil
	case err != nil:
		return fmt.Errorf("taptest: server ended the stream: %w", err)
	}
	r.res.Replies = append(r.res.Replies, reply)
	return fmt.Errorf("taptest: unexpected %s reply after the last message", replyKind(reply))
}

func (r *runner) cancelDue() bool {
	if r.opt.CancelAfter > 0 && r.res.Sent >= r.opt.CancelAfter {
		r.cancel()
		r.cancelled = true
		return true
	}
	return false
}

// afterRequestHeaders returns the messages Envoy sends after the request
// headers, in order, given the filter mode.
func (r *runner) afterRequestHeaders(mode filterMode) ([]*extprocv3.ProcessingRequest, error) {
	msgs, err := r.requestSide(mode)
	if err != nil {
		return nil, err
	}
	if mode.responseHeaders {
		msgs = append(msgs, r.responseHeaders())
	}
	resp, err := r.responseSide(mode)
	if err != nil {
		return nil, err
	}
	return append(msgs, resp...), nil
}

// requestSide returns the request body and trailer messages.
func (r *runner) requestSide(mode filterMode) ([]*extprocv3.ProcessingRequest, error) {
	ex := r.ex
	trailers := len(ex.RequestTrailers) > 0
	msgs, err := r.bodyMessages(ex.RequestBody, false, mode.requestBody, trailers)
	if err != nil {
		return nil, err
	}
	if mode.requestTrailers && trailers {
		msgs = append(msgs, r.trailers(ex.RequestTrailers, false))
	}
	return msgs, nil
}

// responseSide returns the response body and trailer messages.
func (r *runner) responseSide(mode filterMode) ([]*extprocv3.ProcessingRequest, error) {
	ex := r.ex
	trailers := len(ex.ResponseTrailers) > 0
	msgs, err := r.bodyMessages(ex.ResponseBody, true, mode.responseBody, trailers)
	if err != nil {
		return nil, err
	}
	if mode.responseTrailers && trailers {
		msgs = append(msgs, r.trailers(ex.ResponseTrailers, true))
	}
	return msgs, nil
}

// newMessage returns an empty message; the first one carries the filter's
// protocol_config, as Envoy sends it.
func (r *runner) newMessage() *extprocv3.ProcessingRequest {
	m := &extprocv3.ProcessingRequest{}
	if !r.configSent {
		r.configSent = true
		mode := r.configuredMode()
		m.ProtocolConfig = &extprocv3.ProtocolConfiguration{
			RequestBodyMode:  mode.requestBody,
			ResponseBodyMode: mode.responseBody,
		}
	}
	return m
}

func (r *runner) requestHeaders() *extprocv3.ProcessingRequest {
	ex := r.ex
	var pseudo [][2]string
	for _, p := range [][2]string{
		{":method", ex.Method}, {":scheme", ex.Scheme}, {":authority", ex.Authority}, {":path", ex.Path},
	} {
		if p[1] != "" {
			pseudo = append(pseudo, p)
		}
	}
	m := r.newMessage()
	m.Request = &extprocv3.ProcessingRequest_RequestHeaders{RequestHeaders: &extprocv3.HttpHeaders{
		Headers:     r.headerMap(pseudo, ex.RequestHeaders),
		EndOfStream: len(ex.RequestBody) == 0 && len(ex.RequestTrailers) == 0,
	}}
	return m
}

func (r *runner) responseHeaders() *extprocv3.ProcessingRequest {
	ex := r.ex
	var pseudo [][2]string
	if ex.Status != 0 {
		pseudo = append(pseudo, [2]string{":status", strconv.Itoa(ex.Status)})
	}
	m := r.newMessage()
	m.Request = &extprocv3.ProcessingRequest_ResponseHeaders{ResponseHeaders: &extprocv3.HttpHeaders{
		Headers:     r.headerMap(pseudo, ex.ResponseHeaders),
		EndOfStream: len(ex.ResponseBody) == 0 && len(ex.ResponseTrailers) == 0,
	}}
	return m
}

// bodyMessages splits one body the way Envoy sends it in mode. end_of_stream
// marks the last message only when Envoy has seen the whole body and no
// trailers follow (whether or not they are sent).
func (r *runner) bodyMessages(body []byte, response bool, mode filterv3.ProcessingMode_BodySendMode, trailers bool) ([]*extprocv3.ProcessingRequest, error) {
	if len(body) == 0 {
		return nil, nil
	}
	switch mode {
	case filterv3.ProcessingMode_NONE:
		return nil, nil
	case filterv3.ProcessingMode_STREAMED:
		msgs := make([]*extprocv3.ProcessingRequest, 0, (len(body)+r.opt.ChunkSize-1)/r.opt.ChunkSize)
		for off := 0; off < len(body); off += r.opt.ChunkSize {
			end := min(off+r.opt.ChunkSize, len(body))
			msgs = append(msgs, r.body(body[off:end], response, end == len(body) && !trailers))
		}
		return msgs, nil
	case filterv3.ProcessingMode_BUFFERED_PARTIAL:
		n := min(len(body), r.opt.BufferLimit)
		return []*extprocv3.ProcessingRequest{r.body(body[:n], response, n == len(body) && !trailers)}, nil
	case filterv3.ProcessingMode_BUFFERED:
		if len(body) > r.opt.BufferLimit {
			return nil, fmt.Errorf("taptest: %d-byte body exceeds the BUFFERED limit of %d bytes; Envoy would reject the request",
				len(body), r.opt.BufferLimit)
		}
		return []*extprocv3.ProcessingRequest{r.body(body, response, !trailers)}, nil
	}
	return nil, fmt.Errorf("taptest: body send mode %s is not supported", mode)
}

func (r *runner) body(data []byte, response, eos bool) *extprocv3.ProcessingRequest {
	b := &extprocv3.HttpBody{Body: data, EndOfStream: eos}
	m := r.newMessage()
	if response {
		m.Request = &extprocv3.ProcessingRequest_ResponseBody{ResponseBody: b}
	} else {
		m.Request = &extprocv3.ProcessingRequest_RequestBody{RequestBody: b}
	}
	return m
}

func (r *runner) trailers(t [][2]string, response bool) *extprocv3.ProcessingRequest {
	tr := &extprocv3.HttpTrailers{Trailers: r.headerMap(nil, t)}
	m := r.newMessage()
	if response {
		m.Request = &extprocv3.ProcessingRequest_ResponseTrailers{ResponseTrailers: tr}
	} else {
		m.Request = &extprocv3.ProcessingRequest_RequestTrailers{RequestTrailers: tr}
	}
	return m
}

// headerMap builds Envoy's header list: pseudo-headers first, names
// lowercase, values in raw_value (or value for legacy Envoy).
func (r *runner) headerMap(pseudo, headers [][2]string) *corev3.HeaderMap {
	hm := &corev3.HeaderMap{Headers: make([]*corev3.HeaderValue, 0, len(pseudo)+len(headers))}
	for _, list := range [][][2]string{pseudo, headers} {
		for _, kv := range list {
			hv := &corev3.HeaderValue{Key: strings.ToLower(kv[0])}
			if r.opt.LegacyHeaderValues {
				hv.Value = kv[1]
			} else {
				hv.RawValue = []byte(kv[1])
			}
			hm.Headers = append(hm.Headers, hv)
		}
	}
	return hm
}

// matches reports whether reply is the response type Envoy expects for req.
func matches(req *extprocv3.ProcessingRequest, reply *extprocv3.ProcessingResponse) bool {
	var ok bool
	switch req.GetRequest().(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		_, ok = reply.GetResponse().(*extprocv3.ProcessingResponse_RequestHeaders)
	case *extprocv3.ProcessingRequest_RequestBody:
		_, ok = reply.GetResponse().(*extprocv3.ProcessingResponse_RequestBody)
	case *extprocv3.ProcessingRequest_RequestTrailers:
		_, ok = reply.GetResponse().(*extprocv3.ProcessingResponse_RequestTrailers)
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		_, ok = reply.GetResponse().(*extprocv3.ProcessingResponse_ResponseHeaders)
	case *extprocv3.ProcessingRequest_ResponseBody:
		_, ok = reply.GetResponse().(*extprocv3.ProcessingResponse_ResponseBody)
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		_, ok = reply.GetResponse().(*extprocv3.ProcessingResponse_ResponseTrailers)
	}
	return ok
}

func messageKind(m *extprocv3.ProcessingRequest) string {
	switch m.GetRequest().(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return "request_headers"
	case *extprocv3.ProcessingRequest_RequestBody:
		return "request_body"
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return "request_trailers"
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return "response_headers"
	case *extprocv3.ProcessingRequest_ResponseBody:
		return "response_body"
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return "response_trailers"
	}
	return "empty message"
}

func replyKind(r *extprocv3.ProcessingResponse) string {
	switch r.GetResponse().(type) {
	case *extprocv3.ProcessingResponse_RequestHeaders:
		return "request_headers"
	case *extprocv3.ProcessingResponse_RequestBody:
		return "request_body"
	case *extprocv3.ProcessingResponse_RequestTrailers:
		return "request_trailers"
	case *extprocv3.ProcessingResponse_ResponseHeaders:
		return "response_headers"
	case *extprocv3.ProcessingResponse_ResponseBody:
		return "response_body"
	case *extprocv3.ProcessingResponse_ResponseTrailers:
		return "response_trailers"
	case *extprocv3.ProcessingResponse_ImmediateResponse:
		return "immediate_response"
	case *extprocv3.ProcessingResponse_StreamedImmediateResponse:
		return "streamed_immediate_response"
	}
	return "empty response"
}

// IsNoop reports whether reply lets Envoy continue without changing anything:
// a headers, body or trailers response with no status change, header, body or
// trailer mutation, and no route cache clearing. Immediate responses are
// never no-ops. Dynamic metadata, mode_override and timeout overrides do not
// change traffic and are allowed.
func IsNoop(reply *extprocv3.ProcessingResponse) bool {
	var common *extprocv3.CommonResponse
	switch r := reply.GetResponse().(type) {
	case *extprocv3.ProcessingResponse_RequestHeaders:
		common = r.RequestHeaders.GetResponse()
	case *extprocv3.ProcessingResponse_ResponseHeaders:
		common = r.ResponseHeaders.GetResponse()
	case *extprocv3.ProcessingResponse_RequestBody:
		common = r.RequestBody.GetResponse()
	case *extprocv3.ProcessingResponse_ResponseBody:
		common = r.ResponseBody.GetResponse()
	case *extprocv3.ProcessingResponse_RequestTrailers:
		return r.RequestTrailers.GetHeaderMutation() == nil
	case *extprocv3.ProcessingResponse_ResponseTrailers:
		return r.ResponseTrailers.GetHeaderMutation() == nil
	default:
		return false
	}
	return common.GetStatus() == extprocv3.CommonResponse_CONTINUE &&
		common.GetHeaderMutation() == nil &&
		common.GetBodyMutation() == nil &&
		common.GetTrailers() == nil &&
		!common.GetClearRouteCache()
}

// FromHTTP converts a request and its response into an Exchange. Header names
// are lowercased, sorted for a stable order, and multi-valued headers become
// repeated pairs. The request's Host becomes the authority (Envoy turns Host
// into :authority), the request URI becomes the path, and the scheme is taken
// from the URL or else from TLS. Response header keys carrying
// http.TrailerPrefix become response trailers, following net/http's handler
// convention. A zero status means 200, as in net/http. The body slices are
// referenced, not copied.
func FromHTTP(req *http.Request, reqBody []byte, status int, respHeader http.Header, respBody []byte) Exchange {
	ex := Exchange{RequestBody: reqBody, Status: status, ResponseBody: respBody}
	if ex.Status == 0 {
		ex.Status = http.StatusOK
	}
	if req != nil {
		ex.Method = req.Method
		if ex.Method == "" {
			ex.Method = http.MethodGet
		}
		ex.Scheme = requestScheme(req)
		ex.Authority = req.Host
		if ex.Authority == "" && req.URL != nil {
			ex.Authority = req.URL.Host
		}
		ex.Path = requestPath(req)
		ex.RequestHeaders = headerPairs(req.Header, func(k string) bool { return !strings.EqualFold(k, "host") })
		ex.RequestTrailers = headerPairs(req.Trailer, nil)
	}
	ex.ResponseHeaders = headerPairs(respHeader, func(k string) bool { return !strings.HasPrefix(k, http.TrailerPrefix) })
	trailers := make(http.Header)
	for k, v := range respHeader {
		if name, ok := strings.CutPrefix(k, http.TrailerPrefix); ok && name != "" {
			trailers[name] = v
		}
	}
	ex.ResponseTrailers = headerPairs(trailers, nil)
	return ex
}

func requestScheme(req *http.Request) string {
	switch {
	case req.URL != nil && req.URL.Scheme != "":
		return strings.ToLower(req.URL.Scheme)
	case req.TLS != nil:
		return "https"
	}
	return "http"
}

// requestPath returns the request target as Envoy's :path: the raw origin-form
// target a server received, else the URL's path and query.
func requestPath(req *http.Request) string {
	if strings.HasPrefix(req.RequestURI, "/") {
		return req.RequestURI
	}
	if req.URL != nil {
		return req.URL.RequestURI()
	}
	return "/"
}

// headerPairs flattens h into sorted lowercase name/value pairs, keeping only
// names keep accepts (all when keep is nil). It returns nil for no pairs.
func headerPairs(h http.Header, keep func(string) bool) [][2]string {
	var pairs [][2]string
	for _, k := range slices.Sorted(maps.Keys(h)) {
		if keep != nil && !keep(k) {
			continue
		}
		name := strings.ToLower(k)
		for _, v := range h[k] {
			pairs = append(pairs, [2]string{name, v})
		}
	}
	return pairs
}

// Dial serves srv on an in-memory gRPC connection and returns a client for
// it. The server and connection are stopped by tb.Cleanup, which also waits
// for srv's handlers to return.
func Dial(tb testing.TB, srv extprocv3.ExternalProcessorServer) extprocv3.ExternalProcessorClient {
	tb.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(grpc.MaxRecvMsgSize(16<<20), grpc.WaitForHandlers(true))
	extprocv3.RegisterExternalProcessorServer(gs, srv)
	served := make(chan error, 1)
	go func() { served <- gs.Serve(lis) }()

	conn, err := grpc.NewClient("passthrough:///bufconn",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		gs.Stop()
		<-served
		tb.Fatalf("taptest: dial bufconn: %v", err)
	}
	tb.Cleanup(func() {
		if err := conn.Close(); err != nil {
			tb.Errorf("taptest: close client connection: %v", err)
		}
		gs.Stop()
		// Stop can win the race with a Serve goroutine that has not taken
		// the server yet (a test that never opened a stream); that is benign.
		if err := <-served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			tb.Errorf("taptest: serve: %v", err)
		}
	})
	return extprocv3.NewExternalProcessorClient(conn)
}
