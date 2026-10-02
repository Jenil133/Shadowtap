package taptest

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	filterv3 "github.com/envoyproxy/go-control-plane/envoy/extensions/filters/http/ext_proc/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// recorder is a scriptable ext_proc server that records what it receives.
type recorder struct {
	extprocv3.UnimplementedExternalProcessorServer

	// reply returns the response to the i-th message, or nil for none.
	reply func(i int, req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse
	// stopAfter, when positive, ends the stream after that many messages.
	stopAfter int
	// stopErr is returned when the stream ends because of stopAfter.
	stopErr error

	mu    sync.Mutex
	msgs  []*extprocv3.ProcessingRequest
	md    metadata.MD
	ended chan error // why each stream ended
}

func newRecorder() *recorder { return &recorder{ended: make(chan error, 16)} }

func (r *recorder) Process(stream extprocv3.ExternalProcessor_ProcessServer) error {
	md, _ := metadata.FromIncomingContext(stream.Context())
	r.mu.Lock()
	r.md = md
	r.mu.Unlock()
	for i := 0; ; i++ {
		req, err := stream.Recv()
		if err != nil {
			r.ended <- err
			return nil
		}
		r.mu.Lock()
		r.msgs = append(r.msgs, req)
		r.mu.Unlock()
		if r.reply != nil {
			if resp := r.reply(i, req); resp != nil {
				if err := stream.Send(resp); err != nil {
					r.ended <- err
					return err
				}
			}
		}
		if r.stopAfter > 0 && i+1 >= r.stopAfter {
			r.ended <- r.stopErr
			return r.stopErr
		}
	}
}

func (r *recorder) messages() []*extprocv3.ProcessingRequest {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]*extprocv3.ProcessingRequest(nil), r.msgs...)
}

func (r *recorder) metadata() metadata.MD {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.md
}

func (r *recorder) waitEnded(t *testing.T) error {
	t.Helper()
	select {
	case err := <-r.ended:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("server stream never ended")
		return nil
	}
}

// noop answers every message with the matching no-op reply.
func noop(_ int, req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	return matchingReply(req)
}

func matchingReply(req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
	common := &extprocv3.CommonResponse{}
	switch req.GetRequest().(type) {
	case *extprocv3.ProcessingRequest_RequestHeaders:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: &extprocv3.HeadersResponse{Response: common}}}
	case *extprocv3.ProcessingRequest_RequestBody:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{RequestBody: &extprocv3.BodyResponse{Response: common}}}
	case *extprocv3.ProcessingRequest_RequestTrailers:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{RequestTrailers: &extprocv3.TrailersResponse{}}}
	case *extprocv3.ProcessingRequest_ResponseHeaders:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{ResponseHeaders: &extprocv3.HeadersResponse{Response: common}}}
	case *extprocv3.ProcessingRequest_ResponseBody:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{ResponseBody: &extprocv3.BodyResponse{Response: common}}}
	case *extprocv3.ProcessingRequest_ResponseTrailers:
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{ResponseTrailers: &extprocv3.TrailersResponse{}}}
	}
	return nil
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func postExchange(reqBody, respBody int) Exchange {
	return Exchange{
		Method:          "POST",
		Scheme:          "http",
		Authority:       "demo-api:8080",
		Path:            "/api/users?x=1",
		RequestHeaders:  [][2]string{{"Content-Type", "application/json"}, {"x-request-id", "r1"}, {"x-multi", "a"}, {"x-multi", "b"}},
		RequestBody:     bytes.Repeat([]byte("q"), reqBody),
		Status:          201,
		ResponseHeaders: [][2]string{{"content-type", "application/json"}},
		ResponseBody:    bytes.Repeat([]byte("s"), respBody),
	}
}

// kinds describes a message sequence as "kind" or "kind!" (end_of_stream).
func kinds(msgs []*extprocv3.ProcessingRequest) []string {
	var out []string
	for _, m := range msgs {
		k := messageKind(m)
		eos := false
		switch r := m.GetRequest().(type) {
		case *extprocv3.ProcessingRequest_RequestHeaders:
			eos = r.RequestHeaders.GetEndOfStream()
		case *extprocv3.ProcessingRequest_ResponseHeaders:
			eos = r.ResponseHeaders.GetEndOfStream()
		case *extprocv3.ProcessingRequest_RequestBody:
			eos = r.RequestBody.GetEndOfStream()
		case *extprocv3.ProcessingRequest_ResponseBody:
			eos = r.ResponseBody.GetEndOfStream()
		}
		if eos {
			k += "!"
		}
		out = append(out, k)
	}
	return out
}

func bodyOf(msgs []*extprocv3.ProcessingRequest, response bool) []byte {
	var b []byte
	for _, m := range msgs {
		if response {
			b = append(b, m.GetResponseBody().GetBody()...)
		} else {
			b = append(b, m.GetRequestBody().GetBody()...)
		}
	}
	return b
}

func headerPairsOf(hm *corev3.HeaderMap) [][2]string {
	var out [][2]string
	for _, h := range hm.GetHeaders() {
		v := h.GetValue()
		if len(h.GetRawValue()) > 0 {
			v = string(h.GetRawValue())
		}
		out = append(out, [2]string{h.GetKey(), v})
	}
	return out
}

func TestObserveSequence(t *testing.T) {
	rec := newRecorder()
	client := Dial(t, rec)
	ex := postExchange(40_000, 20_000)
	res, err := Run(testCtx(t), client, ex, Options{ChunkSize: 16_384})
	if err != nil {
		t.Fatal(err)
	}
	msgs := rec.messages()
	want := []string{"request_headers", "request_body", "request_body", "request_body!", "response_headers", "response_body", "response_body!"}
	if got := kinds(msgs); !reflect.DeepEqual(got, want) {
		t.Fatalf("sequence = %v, want %v", got, want)
	}
	if res.Sent != len(want) || len(res.Replies) != 0 || res.ServerClosedEarly || res.ModeOverride != nil {
		t.Errorf("result = %+v", res)
	}
	for i, m := range msgs {
		if !m.GetObservabilityMode() {
			t.Errorf("message %d: observability_mode not set", i)
		}
		if (m.GetProtocolConfig() != nil) != (i == 0) {
			t.Errorf("message %d: protocol_config = %v", i, m.GetProtocolConfig())
		}
	}
	if pc := msgs[0].GetProtocolConfig(); pc.GetRequestBodyMode() != filterv3.ProcessingMode_STREAMED || pc.GetResponseBodyMode() != filterv3.ProcessingMode_STREAMED {
		t.Errorf("protocol_config = %v", pc)
	}
	if got := len(msgs[1].GetRequestBody().GetBody()); got != 16_384 {
		t.Errorf("first chunk = %d bytes", got)
	}
	if !bytes.Equal(bodyOf(msgs, false), ex.RequestBody) || !bytes.Equal(bodyOf(msgs, true), ex.ResponseBody) {
		t.Error("bodies do not reassemble")
	}
	wantReq := [][2]string{
		{":method", "POST"}, {":scheme", "http"}, {":authority", "demo-api:8080"}, {":path", "/api/users?x=1"},
		{"content-type", "application/json"}, {"x-request-id", "r1"}, {"x-multi", "a"}, {"x-multi", "b"},
	}
	if got := headerPairsOf(msgs[0].GetRequestHeaders().GetHeaders()); !reflect.DeepEqual(got, wantReq) {
		t.Errorf("request headers = %v, want %v", got, wantReq)
	}
	for _, h := range msgs[0].GetRequestHeaders().GetHeaders().GetHeaders() {
		if h.GetValue() != "" {
			t.Errorf("header %s uses value instead of raw_value", h.GetKey())
		}
	}
	wantResp := [][2]string{{":status", "201"}, {"content-type", "application/json"}}
	if got := headerPairsOf(msgs[4].GetResponseHeaders().GetHeaders()); !reflect.DeepEqual(got, wantResp) {
		t.Errorf("response headers = %v, want %v", got, wantResp)
	}
	if err := rec.waitEnded(t); !errors.Is(err, io.EOF) {
		t.Errorf("server stream ended with %v, want EOF", err)
	}
}

func TestLegacyHeaderValues(t *testing.T) {
	msgs, err := Messages(postExchange(0, 0), Options{LegacyHeaderValues: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range msgs[0].GetRequestHeaders().GetHeaders().GetHeaders() {
		if h.GetValue() == "" || len(h.GetRawValue()) != 0 {
			t.Errorf("header %s: value=%q raw_value=%q", h.GetKey(), h.GetValue(), h.GetRawValue())
		}
	}
}

func TestInlineSequence(t *testing.T) {
	rec := newRecorder()
	rec.reply = noop
	client := Dial(t, rec)
	ex := postExchange(3000, 10)
	res, err := Run(testCtx(t), client, ex, Options{Mode: capture.ModeInline, BufferLimit: 1000})
	if err != nil {
		t.Fatal(err)
	}
	msgs := rec.messages()
	// The request body is over the buffer limit: one partial message without
	// end_of_stream, the rest is never sent.
	want := []string{"request_headers", "request_body", "response_headers", "response_body!"}
	if got := kinds(msgs); !reflect.DeepEqual(got, want) {
		t.Fatalf("sequence = %v, want %v", got, want)
	}
	if got := msgs[1].GetRequestBody().GetBody(); !bytes.Equal(got, ex.RequestBody[:1000]) {
		t.Errorf("partial body = %d bytes", len(got))
	}
	for i, m := range msgs {
		if m.GetObservabilityMode() {
			t.Errorf("message %d has observability_mode set", i)
		}
	}
	if pc := msgs[0].GetProtocolConfig(); pc.GetRequestBodyMode() != filterv3.ProcessingMode_BUFFERED_PARTIAL {
		t.Errorf("protocol_config = %v", pc)
	}
	if res.Sent != 4 || len(res.Replies) != 4 || res.ServerClosedEarly {
		t.Errorf("result = %+v", res)
	}
	for _, r := range res.Replies {
		if !IsNoop(r) {
			t.Errorf("reply %v is not a no-op", r)
		}
	}
}

func TestGetWithoutBody(t *testing.T) {
	ex := Exchange{Method: "GET", Path: "/healthz", Status: 204}
	for _, mode := range []string{capture.ModeObserve, capture.ModeInline} {
		msgs, err := Messages(ex, Options{Mode: mode})
		if err != nil {
			t.Fatal(err)
		}
		want := []string{"request_headers!", "response_headers!"}
		if got := kinds(msgs); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: sequence = %v, want %v", mode, got, want)
		}
	}
}

func TestTrailers(t *testing.T) {
	ex := postExchange(10, 10)
	ex.RequestTrailers = [][2]string{{"x-checksum", "abc"}}
	ex.ResponseTrailers = [][2]string{{"grpc-status", "0"}}
	tests := []struct {
		name string
		opt  Options
		want []string
	}{
		{"observe sent", Options{SendTrailers: true},
			[]string{"request_headers", "request_body", "request_trailers", "response_headers", "response_body", "response_trailers"}},
		// Trailers keep end_of_stream off the last body chunk even when the
		// filter does not send them, as in Envoy.
		{"observe skipped", Options{},
			[]string{"request_headers", "request_body", "response_headers", "response_body"}},
		{"inline sent", Options{Mode: capture.ModeInline, SendTrailers: true},
			[]string{"request_headers", "request_body", "request_trailers", "response_headers", "response_body", "response_trailers"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msgs, err := Messages(ex, tt.opt)
			if err != nil {
				t.Fatal(err)
			}
			if got := kinds(msgs); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("sequence = %v, want %v", got, tt.want)
			}
			if tt.opt.SendTrailers {
				got := headerPairsOf(msgs[len(msgs)-1].GetResponseTrailers().GetTrailers())
				if !reflect.DeepEqual(got, [][2]string{{"grpc-status", "0"}}) {
					t.Errorf("response trailers = %v", got)
				}
			}
		})
	}
}

func TestInlineModeOverride(t *testing.T) {
	ex := postExchange(100, 100)
	ex.RequestTrailers = [][2]string{{"x-t", "1"}}
	ex.ResponseTrailers = [][2]string{{"x-t", "2"}}
	tests := []struct {
		name     string
		override *filterv3.ProcessingMode
		want     []string
	}{
		{"skip everything", &filterv3.ProcessingMode{
			RequestBodyMode: filterv3.ProcessingMode_NONE, ResponseHeaderMode: filterv3.ProcessingMode_SKIP,
			ResponseBodyMode: filterv3.ProcessingMode_NONE, RequestTrailerMode: filterv3.ProcessingMode_SKIP,
			ResponseTrailerMode: filterv3.ProcessingMode_SKIP,
		}, []string{"request_headers"}},
		// DEFAULT header and trailer modes keep the configured behaviour; the
		// zero body mode is NONE.
		{"bodies off", &filterv3.ProcessingMode{}, []string{"request_headers", "request_trailers", "response_headers", "response_trailers"}},
		{"stream response body", &filterv3.ProcessingMode{
			ResponseBodyMode: filterv3.ProcessingMode_STREAMED, RequestTrailerMode: filterv3.ProcessingMode_SKIP,
			ResponseTrailerMode: filterv3.ProcessingMode_SKIP,
		}, []string{"request_headers", "response_headers", "response_body", "response_body"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := newRecorder()
			rec.reply = func(i int, req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
				r := matchingReply(req)
				if i == 0 {
					r.ModeOverride = tt.override
				}
				return r
			}
			client := Dial(t, rec)
			res, err := Run(testCtx(t), client, ex, Options{Mode: capture.ModeInline, SendTrailers: true, ChunkSize: 64})
			if err != nil {
				t.Fatal(err)
			}
			if got := kinds(rec.messages()); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sequence = %v, want %v", got, tt.want)
			}
			if !proto.Equal(res.ModeOverride, tt.override) {
				t.Errorf("ModeOverride = %v, want %v", res.ModeOverride, tt.override)
			}
			if res.Sent != len(tt.want) || len(res.Replies) != len(tt.want) {
				t.Errorf("result = %+v", res)
			}
		})
	}
}

func TestBufferedOverrideOverLimit(t *testing.T) {
	rec := newRecorder()
	rec.reply = func(i int, req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
		r := matchingReply(req)
		if i == 0 {
			r.ModeOverride = &filterv3.ProcessingMode{RequestBodyMode: filterv3.ProcessingMode_BUFFERED}
		}
		return r
	}
	_, err := Run(testCtx(t), Dial(t, rec), postExchange(100, 0), Options{Mode: capture.ModeInline, BufferLimit: 10})
	if err == nil || !strings.Contains(err.Error(), "BUFFERED") {
		t.Fatalf("err = %v, want a BUFFERED limit error", err)
	}
}

func TestInlineRejectsWrongReplies(t *testing.T) {
	tests := []struct {
		name  string
		reply *extprocv3.ProcessingResponse
	}{
		{"mismatched type", &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{ResponseHeaders: &extprocv3.HeadersResponse{}}}},
		{"immediate response", &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ImmediateResponse{ImmediateResponse: &extprocv3.ImmediateResponse{}}}},
		{"empty", &extprocv3.ProcessingResponse{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := newRecorder()
			rec.reply = func(int, *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse { return tt.reply }
			res, err := Run(testCtx(t), Dial(t, rec), postExchange(1, 1), Options{Mode: capture.ModeInline})
			if err == nil || !strings.Contains(err.Error(), "answered with") {
				t.Fatalf("err = %v, want a reply type error", err)
			}
			if res.Sent != 1 || len(res.Replies) != 1 {
				t.Errorf("result = %+v", res)
			}
		})
	}
}

func TestObserveRejectsReplies(t *testing.T) {
	rec := newRecorder()
	rec.reply = noop
	res, err := Run(testCtx(t), Dial(t, rec), postExchange(1, 1), Options{})
	if err == nil || !strings.Contains(err.Error(), "observability mode") {
		t.Fatalf("err = %v, want an observe reply error", err)
	}
	if len(res.Replies) == 0 {
		t.Error("replies not reported")
	}
}

func TestMetadata(t *testing.T) {
	rec := newRecorder()
	_, err := Run(testCtx(t), Dial(t, rec), postExchange(0, 0), Options{
		Metadata: map[string]string{"x-shadowtap-service": "checkout", "x-other": "1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	md := rec.metadata()
	if got := md.Get("x-shadowtap-service"); !reflect.DeepEqual(got, []string{"checkout"}) {
		t.Errorf("x-shadowtap-service = %v", got)
	}
	if got := md.Get("x-other"); !reflect.DeepEqual(got, []string{"1"}) {
		t.Errorf("x-other = %v", got)
	}
}

func TestCancelAfterInline(t *testing.T) {
	rec := newRecorder()
	rec.reply = noop
	res, err := Run(testCtx(t), Dial(t, rec), postExchange(10, 10), Options{Mode: capture.ModeInline, CancelAfter: 2})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent != 2 || len(res.Replies) != 2 {
		t.Errorf("result = %+v", res)
	}
	if err := rec.waitEnded(t); status.Code(err) != codes.Canceled {
		t.Errorf("server stream ended with %v, want Canceled", err)
	}
	if got := len(rec.messages()); got != 2 {
		t.Errorf("server received %d messages, want 2", got)
	}
}

func TestCancelAfterObserve(t *testing.T) {
	rec := newRecorder()
	res, err := Run(testCtx(t), Dial(t, rec), postExchange(100, 100), Options{ChunkSize: 10, CancelAfter: 3})
	if err != nil {
		t.Fatal(err)
	}
	if res.Sent != 3 {
		t.Errorf("sent %d messages, want 3", res.Sent)
	}
	// The reset races with the messages in flight, as in Envoy.
	if err := rec.waitEnded(t); status.Code(err) != codes.Canceled {
		t.Errorf("server stream ended with %v, want Canceled", err)
	}
	if got := len(rec.messages()); got > 3 {
		t.Errorf("server received %d messages, want at most 3", got)
	}
}

func TestServerClosesEarly(t *testing.T) {
	t.Run("inline close instead of reply", func(t *testing.T) {
		rec := newRecorder()
		rec.stopAfter = 1
		res, err := Run(testCtx(t), Dial(t, rec), postExchange(10, 10), Options{Mode: capture.ModeInline})
		if err != nil {
			t.Fatal(err)
		}
		if !res.ServerClosedEarly || res.Sent != 1 || len(res.Replies) != 0 {
			t.Errorf("result = %+v", res)
		}
	})
	t.Run("observe close", func(t *testing.T) {
		rec := newRecorder()
		rec.stopAfter = 1
		res, err := Run(testCtx(t), Dial(t, rec), postExchange(1000, 1000), Options{ChunkSize: 10})
		if err != nil {
			t.Fatal(err)
		}
		if res.Sent < 1 || len(res.Replies) != 0 {
			t.Errorf("result = %+v", res)
		}
		if got := len(rec.messages()); got != 1 {
			t.Errorf("server read %d messages, want 1", got)
		}
	})
	t.Run("observe error status", func(t *testing.T) {
		rec := newRecorder()
		rec.stopAfter = 1
		rec.stopErr = status.Error(codes.Internal, "boom")
		_, err := Run(testCtx(t), Dial(t, rec), postExchange(1000, 1000), Options{ChunkSize: 10})
		if status.Code(err) != codes.Internal {
			t.Fatalf("err = %v, want Internal", err)
		}
	})
	t.Run("inline error status", func(t *testing.T) {
		rec := newRecorder()
		rec.stopAfter = 1
		rec.stopErr = status.Error(codes.Internal, "boom")
		_, err := Run(testCtx(t), Dial(t, rec), postExchange(10, 10), Options{Mode: capture.ModeInline})
		if status.Code(err) != codes.Internal {
			t.Fatalf("err = %v, want Internal", err)
		}
	})
}

func TestInvalidOptions(t *testing.T) {
	if _, err := Messages(Exchange{}, Options{Mode: "tee"}); err == nil {
		t.Error("unknown mode accepted")
	}
	if _, err := Run(testCtx(t), Dial(t, newRecorder()), Exchange{}, Options{Mode: "tee"}); err == nil {
		t.Error("unknown mode accepted by Run")
	}
}

func TestFromHTTP(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "http://shop.example:8443/api/orders?id=7&b=%2F", strings.NewReader("{}"))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Add("X-Tag", "a")
	req.Header.Add("X-Tag", "b")
	req.Trailer = http.Header{"X-Checksum": {"c1"}}
	respHeader := http.Header{
		"Content-Type":                     {"application/json"},
		"Trailer":                          {"Grpc-Status"},
		http.TrailerPrefix + "Grpc-Status": {"0"},
	}
	ex := FromHTTP(req, []byte("{}"), 0, respHeader, []byte(`{"ok":true}`))
	want := Exchange{
		Method:           "POST",
		Scheme:           "http",
		Authority:        "shop.example:8443",
		Path:             "/api/orders?id=7&b=%2F",
		RequestHeaders:   [][2]string{{"content-type", "application/json"}, {"x-tag", "a"}, {"x-tag", "b"}},
		RequestBody:      []byte("{}"),
		RequestTrailers:  [][2]string{{"x-checksum", "c1"}},
		Status:           200,
		ResponseHeaders:  [][2]string{{"content-type", "application/json"}, {"trailer", "Grpc-Status"}},
		ResponseBody:     []byte(`{"ok":true}`),
		ResponseTrailers: [][2]string{{"grpc-status", "0"}},
	}
	if !reflect.DeepEqual(ex, want) {
		t.Errorf("FromHTTP =\n%+v\nwant\n%+v", ex, want)
	}

	// A client-side request has no RequestURI; TLS sets the scheme.
	creq, err := http.NewRequest(http.MethodGet, "/v1/items?q=x", nil)
	if err != nil {
		t.Fatal(err)
	}
	creq.Host = "items.svc"
	creq.TLS = req.TLS
	creq.URL.Scheme = ""
	got := FromHTTP(creq, nil, http.StatusNotFound, nil, nil)
	if got.Path != "/v1/items?q=x" || got.Authority != "items.svc" || got.Scheme != "http" || got.Status != 404 || got.Method != "GET" {
		t.Errorf("client request = %+v", got)
	}
	if got := FromHTTP(nil, nil, 0, nil, nil); got.Status != 200 || got.Method != "" {
		t.Errorf("nil request = %+v", got)
	}
}

func TestIsNoop(t *testing.T) {
	headers := func(c *extprocv3.CommonResponse) *extprocv3.ProcessingResponse {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: &extprocv3.HeadersResponse{Response: c}}}
	}
	tests := []struct {
		name  string
		reply *extprocv3.ProcessingResponse
		want  bool
	}{
		{"empty common", headers(&extprocv3.CommonResponse{}), true},
		{"nil common", headers(nil), true},
		{"with mode override", &extprocv3.ProcessingResponse{
			Response:     &extprocv3.ProcessingResponse_RequestHeaders{RequestHeaders: &extprocv3.HeadersResponse{}},
			ModeOverride: &filterv3.ProcessingMode{},
		}, true},
		{"trailers", &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseTrailers{ResponseTrailers: &extprocv3.TrailersResponse{}}}, true},
		{"header mutation", headers(&extprocv3.CommonResponse{HeaderMutation: &extprocv3.HeaderMutation{}}), false},
		{"body mutation", headers(&extprocv3.CommonResponse{BodyMutation: &extprocv3.BodyMutation{}}), false},
		{"replace status", headers(&extprocv3.CommonResponse{Status: extprocv3.CommonResponse_CONTINUE_AND_REPLACE}), false},
		{"clear route cache", headers(&extprocv3.CommonResponse{ClearRouteCache: true}), false},
		{"trailer mutation", &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestTrailers{
			RequestTrailers: &extprocv3.TrailersResponse{HeaderMutation: &extprocv3.HeaderMutation{}},
		}}, false},
		{"immediate", &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ImmediateResponse{ImmediateResponse: &extprocv3.ImmediateResponse{}}}, false},
		{"empty", &extprocv3.ProcessingResponse{}, false},
	}
	for _, tt := range tests {
		if got := IsNoop(tt.reply); got != tt.want {
			t.Errorf("%s: IsNoop = %v, want %v", tt.name, got, tt.want)
		}
	}
}

// Envoy applies a mode_override on the response headers reply to the rest of
// the response, so the simulator must too.
func TestResponseHeadersModeOverride(t *testing.T) {
	rec := newRecorder()
	override := &filterv3.ProcessingMode{ResponseBodyMode: filterv3.ProcessingMode_NONE, ResponseTrailerMode: filterv3.ProcessingMode_SKIP}
	rec.reply = func(_ int, req *extprocv3.ProcessingRequest) *extprocv3.ProcessingResponse {
		resp := matchingReply(req)
		if req.GetResponseHeaders() != nil {
			resp.ModeOverride = override
		}
		return resp
	}
	client := Dial(t, rec)
	ex := Exchange{
		Method: "POST", Scheme: "http", Authority: "svc", Path: "/",
		RequestBody: []byte("request"), Status: 200, ResponseBody: []byte("response"),
	}
	res, err := Run(context.Background(), client, ex, Options{Mode: capture.ModeInline})
	if err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, m := range rec.messages() {
		kinds = append(kinds, messageKind(m))
	}
	if want := []string{"request_headers", "request_body", "response_headers"}; !slices.Equal(kinds, want) {
		t.Errorf("messages = %v, want %v", kinds, want)
	}
	if !proto.Equal(res.ResponseModeOverride, override) || res.ModeOverride != nil {
		t.Errorf("overrides = %v / %v", res.ModeOverride, res.ResponseModeOverride)
	}
}
