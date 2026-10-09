package telemetry

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/attribute"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// teeFixture is a tee over a JSON handler and a real otelslog bridge backed
// by an in-memory exporter.
type teeFixture struct {
	handler slog.Handler
	buf     *syncBuffer
	exp     *memLogExporter
}

func newTeeFixture(t *testing.T, level slog.Level) *teeFixture {
	t.Helper()
	buf := &syncBuffer{}
	exp := &memLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	t.Cleanup(func() { _ = lp.Shutdown(context.Background()) })
	base := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})
	bridge := otelslog.NewHandler("test", otelslog.WithLoggerProvider(lp))
	return &teeFixture{handler: newTeeHandler(base, bridge), buf: buf, exp: exp}
}

// flatAttrs renders a record's attributes with dotted keys for groups.
func flatAttrs(rec sdklog.Record) map[string]string {
	out := make(map[string]string)
	var walk func(prefix string, kv attribute.KeyValue)
	walk = func(prefix string, kv attribute.KeyValue) {
		key := prefix + string(kv.Key)
		if kv.Value.Type() == attribute.MAP {
			for _, nested := range kv.Value.AsMap() {
				walk(key+".", nested)
			}
			return
		}
		out[key] = kv.Value.Emit()
	}
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		walk("", kv)
		return true
	})
	return out
}

func TestTeeHandlerTraceIDs(t *testing.T) {
	ctx := testContext(t)
	exp := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exp))
	t.Cleanup(func() { _ = tp.Shutdown(ctx) })
	recordingCtx, span := tp.Tracer("test").Start(ctx, "op")
	t.Cleanup(func() { span.End() })
	remoteCtx, _ := ContextWithRemoteParent(ctx, testTraceID, testSpanID, true)

	tests := []struct {
		name      string
		ctx       context.Context
		wantTrace string
		wantSpan  string
	}{
		{name: "no span", ctx: ctx},
		{name: "recording span", ctx: recordingCtx, wantTrace: span.SpanContext().TraceID().String(), wantSpan: span.SpanContext().SpanID().String()},
		{name: "remote parent only", ctx: remoteCtx, wantTrace: testTraceID, wantSpan: testSpanID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTeeFixture(t, slog.LevelInfo)
			slog.New(f.handler).InfoContext(tt.ctx, "hello", "k", "v")

			lines := f.buf.lines(t)
			if len(lines) != 1 {
				t.Fatalf("process log lines = %v, want one", lines)
			}
			gotTrace, _ := lines[0][LogKeyTraceID].(string)
			gotSpan, _ := lines[0][LogKeySpanID].(string)
			if gotTrace != tt.wantTrace || gotSpan != tt.wantSpan {
				t.Errorf("process line ids = %q/%q, want %q/%q", gotTrace, gotSpan, tt.wantTrace, tt.wantSpan)
			}
			if lines[0]["k"] != "v" || lines[0]["msg"] != "hello" {
				t.Errorf("process line = %v", lines[0])
			}

			recs := f.exp.all()
			if len(recs) != 1 {
				t.Fatalf("exported records = %d, want one", len(recs))
			}
			rec := recs[0]
			if rec.Body().AsString() != "hello" || flatAttrs(rec)["k"] != "v" {
				t.Errorf("exported record body %q attrs %v", rec.Body().AsString(), flatAttrs(rec))
			}
			if tt.wantTrace == "" {
				if rec.TraceID().IsValid() {
					t.Errorf("exported record has trace id %s without a span", rec.TraceID())
				}
				return
			}
			if rec.TraceID().String() != tt.wantTrace || rec.SpanID().String() != tt.wantSpan {
				t.Errorf("exported record ids = %s/%s, want %s/%s", rec.TraceID(), rec.SpanID(), tt.wantTrace, tt.wantSpan)
			}
			// The ids are not duplicated as attributes on the exported copy.
			if _, has := flatAttrs(rec)[LogKeyTraceID]; has {
				t.Error("exported record carries trace_id as an attribute")
			}
		})
	}
}

func TestTeeHandlerAttrsAndGroups(t *testing.T) {
	ctx := testContext(t)
	remoteCtx, _ := ContextWithRemoteParent(ctx, testTraceID, testSpanID, true)
	build := func(l *slog.Logger) *slog.Logger {
		return l.With("component", "tap").WithGroup("req").With("method", "GET")
	}
	tests := []struct {
		name string
		ctx  context.Context
		// want maps dotted paths in the process log line to values.
		want map[string]string
		// wantAbsent lists paths that must not exist in the process line.
		wantAbsent []string
		wantOTel   map[string]string
	}{
		{
			name:       "without span",
			ctx:        ctx,
			want:       map[string]string{"component": "tap", "req.method": "GET", "req.path": "/x", "msg": "hit"},
			wantAbsent: []string{LogKeyTraceID, "req." + LogKeyTraceID},
			wantOTel:   map[string]string{"component": "tap", "req.method": "GET", "req.path": "/x"},
		},
		{
			name: "with span the ids stay top level",
			ctx:  remoteCtx,
			want: map[string]string{
				"component": "tap", "req.method": "GET", "req.path": "/x",
				LogKeyTraceID: testTraceID, LogKeySpanID: testSpanID,
			},
			wantAbsent: []string{"req." + LogKeyTraceID, "req." + LogKeySpanID},
			wantOTel:   map[string]string{"component": "tap", "req.method": "GET", "req.path": "/x"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTeeFixture(t, slog.LevelInfo)
			build(slog.New(f.handler)).InfoContext(tt.ctx, "hit", "path", "/x")

			lines := f.buf.lines(t)
			if len(lines) != 1 {
				t.Fatalf("process log lines = %v, want one", lines)
			}
			for path, want := range tt.want {
				if got, ok := lookup(lines[0], path); !ok || got != want {
					t.Errorf("process line %s = %v (%v), want %q; line %v", path, got, ok, want, lines[0])
				}
			}
			for _, path := range tt.wantAbsent {
				if got, ok := lookup(lines[0], path); ok {
					t.Errorf("process line has %s = %v, want absent", path, got)
				}
			}

			recs := f.exp.all()
			if len(recs) != 1 {
				t.Fatalf("exported records = %d, want one", len(recs))
			}
			got := flatAttrs(recs[0])
			for k, want := range tt.wantOTel {
				if got[k] != want {
					t.Errorf("exported attribute %s = %q, want %q; all %v", k, got[k], want, got)
				}
			}
		})
	}
}

func TestTeeHandlerDoesNotDuplicateIDs(t *testing.T) {
	ctx := testContext(t)
	remoteCtx, _ := ContextWithRemoteParent(ctx, testTraceID, testSpanID, true)
	const manualTrace, manualSpan = "deadbeefdeadbeefdeadbeefdeadbeef", "0000000000000001"
	tests := []struct {
		name string
		log  func(l *slog.Logger)
		// want maps dotted paths in the process line to values; wantKeys
		// counts how often each key may appear anywhere in the raw line.
		want     map[string]string
		wantKeys map[string]int
	}{
		{
			name: "record attribute wins",
			log: func(l *slog.Logger) {
				l.LogAttrs(remoteCtx, slog.LevelInfo, "m", slog.String(LogKeyTraceID, manualTrace))
			},
			want:     map[string]string{LogKeyTraceID: manualTrace, LogKeySpanID: testSpanID},
			wantKeys: map[string]int{LogKeyTraceID: 1, LogKeySpanID: 1},
		},
		{
			name:     "bound attribute wins",
			log:      func(l *slog.Logger) { l.With(LogKeyTraceID, manualTrace).InfoContext(remoteCtx, "m") },
			want:     map[string]string{LogKeyTraceID: manualTrace, LogKeySpanID: testSpanID},
			wantKeys: map[string]int{LogKeyTraceID: 1, LogKeySpanID: 1},
		},
		{
			name: "both ids bound",
			log: func(l *slog.Logger) {
				l.With(LogKeyTraceID, manualTrace, LogKeySpanID, manualSpan).InfoContext(remoteCtx, "m")
			},
			want:     map[string]string{LogKeyTraceID: manualTrace, LogKeySpanID: manualSpan},
			wantKeys: map[string]int{LogKeyTraceID: 1, LogKeySpanID: 1},
		},
		{
			name:     "id bound inside a group does not count",
			log:      func(l *slog.Logger) { l.WithGroup("req").With(LogKeyTraceID, manualTrace).InfoContext(remoteCtx, "m") },
			want:     map[string]string{LogKeyTraceID: testTraceID, LogKeySpanID: testSpanID, "req." + LogKeyTraceID: manualTrace},
			wantKeys: map[string]int{LogKeyTraceID: 2, LogKeySpanID: 1},
		},
		{
			name:     "record attribute under an open group does not count",
			log:      func(l *slog.Logger) { l.WithGroup("req").InfoContext(remoteCtx, "m", LogKeyTraceID, manualTrace) },
			want:     map[string]string{LogKeyTraceID: testTraceID, LogKeySpanID: testSpanID, "req." + LogKeyTraceID: manualTrace},
			wantKeys: map[string]int{LogKeyTraceID: 2, LogKeySpanID: 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTeeFixture(t, slog.LevelInfo)
			tt.log(slog.New(f.handler))
			raw := f.buf.raw()
			if len(raw) != 1 {
				t.Fatalf("process log lines = %q, want one", raw)
			}
			for key, n := range tt.wantKeys {
				if got := strings.Count(raw[0], `"`+key+`":`); got != n {
					t.Errorf("key %s appears %d times, want %d: %s", key, got, n, raw[0])
				}
			}
			line := f.buf.lines(t)[0]
			for path, want := range tt.want {
				if got, ok := lookup(line, path); !ok || got != want {
					t.Errorf("%s = %v (%v), want %q; line %v", path, got, ok, want, line)
				}
			}
		})
	}
}

func TestTeeHandlerSiblingsDoNotShareOps(t *testing.T) {
	ctx := testContext(t)
	remoteCtx, _ := ContextWithRemoteParent(ctx, testTraceID, testSpanID, true)
	f := newTeeFixture(t, slog.LevelInfo)
	parent := slog.New(f.handler).With("component", "tap")
	a := parent.With("side", "a")
	b := parent.With("side", "b")
	a.InfoContext(remoteCtx, "from a")
	b.InfoContext(remoteCtx, "from b")

	lines := f.buf.lines(t)
	if len(lines) != 2 {
		t.Fatalf("process log lines = %v, want two", lines)
	}
	for i, want := range []string{"a", "b"} {
		if lines[i]["side"] != want || lines[i]["component"] != "tap" || lines[i][LogKeyTraceID] != testTraceID {
			t.Errorf("line %d = %v, want side %s", i, lines[i], want)
		}
	}
}

func TestTeeHandlerLevels(t *testing.T) {
	ctx := testContext(t)
	tests := []struct {
		name       string
		baseLevel  slog.Level
		level      slog.Level
		wantLogged bool
	}{
		{name: "debug below info is dropped everywhere", baseLevel: slog.LevelInfo, level: slog.LevelDebug},
		{name: "info at info passes", baseLevel: slog.LevelInfo, level: slog.LevelInfo, wantLogged: true},
		{name: "warn above info passes", baseLevel: slog.LevelInfo, level: slog.LevelWarn, wantLogged: true},
		{name: "debug at debug passes", baseLevel: slog.LevelDebug, level: slog.LevelDebug, wantLogged: true},
		{name: "info below error is dropped", baseLevel: slog.LevelError, level: slog.LevelInfo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newTeeFixture(t, tt.baseLevel)
			if got := f.handler.Enabled(ctx, tt.level); got != tt.wantLogged {
				t.Errorf("Enabled(%v) = %v, want %v", tt.level, got, tt.wantLogged)
			}
			slog.New(f.handler).Log(ctx, tt.level, "msg")
			// Handle called directly must apply the gate too.
			var rec slog.Record
			rec = slog.NewRecord(rec.Time, tt.level, "direct", 0)
			if err := f.handler.Handle(ctx, rec); err != nil {
				t.Fatalf("Handle: %v", err)
			}
			wantLines := 0
			if tt.wantLogged {
				wantLines = 2
			}
			if got := len(f.buf.lines(t)); got != wantLines {
				t.Errorf("process log lines = %d, want %d", got, wantLines)
			}
			if got := len(f.exp.all()); got != wantLines {
				t.Errorf("exported records = %d, want %d", got, wantLines)
			}
		})
	}
}

func TestTeeHandlerNoopDerivations(t *testing.T) {
	f := newTeeFixture(t, slog.LevelInfo)
	if got := f.handler.WithAttrs(nil); got != f.handler {
		t.Error("WithAttrs(nil) returned a new handler")
	}
	if got := f.handler.WithGroup(""); got != f.handler {
		t.Error(`WithGroup("") returned a new handler`)
	}
}

func TestTeeHandlerNilBase(t *testing.T) {
	ctx := testContext(t)
	exp := &memLogExporter{}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewSimpleProcessor(exp)))
	t.Cleanup(func() { _ = lp.Shutdown(ctx) })
	bridge := otelslog.NewHandler("test", otelslog.WithLoggerProvider(lp))

	h := newTeeHandler(nil, bridge)
	if !h.Enabled(ctx, slog.LevelDebug) {
		t.Fatal("tee without base is not enabled although the bridge is")
	}
	slog.New(h).With("a", 1).WithGroup("g").Info("only exported", "b", 2)
	recs := exp.all()
	if len(recs) != 1 {
		t.Fatalf("exported records = %d, want one", len(recs))
	}
	if got := flatAttrs(recs[0]); got["a"] != "1" || got["g.b"] != "2" {
		t.Errorf("exported attributes = %v", got)
	}

	empty := newTeeHandler(nil, nil)
	if empty.Enabled(ctx, slog.LevelError) {
		t.Error("tee without handlers is enabled")
	}
	if err := empty.Handle(ctx, slog.Record{}); err != nil {
		t.Errorf("Handle on empty tee = %v", err)
	}
}

// failingHandler reports an error on every record.
type failingHandler struct {
	slog.Handler
	err error
}

func (h failingHandler) Handle(context.Context, slog.Record) error { return h.err }

func TestTeeHandlerJoinsErrors(t *testing.T) {
	ctx := testContext(t)
	baseErr := errors.New("base failed")
	otelErr := errors.New("bridge failed")
	base := failingHandler{Handler: slog.NewJSONHandler(&syncBuffer{}, nil), err: baseErr}
	bridge := failingHandler{Handler: slog.NewJSONHandler(&syncBuffer{}, nil), err: otelErr}

	err := newTeeHandler(base, bridge).Handle(ctx, slog.NewRecord(slog.Record{}.Time, slog.LevelInfo, "x", 0))
	if !errors.Is(err, baseErr) || !errors.Is(err, otelErr) {
		t.Fatalf("Handle error = %v, want both %v and %v", err, baseErr, otelErr)
	}
	if !strings.Contains(err.Error(), "base failed") || !strings.Contains(err.Error(), "bridge failed") {
		t.Errorf("Handle error text = %q", err)
	}
}
