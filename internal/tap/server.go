package tap

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"

	"github.com/Jenil133/Shadowtap/internal/capture"
)

// logInterval bounds how often a recurring problem (sink errors, a full queue,
// invalid exchanges, panics) is logged.
const logInterval = 10 * time.Second

// Server is the ext_proc server. Each Envoy stream is one HTTP request: Process
// samples it, buffers what it needs and, once the response is complete, hands
// the exchange to a bounded queue. Workers scrub queued exchanges and publish
// them to the sink.
//
// Process never mutates traffic: in inline mode every message gets a matching
// no-op reply, in observability mode nothing is sent. Metrics are prefixed
// shadowtap_tap_; see New.
type Server struct {
	extprocv3.UnimplementedExternalProcessorServer

	cfg      Config
	sampler  Sampler
	scrubber Scrubber
	sink     Sink
	log      *slog.Logger
	metrics  *metrics
	services *serviceLabels
	now      func() time.Time // exchange timestamps; replaced in tests

	queue         chan job
	mu            sync.RWMutex // guards closing; held for reading while sending on queue
	closing       bool
	stop          sync.Once
	abandon       context.CancelFunc // cancels the workers' context
	live          atomic.Int64       // workers still running
	pending       atomic.Int64       // queued or in-flight exchanges
	queued        atomic.Int64       // body bytes held by queued or in-flight exchanges
	maxQueueBytes int64
	done          chan struct{} // closed when the last worker exits

	queueFullLog, sinkErrLog, invalidLog, panicLog rate.Sometimes
}

// job is a finalized exchange waiting for a worker. Bodies travel as the raw
// bytes the stream kept; the worker turns them into Body data so the stream
// goroutine never pays for string conversion or base64.
type job struct {
	x        *capture.Exchange
	reqBody  []byte
	respBody []byte
	service  string // bounded metrics label
}

// size is what a job counts against MaxQueueBytes: its bodies, which
// dominate, plus an allowance for headers and bookkeeping.
func (j job) size() int64 { return int64(len(j.reqBody)+len(j.respBody)) + jobOverhead }

const jobOverhead = 4 << 10

// New validates cfg, registers the tap's metrics on reg (nil skips
// registration) and starts cfg.Workers workers. An empty cfg.Name defaults to
// the hostname; a nil log discards output. Call Shutdown to stop the workers.
//
// Metrics: streams_active, streams_total{mode}, decisions_total{service,decision},
// exchanges_total{service,outcome}, redactions_total{kind},
// body_omitted_total{reason}, queue_depth, message_seconds{type} and
// process_seconds, all prefixed shadowtap_tap_. The service label holds the
// first 100 distinct service names; later ones are counted as "other".
func New(cfg Config, sampler Sampler, scrubber Scrubber, sink Sink, reg prometheus.Registerer, log *slog.Logger) (*Server, error) {
	if err := validate(cfg, sampler, scrubber, sink); err != nil {
		return nil, err
	}
	// gRPC metadata keys are lowercase on the wire; normalizing once keeps the
	// per-stream lookup exact.
	cfg.ServiceMetadataKey = strings.ToLower(strings.TrimSpace(cfg.ServiceMetadataKey))
	if cfg.Name == "" {
		host, err := os.Hostname()
		if err != nil {
			return nil, fmt.Errorf("tap: name is empty and the hostname is unavailable: %w", err)
		}
		cfg.Name = host
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	if cfg.MaxQueueBytes == 0 {
		cfg.MaxQueueBytes = DefaultMaxQueueBytes
	}
	s := &Server{
		cfg:           cfg,
		sampler:       sampler,
		scrubber:      scrubber,
		sink:          sink,
		log:           log,
		services:      newServiceLabels(maxServiceLabels),
		now:           time.Now,
		queue:         make(chan job, cfg.QueueSize),
		maxQueueBytes: int64(cfg.MaxQueueBytes),
		done:          make(chan struct{}),
		queueFullLog:  rate.Sometimes{Interval: logInterval},
		sinkErrLog:    rate.Sometimes{Interval: logInterval},
		invalidLog:    rate.Sometimes{Interval: logInterval},
		panicLog:      rate.Sometimes{Interval: logInterval},
	}
	s.metrics = newMetrics(func() int { return len(s.queue) })
	if err := s.metrics.register(reg); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.abandon = cancel
	s.live.Store(int64(cfg.Workers))
	for range cfg.Workers {
		go s.work(ctx)
	}
	return s, nil
}

func validate(cfg Config, sampler Sampler, scrubber Scrubber, sink Sink) error {
	var errs []error
	if cfg.MaxBodyBytes <= 0 {
		errs = append(errs, fmt.Errorf("max_body_bytes must be positive, got %d", cfg.MaxBodyBytes))
	}
	if cfg.QueueSize <= 0 {
		errs = append(errs, fmt.Errorf("queue_size must be positive, got %d", cfg.QueueSize))
	}
	if cfg.MaxQueueBytes < 0 {
		errs = append(errs, fmt.Errorf("max_queue_bytes must not be negative, got %d", cfg.MaxQueueBytes))
	}
	if cfg.Workers <= 0 {
		errs = append(errs, fmt.Errorf("workers must be positive, got %d", cfg.Workers))
	}
	if strings.TrimSpace(cfg.ServiceMetadataKey) == "" {
		errs = append(errs, errors.New("service_metadata_key must not be empty"))
	}
	if sampler == nil {
		errs = append(errs, errors.New("sampler is nil"))
	}
	if scrubber == nil {
		errs = append(errs, errors.New("scrubber is nil"))
	}
	if sink == nil {
		errs = append(errs, errors.New("sink is nil"))
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("tap: invalid config: %w", err)
	}
	return nil
}

// Register registers s as the ExternalProcessor service on g.
func (s *Server) Register(g *grpc.Server) {
	extprocv3.RegisterExternalProcessorServer(g, s)
}

// Shutdown stops accepting exchanges and waits until the workers have scrubbed
// and published everything already queued. Streams keep being served (inline
// replies continue) but exchanges they finish from now on are dropped and
// counted as "shutdown". If ctx ends first, Shutdown logs how many exchanges
// were still queued, tells the workers to drop them instead of publishing, and
// returns ctx.Err(). It is safe to call more than once.
func (s *Server) Shutdown(ctx context.Context) error {
	s.stop.Do(func() {
		s.mu.Lock()
		s.closing = true
		close(s.queue)
		s.mu.Unlock()
	})
	select {
	case <-s.done:
		s.abandon()
		return nil
	default:
	}
	select {
	case <-s.done:
		s.abandon()
		return nil
	case <-ctx.Done():
		if s.pending.Load() == 0 {
			// Nothing to lose: idle workers exit as soon as they see the
			// closed, empty queue.
			<-s.done
			s.abandon()
			return nil
		}
		left := len(s.queue)
		s.abandon()
		s.log.Warn("tap: shutdown deadline passed before the queue drained; dropping the rest",
			"queued", left, "err", ctx.Err())
		return ctx.Err()
	}
}

// submit queues a finalized exchange without blocking.
func (s *Server) submit(j job) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closing {
		s.metrics.exchange(j.service, outcomeShutdown)
		return
	}
	size := j.size()
	if s.queued.Add(size) > s.maxQueueBytes {
		s.queued.Add(-size)
		s.dropQueueFull(j)
		return
	}
	s.pending.Add(1)
	select {
	case s.queue <- j:
	default:
		s.pending.Add(-1)
		s.queued.Add(-size)
		s.dropQueueFull(j)
	}
}

func (s *Server) dropQueueFull(j job) {
	s.metrics.exchange(j.service, outcomeQueueFull)
	s.queueFullLog.Do(func() {
		s.log.Warn("tap: queue full, dropping exchanges",
			"queue_size", cap(s.queue), "max_queue_bytes", s.maxQueueBytes)
	})
}

func (s *Server) work(ctx context.Context) {
	defer func() {
		if s.live.Add(-1) == 0 {
			close(s.done)
		}
	}()
	for j := range s.queue {
		if ctx.Err() != nil { // Shutdown gave up waiting
			s.metrics.exchange(j.service, outcomeShutdown)
		} else {
			start := time.Now()
			outcome := s.scrubAndPublish(ctx, j)
			s.metrics.processSeconds.Observe(time.Since(start).Seconds())
			s.metrics.exchange(j.service, outcome)
		}
		s.queued.Add(-j.size())
		s.pending.Add(-1)
	}
}

// scrubAndPublish scrubs, validates and publishes one exchange and returns
// the outcome. A panic in the scrubber or the sink is contained here: the exchange
// is dropped (a half-scrubbed exchange must never be published) and the
// worker carries on.
func (s *Server) scrubAndPublish(ctx context.Context, j job) (outcome string) {
	defer func() {
		if r := recover(); r != nil {
			s.logPanic("scrubbing or publishing an exchange", r)
			outcome = outcomePanic
		}
	}()
	x := j.x
	x.Request.Body.SetData(j.reqBody)
	x.Response.Body.SetData(j.respBody)
	s.scrubber.Exchange(x)
	s.metrics.scrubbed(x)
	if err := x.Validate(); err != nil {
		s.invalidLog.Do(func() {
			s.log.Warn("tap: dropping invalid exchange", "service", x.Service, "err", err)
		})
		return outcomeInvalid
	}
	if err := s.sink.Publish(ctx, x); err != nil {
		s.sinkErrLog.Do(func() {
			s.log.Warn("tap: sink rejected exchange", "service", x.Service, "err", err)
		})
		return outcomeSinkError
	}
	return outcomePublished
}

// logPanic logs a recovered panic. Only runtime errors are logged verbatim:
// other panic values may have been built from captured data, which must never
// reach the logs.
func (s *Server) logPanic(where string, r any) {
	detail := fmt.Sprintf("panic of type %T", r)
	if err, ok := r.(runtime.Error); ok {
		detail = err.Error()
	}
	s.panicLog.Do(func() {
		s.log.Error("tap: recovered from panic", "while", where, "panic", detail, "stack", string(debug.Stack()))
	})
}
