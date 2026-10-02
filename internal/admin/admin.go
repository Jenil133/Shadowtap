// Package admin serves the operational HTTP endpoints of Shadowtap
// processes: liveness, readiness and Prometheus metrics.
package admin

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server serves /healthz, /readyz and /metrics.
type Server struct {
	srv *http.Server
}

// New returns an admin server. ready is called for every /readyz request and
// should be cheap (see Readiness).
func New(gatherer prometheus.Gatherer, ready func(context.Context) error, log *slog.Logger) *Server {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if err := ready(r.Context()); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("not ready: " + err.Error() + "\n"))
			return
		}
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelError),
		EnableOpenMetrics: true,
	}))
	return &Server{srv: &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
	}}
}

// Serve accepts connections on ln until Shutdown. It returns nil after a
// clean shutdown.
func (s *Server) Serve(ln net.Listener) error {
	if err := s.srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Shutdown stops the server, waiting for in-flight requests until ctx ends.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

// Readiness answers readiness probes from a cached dependency check, so
// frequent probes do not hammer the dependency, and reports not ready once
// draining has started.
type Readiness struct {
	check    func(context.Context) error
	ttl      time.Duration
	timeout  time.Duration
	now      func() time.Time
	draining atomic.Bool

	mu      sync.Mutex
	checked time.Time
	lastErr error
}

// NewReadiness caches the result of check for ttl.
func NewReadiness(check func(context.Context) error, ttl time.Duration) *Readiness {
	return &Readiness{check: check, ttl: ttl, timeout: 2 * time.Second, now: time.Now}
}

// errDraining is reported once SetDraining has been called.
var errDraining = errors.New("draining")

// SetDraining makes every later check fail, so load balancers stop sending
// new work while the process shuts down.
func (r *Readiness) SetDraining() { r.draining.Store(true) }

// Check reports whether the process is ready.
func (r *Readiness) Check(ctx context.Context) error {
	if r.draining.Load() {
		return errDraining
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.checked.IsZero() && r.now().Sub(r.checked) < r.ttl {
		return r.lastErr
	}
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()
	r.lastErr = r.check(ctx)
	r.checked = r.now()
	return r.lastErr
}
