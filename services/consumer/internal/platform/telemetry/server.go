package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	// readinessCacheTTL keeps frequent probes from hammering the dependencies
	// (ADR 0013: cached for 1-2 s).
	readinessCacheTTL = time.Second
	// readinessCheckTimeout bounds every dependency check.
	readinessCheckTimeout = 2 * time.Second
)

// ReadinessCheck is one dependency that must answer for the service to be ready.
type ReadinessCheck struct {
	// Name labels the check in logs and in the /readyz body. It must not contain
	// secrets.
	Name  string
	Check func(ctx context.Context) error
}

// Server is the consumer's internal HTTP server: GET /healthz (liveness, always
// 200), GET /readyz (readiness) and GET /metrics. It is never published by
// Compose (ADR 0013).
type Server struct {
	srv    *http.Server
	logger *slog.Logger
	now    func() time.Time

	notReady atomic.Bool // set by MarkNotReady

	mu         sync.Mutex
	checks     []ReadinessCheck
	cachedAt   time.Time
	cachedFail []string
	cachedOK   bool
	hasCache   bool
	lastOK     bool // last reported state, for transition logging
	hasLast    bool
}

// NewServer builds the server on addr. gatherer is normally the registry from
// NewRegistry. The checks decide /readyz; more can be added with AddReadinessCheck
// before serving.
func NewServer(addr string, gatherer prometheus.Gatherer, logger *slog.Logger, checks ...ReadinessCheck) *Server {
	s := &Server{logger: logger, now: time.Now, checks: checks}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)
	mux.Handle("GET /metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
	s.srv = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    8 << 10,
	}
	return s
}

// AddReadinessCheck registers one more dependency. Call it before the server
// starts serving; it is safe to call concurrently with probes.
func (s *Server) AddReadinessCheck(c ReadinessCheck) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.checks = append(s.checks, c)
	s.hasCache = false
}

// MarkNotReady makes /readyz answer 503 from now on (shutdown stage 1, ADR 0012).
// /healthz stays 200 during the drain.
func (s *Server) MarkNotReady() {
	s.notReady.Store(true)
}

// Handler returns the HTTP handler, for tests and for embedding.
func (s *Server) Handler() http.Handler { return s.srv.Handler }

// ListenAndServe listens on the configured address and serves until Shutdown. It
// returns http.ErrServerClosed after a graceful shutdown.
func (s *Server) ListenAndServe() error {
	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.srv.Addr, err)
	}
	return s.Serve(l)
}

// Serve serves on l until Shutdown. It returns http.ErrServerClosed after a
// graceful shutdown.
func (s *Server) Serve(l net.Listener) error {
	return s.srv.Serve(l)
}

// Shutdown stops accepting connections and waits for in-flight probes until ctx
// ends.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.srv.Shutdown(ctx)
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = fmt.Fprintln(w, "ok") // a failed probe write means the client left
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	failed, ok := s.readiness(r.Context())
	if ok {
		_, _ = fmt.Fprintln(w, "ok")
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, "not ready: %s\n", strings.Join(failed, ", "))
}

// readiness returns the names of the failing checks and whether the service is
// ready. The outcome is cached for readinessCacheTTL; MarkNotReady bypasses the cache.
func (s *Server) readiness(ctx context.Context) (failed []string, ok bool) {
	// Checked before taking the lock: shutdown must not wait for a slow check.
	if s.notReady.Load() {
		return []string{"shutting down"}, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notReady.Load() {
		return []string{"shutting down"}, false
	}
	now := s.now()
	if s.hasCache && now.Sub(s.cachedAt) < readinessCacheTTL {
		return s.cachedFail, s.cachedOK
	}
	var errs []error
	for _, c := range s.checks {
		cctx, cancel := context.WithTimeout(ctx, readinessCheckTimeout)
		err := c.Check(cctx)
		cancel()
		if err != nil {
			failed = append(failed, c.Name)
			errs = append(errs, fmt.Errorf("%s: %w", c.Name, err))
		}
	}
	ok = len(failed) == 0
	// A canceled probe says nothing about the dependencies: do not cache or log it.
	if ctx.Err() == nil {
		s.cachedAt, s.cachedFail, s.cachedOK, s.hasCache = now, failed, ok, true
		s.logTransition(ctx, ok, errors.Join(errs...))
	}
	return failed, ok
}

// logTransition logs only when the readiness state changes, so a long outage
// does not produce one record per probe.
func (s *Server) logTransition(ctx context.Context, ok bool, err error) {
	if s.hasLast && s.lastOK == ok {
		return
	}
	s.lastOK, s.hasLast = ok, true
	if ok {
		s.logger.InfoContext(ctx, "readiness changed", "ready", true)
		return
	}
	s.logger.WarnContext(ctx, "readiness changed", "ready", false, "error", err)
}

// Probe performs the healthcheck subcommand's request: GET /readyz on the local
// listener described by addr (the HTTP_ADDR value), with a 2 s timeout. It returns
// an error for a connection failure, a timeout or any status other than 200.
func Probe(ctx context.Context, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parse address %q: %w", addr, err)
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	url := "http://" + net.JoinHostPort(host, port) + "/readyz"
	// A one-shot client with its own transport: no shared global, no idle connections kept.
	client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("probe %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }() // read-only response, nothing to lose on a failed close
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probe %s: status %d", url, resp.StatusCode)
	}
	return nil
}
