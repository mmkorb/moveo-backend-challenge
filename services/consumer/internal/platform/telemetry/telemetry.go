// Package telemetry holds the consumer's cross-cutting observability code
// (ADR 0013): the JSON slog logger, the Prometheus registry and the small internal
// HTTP server that serves /healthz, /readyz and /metrics. It imports no domain,
// application or adapter package, and keeps no package-level state.
package telemetry

import (
	"io"
	"log/slog"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// ServiceName is the value of the "service" field on every log record.
const ServiceName = "consumer"

// NewLogger returns the process logger: JSON records on w at the given level, each
// carrying service=consumer. It never touches slog's default logger (ADR 0013).
func NewLogger(w io.Writer, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", ServiceName)
}

// NewRegistry returns a fresh registry with the Go runtime and process collectors
// registered. Components register their own collectors on it (ADR 0013); the
// default registry is never used.
func NewRegistry() *prometheus.Registry {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return reg
}
