// Package telemetry builds the producer's observability primitives: the JSON
// logger, the Prometheus registry and metrics, and the readiness state. It is
// created once in main and injected; there are no package-level collectors.
package telemetry

import (
	"io"
	"log/slog"
)

// NewLogger returns a JSON logger that writes to w and stamps every record
// with the service name (ADR 0013). Callers inject it; nothing calls
// slog.SetDefault.
func NewLogger(w io.Writer, service string, level slog.Level) *slog.Logger {
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: level})
	return slog.New(h).With("service", service)
}
