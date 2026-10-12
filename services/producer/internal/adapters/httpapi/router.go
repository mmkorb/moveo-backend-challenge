package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
)

// readyTimeout bounds the dependency check behind /readyz (ADR 0005).
const readyTimeout = time.Second

// Deps are the collaborators of the router. All fields are required.
type Deps struct {
	// Registrar is the RegisterActivity use case.
	Registrar activityRegistrar
	// Metrics counts publish outcomes.
	Metrics publishCounter
	// Ready reports whether the service can do useful work (Kafka reachable
	// and not shutting down). It backs GET /readyz.
	Ready func(context.Context) error
	// MetricsHandler serves GET /metrics.
	MetricsHandler http.Handler
	Logger         *slog.Logger
	// PublishTimeout bounds one publish, from the request to the broker ack.
	PublishTimeout time.Duration
}

func (d Deps) validate() error {
	switch {
	case d.Registrar == nil:
		return errors.New("registrar is required")
	case d.Metrics == nil:
		return errors.New("metrics is required")
	case d.Ready == nil:
		return errors.New("readiness check is required")
	case d.MetricsHandler == nil:
		return errors.New("metrics handler is required")
	case d.Logger == nil:
		return errors.New("logger is required")
	case d.PublishTimeout <= 0:
		return errors.New("publish timeout must be positive")
	}
	return nil
}

// NewRouter builds the HTTP handler: the ingestion route, the operational
// routes and the middleware chain (request ID, trace context, access log, recover).
func NewRouter(d Deps) (http.Handler, error) {
	if err := d.validate(); err != nil {
		return nil, errors.Join(errors.New("httpapi router"), err)
	}
	mux := http.NewServeMux()
	mux.Handle("POST /v1/activities", &activityHandler{
		reg:            d.Registrar,
		log:            d.Logger,
		metrics:        d.Metrics,
		publishTimeout: d.PublishTimeout,
	})
	mux.HandleFunc("GET /healthz", healthz)
	mux.HandleFunc("GET /readyz", readyz(d.Ready, d.Logger))
	mux.Handle("GET /metrics", d.MetricsHandler)
	return Chain(mux, RequestID, Trace, Logging(d.Logger), Recover(d.Logger)), nil
}

type statusResponse struct {
	Status string `json:"status"`
}

// healthz is liveness: it never checks a dependency (ADR 0013).
func healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, statusResponse{Status: "ok"})
}

// readyz is readiness: 200 when the dependency check passes, else 503. The
// failure reason is logged at debug and never put in the response.
func readyz(ready func(context.Context) error, logger *slog.Logger) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readyTimeout)
		defer cancel()
		if err := ready(ctx); err != nil {
			logger.DebugContext(r.Context(), "not ready",
				append(correlation(r.Context()), "error", err)...)
			writeJSON(w, http.StatusServiceUnavailable, statusResponse{Status: "unavailable"})
			return
		}
		writeJSON(w, http.StatusOK, statusResponse{Status: "ready"})
	}
}
