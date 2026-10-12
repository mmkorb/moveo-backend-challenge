// Package httpapi is the producer's inbound HTTP adapter: the ingestion
// handler, the health and metrics routes, and the middleware chain.
package httpapi

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"
	"uuid"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/telemetry"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

// activityRegistrar is satisfied by *application.RegisterActivity.
type activityRegistrar interface {
	Register(ctx context.Context, ev domain.ActivityEvent) (uuid.UUID, error)
}

// publishCounter records the outcome of a publish request.
// *telemetry.Metrics satisfies it.
type publishCounter interface {
	ActivityPublished(result string)
}

type acceptedResponse struct {
	EventID string `json:"event_id"`
}

type activityHandler struct {
	reg            activityRegistrar
	log            *slog.Logger
	metrics        publishCounter
	publishTimeout time.Duration
}

// ServeHTTP implements POST /v1/activities.
func (h *activityHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !isJSON(r.Header.Get("Content-Type")) {
		h.metrics.ActivityPublished(telemetry.ResultInvalid)
		writeProblem(w, http.StatusUnsupportedMediaType, "", nil)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	ev, err := decodeActivity(r.Body)
	if err != nil {
		h.rejectBody(w, r, err)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), h.publishTimeout)
	defer cancel()
	id, err := h.reg.Register(ctx, ev)
	switch {
	case errors.Is(err, ports.ErrPublishUnavailable):
		h.metrics.ActivityPublished(telemetry.ResultKafkaError)
		h.log.WarnContext(r.Context(), "publish unavailable",
			append(correlation(r.Context()), "error", err)...)
		w.Header().Set("Retry-After", "1")
		writeProblem(w, http.StatusServiceUnavailable, "", nil)
	case err != nil:
		h.metrics.ActivityPublished(telemetry.ResultKafkaError)
		h.log.ErrorContext(r.Context(), "register activity",
			append(correlation(r.Context()), "error", err)...)
		writeProblem(w, http.StatusInternalServerError, "", nil)
	default:
		h.metrics.ActivityPublished(telemetry.ResultOK)
		h.log.DebugContext(r.Context(), "activity accepted",
			append(correlation(r.Context()), "event_id", id.String())...)
		writeJSON(w, http.StatusAccepted, acceptedResponse{EventID: id.String()})
	}
}

// rejectBody translates a decode failure into 413, 400 or 500.
func (h *activityHandler) rejectBody(w http.ResponseWriter, r *http.Request, err error) {
	var reqErr *requestError
	switch {
	case tooLarge(err):
		h.metrics.ActivityPublished(telemetry.ResultInvalid)
		writeProblem(w, http.StatusRequestEntityTooLarge, "", nil)
	case errors.As(err, &reqErr):
		h.metrics.ActivityPublished(telemetry.ResultInvalid)
		writeProblem(w, http.StatusBadRequest, reqErr.Detail, reqErr.Violations)
	default:
		h.log.ErrorContext(r.Context(), "decode activity",
			append(correlation(r.Context()), "error", err)...)
		writeProblem(w, http.StatusInternalServerError, "", nil)
	}
}
