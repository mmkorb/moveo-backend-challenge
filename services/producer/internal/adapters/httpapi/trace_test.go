package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/tracectx"
)

const (
	incomingTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	incomingSpanID  = "00f067aa0ba902b7"
	incomingHeader  = "00-" + incomingTraceID + "-" + incomingSpanID + "-01"
)

func TestTrace(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		traceparent []string
		tracestate  []string
		sameTrace   bool
		wantState   string
	}{
		{"should continue a valid trace under a new span", []string{incomingHeader}, []string{"vendor=opaque"}, true, "vendor=opaque"},
		{"should join several tracestate headers", []string{incomingHeader}, []string{"a=1", "b=2"}, true, "a=1,b=2"},
		{"should drop tracestate longer than 512 bytes", []string{incomingHeader}, []string{strings.Repeat("a", 513)}, true, ""},
		{"should start a new trace without traceparent and drop tracestate", nil, []string{"vendor=opaque"}, false, ""},
		{"should start a new trace for an invalid traceparent", []string{"00-" + strings.ToUpper(incomingTraceID) + "-" + incomingSpanID + "-01"}, []string{"vendor=opaque"}, false, ""},
		{"should start a new trace for an all-zero trace id", []string{"00-00000000000000000000000000000000-" + incomingSpanID + "-01"}, nil, false, ""},
		{"should start a new trace for version ff", []string{"ff-" + incomingTraceID + "-" + incomingSpanID + "-01"}, nil, false, ""},
		{"should start a new trace for repeated traceparent headers", []string{incomingHeader, incomingHeader}, nil, false, ""},
		{"should continue a future version by its first four fields", []string{"cc-" + incomingTraceID + "-" + incomingSpanID + "-01-extra"}, nil, true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var seen tracectx.Context
			var ok bool
			h := Trace(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen, ok = tracectx.FromContext(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			for _, v := range tt.traceparent {
				req.Header.Add(tracectx.HeaderTraceparent, v)
			}
			for _, v := range tt.tracestate {
				req.Header.Add(tracectx.HeaderTracestate, v)
			}
			rec := httptest.NewRecorder()

			h.ServeHTTP(rec, req)

			if !ok {
				t.Fatal("handler context has no trace context")
			}
			if got := rec.Header().Get(tracectx.HeaderTraceparent); got != seen.Traceparent() {
				t.Errorf("response traceparent = %q, want the context span %q", got, seen.Traceparent())
			}
			if _, err := tracectx.Parse(rec.Header().Get(tracectx.HeaderTraceparent)); err != nil {
				t.Errorf("response traceparent is invalid: %v", err)
			}
			if (seen.TraceID == incomingTraceID) != tt.sameTrace {
				t.Errorf("trace id = %s, same trace as the caller = %v, want %v", seen.TraceID, seen.TraceID == incomingTraceID, tt.sameTrace)
			}
			if seen.SpanID == incomingSpanID {
				t.Error("the caller's span id was reused instead of a new one")
			}
			if seen.State != tt.wantState {
				t.Errorf("tracestate = %q, want %q", seen.State, tt.wantState)
			}
			if rec.Header().Get(tracectx.HeaderTracestate) != "" {
				t.Error("tracestate must not be echoed in the response")
			}
		})
	}
}

// TestRouterPropagatesTheTrace follows one request through the full chain: the
// registrar (and so the Kafka publisher) sees the producer's span, the
// response and every log line carry the same IDs, and X-Request-ID is intact.
func TestRouterPropagatesTheTrace(t *testing.T) {
	t.Parallel()
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(&syncWriter{w: logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var seen tracectx.Context
	h, err := NewRouter(Deps{
		Registrar: registrarFunc(func(ctx context.Context, _ domain.ActivityEvent) (uuid.UUID, error) {
			seen, _ = tracectx.FromContext(ctx)
			return fixedID, nil
		}),
		Metrics:        noMetrics{},
		Ready:          func(context.Context) error { return nil },
		MetricsHandler: http.NotFoundHandler(),
		Logger:         logger,
		PublishTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/activities", strings.NewReader(validBody))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(RequestIDHeader, "req-1")
	req.Header.Set(tracectx.HeaderTraceparent, incomingHeader)
	req.Header.Set(tracectx.HeaderTracestate, "vendor=opaque")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body)
	}
	if seen.TraceID != incomingTraceID || seen.SpanID == incomingSpanID || seen.State != "vendor=opaque" {
		t.Errorf("registrar context trace = %+v", seen)
	}
	if got := rec.Header().Get(tracectx.HeaderTraceparent); got != seen.Traceparent() {
		t.Errorf("response traceparent = %q, want %q", got, seen.Traceparent())
	}
	if got := rec.Header().Get(RequestIDHeader); got != "req-1" {
		t.Errorf("X-Request-ID = %q, want it unchanged", got)
	}

	lines := 0
	for line := range strings.Lines(logs.String()) {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line is not JSON: %v: %s", err, line)
		}
		lines++
		if rec["trace_id"] != incomingTraceID || rec["span_id"] != seen.SpanID || rec["request_id"] != "req-1" {
			t.Errorf("log line misses the correlation fields: %s", line)
		}
	}
	if lines < 2 { // "activity accepted" (debug) and the access log
		t.Errorf("got %d log lines, want at least 2", lines)
	}
}

func TestCorrelationWithoutTrace(t *testing.T) {
	t.Parallel()
	got := correlation(context.Background())
	if len(got) != 2 || got[0] != "request_id" {
		t.Errorf("correlation without a trace = %v, want only request_id", got)
	}
}
