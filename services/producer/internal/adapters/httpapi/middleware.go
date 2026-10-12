package httpapi

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log/slog"
	"net/http"
	"runtime/debug"
	"time"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/tracectx"
)

// Middleware wraps an http.Handler with a cross-cutting concern.
type Middleware func(http.Handler) http.Handler

// Chain wraps h so that the first middleware is the outermost one.
func Chain(h http.Handler, mws ...Middleware) http.Handler {
	for i := len(mws) - 1; i >= 0; i-- {
		h = mws[i](h)
	}
	return h
}

// RequestIDHeader carries the request ID in requests and responses.
const RequestIDHeader = "X-Request-ID"

type requestIDKey struct{}

// RequestIDFrom returns the request ID stored in ctx by RequestID, or "".
func RequestIDFrom(ctx context.Context) string {
	id, _ := ctx.Value(requestIDKey{}).(string)
	return id
}

// RequestID makes sure every request has an ID, keeps it in the context and
// echoes it in the X-Request-ID response header. A well-formed incoming ID is
// kept so callers can correlate; anything else is replaced.
func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if !validRequestID(id) {
			id = newRequestID()
		}
		w.Header().Set(RequestIDHeader, id)
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), requestIDKey{}, id)))
	})
}

// validRequestID accepts 1..64 characters of [A-Za-z0-9_-], which keeps log
// injection and oversized values out.
func validRequestID(id string) bool {
	if id == "" || len(id) > 64 {
		return false
	}
	for i := range len(id) {
		c := id[i]
		ok := c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_'
		if !ok {
			return false
		}
	}
	return true
}

func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error (Go 1.24+)
	return hex.EncodeToString(b[:])
}

// Trace continues the W3C trace of the request (ADR 0005, ADR 0013). It reads
// traceparent and tracestate, starts a new trace when traceparent is missing
// or invalid, gives this hop a new span ID, stores the result in the request
// context for the logs and the Kafka publisher, and returns the producer's
// span in the traceparent response header.
func Trace(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var traceparent string
		// More than one traceparent header is ambiguous: start a new trace.
		if v := r.Header.Values(tracectx.HeaderTraceparent); len(v) == 1 {
			traceparent = v[0]
		}
		tc := tracectx.Start(traceparent, r.Header.Values(tracectx.HeaderTracestate)...)
		w.Header().Set(tracectx.HeaderTraceparent, tc.Traceparent())
		next.ServeHTTP(w, r.WithContext(tracectx.NewContext(r.Context(), tc)))
	})
}

// correlation returns the log attributes that tie a record to one request and
// one trace: request_id always, trace_id and span_id when Trace ran.
func correlation(ctx context.Context) []any {
	attrs := []any{"request_id", RequestIDFrom(ctx)}
	if tc, ok := tracectx.FromContext(ctx); ok {
		attrs = append(attrs, "trace_id", tc.TraceID, "span_id", tc.SpanID)
	}
	return attrs
}

// statusWriter records the status code and whether the header was written.
type statusWriter struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
}

func (w *statusWriter) WriteHeader(code int) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = code, true
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *statusWriter) Write(b []byte) (int, error) {
	if !w.wroteHeader {
		w.status, w.wroteHeader = http.StatusOK, true
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap lets http.ResponseController reach the underlying writer.
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// Logging writes one access-log record per request with the method, the
// matched route pattern, the status and the duration. It never logs bodies,
// query strings, headers or metadata (ADR 0002). Probe routes log at debug.
func Logging(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			sw := &statusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(sw, r)

			pattern := r.Pattern // set by the mux on this same request
			if pattern == "" {
				pattern = "unmatched"
			}
			level := slog.LevelInfo
			if isProbe(pattern) {
				level = slog.LevelDebug
			}
			logger.Log(r.Context(), level, "http request", append(correlation(r.Context()),
				"method", r.Method,
				"pattern", pattern,
				"status", sw.status,
				"duration", time.Since(start),
			)...)
		})
	}
}

func isProbe(pattern string) bool {
	switch pattern {
	case "GET /healthz", "GET /readyz", "GET /metrics":
		return true
	}
	return false
}

// Recover turns a panic in a handler into a 500 problem response and one
// error log. http.ErrAbortHandler is re-panicked, as net/http expects.
func Recover(logger *slog.Logger) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				if rec == http.ErrAbortHandler { //nolint:errorlint // sentinel panic value, compared by identity per net/http
					panic(rec)
				}
				logger.ErrorContext(r.Context(), "panic in handler", append(correlation(r.Context()),
					"panic", rec,
					"stack", string(debug.Stack()),
				)...)
				if sw, ok := w.(*statusWriter); ok && sw.wroteHeader {
					return // too late to change the response
				}
				writeProblem(w, http.StatusInternalServerError, "", nil)
			}()
			next.ServeHTTP(w, r)
		})
	}
}
