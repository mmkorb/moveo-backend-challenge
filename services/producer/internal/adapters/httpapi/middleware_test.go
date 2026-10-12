package httpapi

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"uuid"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
)

func TestRequestID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		incoming string
		keep     bool
	}{
		{"should keep a well-formed incoming id", "abc-123_XYZ", true},
		{"should generate an id when none is sent", "", false},
		{"should replace an id with unsafe characters", "a b\r\nInjected: 1", false},
		{"should replace an oversized id", strings.Repeat("a", 65), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var seen string
			h := RequestID(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
				seen = RequestIDFrom(r.Context())
			}))
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.incoming != "" {
				req.Header.Set(RequestIDHeader, tt.incoming)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			got := rec.Header().Get(RequestIDHeader)
			if got == "" || got != seen {
				t.Fatalf("header %q and context %q must match and be set", got, seen)
			}
			if tt.keep && got != tt.incoming {
				t.Errorf("id = %q, want incoming %q", got, tt.incoming)
			}
			if !tt.keep && got == tt.incoming {
				t.Errorf("unsafe id %q was kept", got)
			}
			if !validRequestID(got) {
				t.Errorf("id %q is not well-formed", got)
			}
		})
	}
}

func TestChainOrder(t *testing.T) {
	t.Parallel()
	var order []string
	mw := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name+" in")
				next.ServeHTTP(w, r)
				order = append(order, name+" out")
			})
		}
	}
	h := Chain(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { order = append(order, "handler") }),
		mw("a"), mw("b"))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	want := "a in,b in,handler,b out,a out"
	if got := strings.Join(order, ","); got != want {
		t.Errorf("order = %s, want %s", got, want)
	}
}

func TestRecoverTurnsPanicIntoProblem(t *testing.T) {
	t.Parallel()
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(logs, nil))
	h, err := NewRouter(Deps{
		Registrar: registrarFunc(func(context.Context, domain.ActivityEvent) (uuid.UUID, error) {
			panic("boom: secret internals")
		}),
		Metrics:        noMetrics{},
		Ready:          func(context.Context) error { return nil },
		MetricsHandler: http.NotFoundHandler(),
		Logger:         logger,
		PublishTimeout: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/activities", strings.NewReader(validBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	p := decodeProblem(t, rec)
	if strings.Contains(rec.Body.String(), "secret internals") {
		t.Errorf("response leaks the panic value: %s", rec.Body)
	}
	if p.Detail != "internal server error" {
		t.Errorf("detail = %q", p.Detail)
	}
	if !strings.Contains(logs.String(), `"msg":"panic in handler"`) || !strings.Contains(logs.String(), "secret internals") {
		t.Errorf("panic was not logged once with its value: %s", logs)
	}
	if !strings.Contains(logs.String(), `"status":500`) {
		t.Errorf("access log did not record status 500: %s", logs)
	}
}

func TestRecoverRepanicsAbortHandler(t *testing.T) {
	t.Parallel()
	h := Recover(slog.New(slog.DiscardHandler))(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic(http.ErrAbortHandler)
	}))
	defer func() {
		if rec := recover(); !errors.Is(rec.(error), http.ErrAbortHandler) {
			t.Errorf("recovered %v, want http.ErrAbortHandler", rec)
		}
	}()
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))
}

func TestNewRouterRequiresDependencies(t *testing.T) {
	t.Parallel()
	if _, err := NewRouter(Deps{}); err == nil {
		t.Fatal("NewRouter(Deps{}) succeeded, want an error")
	}
}
