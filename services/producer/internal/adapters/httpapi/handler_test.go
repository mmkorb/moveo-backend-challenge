package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/telemetry"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

const validBody = `{"user_id":"12345","activity_type":"page_view","timestamp":"2024-07-01T12:34:56Z",` +
	`"metadata":{"page_url":"https://example.com/home","referrer":"https://google.com"}}`

var fixedID = uuid.MustParse("0199d3a4-7c2e-7b1a-9f3e-2a4b6c8d0e1f")

// fakeRegistrar records the events it receives and returns injected results.
type fakeRegistrar struct {
	mu     sync.Mutex
	events []domain.ActivityEvent
	err    error
	panics bool
}

func (f *fakeRegistrar) Register(_ context.Context, ev domain.ActivityEvent) (uuid.UUID, error) {
	if f.panics {
		panic("boom: secret internals")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
	if f.err != nil {
		return uuid.UUID{}, f.err
	}
	return fixedID, nil
}

func (f *fakeRegistrar) received() []domain.ActivityEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]domain.ActivityEvent(nil), f.events...)
}

type fixture struct {
	handler http.Handler
	reg     *fakeRegistrar
	metrics *telemetry.Metrics
	promReg *prometheus.Registry
	logs    *bytes.Buffer
}

func newFixture(t *testing.T, ready func(context.Context) error) *fixture {
	t.Helper()
	promReg := prometheus.NewRegistry()
	metrics, err := telemetry.NewMetrics(promReg)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	logs := &bytes.Buffer{}
	logger := slog.New(slog.NewJSONHandler(&syncWriter{w: logs}, &slog.HandlerOptions{Level: slog.LevelDebug}))
	if ready == nil {
		ready = func(context.Context) error { return nil }
	}
	reg := &fakeRegistrar{}
	h, err := NewRouter(Deps{
		Registrar:      reg,
		Metrics:        metrics,
		Ready:          ready,
		MetricsHandler: telemetry.Handler(promReg),
		Logger:         logger,
		PublishTimeout: time.Second,
	})
	if err != nil {
		t.Fatalf("NewRouter: %v", err)
	}
	return &fixture{handler: h, reg: reg, metrics: metrics, promReg: promReg, logs: logs}
}

type syncWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.w.Write(p)
}

func (f *fixture) do(method, target, contentType, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	rec := httptest.NewRecorder()
	f.handler.ServeHTTP(rec, req)
	return rec
}

func (f *fixture) count(t *testing.T, result string) float64 {
	t.Helper()
	c, err := f.promReg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	for _, mf := range c {
		if mf.GetName() != telemetry.MetricActivitiesPublished {
			continue
		}
		for _, m := range mf.GetMetric() {
			if m.GetLabel()[0].GetValue() == result {
				return m.GetCounter().GetValue()
			}
		}
	}
	t.Fatalf("metric %s{result=%s} not found", telemetry.MetricActivitiesPublished, result)
	return 0
}

type problemBody struct {
	Type   string         `json:"type"`
	Title  string         `json:"title"`
	Status int            `json:"status"`
	Detail string         `json:"detail"`
	Errors []problemField `json:"errors"`
}

func decodeProblem(t *testing.T, rec *httptest.ResponseRecorder) problemBody {
	t.Helper()
	if got := rec.Header().Get("Content-Type"); got != "application/problem+json" {
		t.Fatalf("Content-Type = %q, want application/problem+json", got)
	}
	var p problemBody
	if err := json.Unmarshal(rec.Body.Bytes(), &p); err != nil {
		t.Fatalf("decode problem %q: %v", rec.Body.String(), err)
	}
	if p.Status != rec.Code {
		t.Errorf("problem status %d != response status %d", p.Status, rec.Code)
	}
	return p
}

func TestPostActivityAccepted(t *testing.T) {
	t.Parallel()
	f := newFixture(t, nil)

	rec := f.do(http.MethodPost, "/v1/activities", "application/json", validBody)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	var resp struct {
		EventID string `json:"event_id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.EventID != fixedID.String() {
		t.Errorf("event_id = %q, want %q", resp.EventID, fixedID)
	}
	if rec.Header().Get(RequestIDHeader) == "" {
		t.Error("response has no X-Request-ID")
	}
	want := domain.ActivityEvent{
		UserID:       "12345",
		ActivityType: domain.ActivityPageView,
		Timestamp:    time.Date(2024, 7, 1, 12, 34, 56, 0, time.UTC),
		Metadata:     map[string]string{"page_url": "https://example.com/home", "referrer": "https://google.com"},
	}
	if diff := cmp.Diff([]domain.ActivityEvent{want}, f.reg.received()); diff != "" {
		t.Errorf("registered events mismatch (-want +got):\n%s", diff)
	}
	if got := f.count(t, telemetry.ResultOK); got != 1 {
		t.Errorf("ok counter = %v, want 1", got)
	}
}

func TestPostActivityAcceptedVariants(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name        string
		contentType string
		body        string
		wantUser    string
		wantType    domain.ActivityType
		wantTS      time.Time
	}{
		{
			name:        "should accept an unknown activity type",
			contentType: "application/json",
			body:        `{"user_id":"u","activity_type":"something_new","timestamp":"2024-07-01T00:00:00Z"}`,
			wantUser:    "u", wantType: "something_new",
			wantTS: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:        "should accept a charset parameter on the content type",
			contentType: "application/json; charset=utf-8",
			body:        `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T00:00:00Z"}`,
			wantUser:    "u", wantType: "click",
			wantTS: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:        "should normalize a non-UTC timestamp to UTC",
			contentType: "application/json",
			body:        `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T23:30:00-03:00"}`,
			wantUser:    "u", wantType: "click",
			wantTS: time.Date(2024, 7, 2, 2, 30, 0, 0, time.UTC),
		},
		{
			name:        "should accept fractional seconds",
			contentType: "application/json",
			body:        `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:00:00.123456Z"}`,
			wantUser:    "u", wantType: "click",
			wantTS: time.Date(2024, 7, 1, 12, 0, 0, 123456000, time.UTC),
		},
		{
			name:        "should accept null metadata",
			contentType: "application/json",
			body:        `{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:00:00Z","metadata":null}`,
			wantUser:    "u", wantType: "click",
			wantTS: time.Date(2024, 7, 1, 12, 0, 0, 0, time.UTC),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, nil)
			rec := f.do(http.MethodPost, "/v1/activities", tt.contentType, tt.body)
			if rec.Code != http.StatusAccepted {
				t.Fatalf("status = %d, want 202; body %s", rec.Code, rec.Body)
			}
			got := f.reg.received()
			if len(got) != 1 {
				t.Fatalf("registered %d events, want 1", len(got))
			}
			if got[0].UserID != tt.wantUser || got[0].ActivityType != tt.wantType || !got[0].Timestamp.Equal(tt.wantTS) {
				t.Errorf("event = %+v, want user %q type %q ts %v", got[0], tt.wantUser, tt.wantType, tt.wantTS)
			}
			if got[0].Timestamp.Location() != time.UTC {
				t.Errorf("timestamp location = %v, want UTC", got[0].Timestamp.Location())
			}
		})
	}
}

func TestPostActivityBadRequest(t *testing.T) {
	t.Parallel()
	longUser := strings.Repeat("u", domain.MaxUserIDBytes+1)
	tests := []struct {
		name       string
		body       string
		wantFields []string // fields expected in errors[]
		wantDetail string   // expected detail when there are no field errors
	}{
		{name: "should reject malformed JSON", body: `{"user_id":`, wantDetail: "body is not valid JSON"},
		{name: "should reject an empty body", body: ``, wantDetail: "body is empty"},
		{name: "should reject a JSON array", body: `[]`, wantDetail: "body must be a JSON object"},
		{name: "should reject an unknown field",
			body:       `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","extra":1}`,
			wantDetail: "body contains an unknown field"},
		{name: "should reject trailing data",
			body:       validBody + ` {"x":1}`,
			wantDetail: "body must contain exactly one JSON object"},
		{name: "should reject trailing garbage",
			body:       validBody + `x`,
			wantDetail: "body must contain exactly one JSON object"},
		{name: "should report a missing user_id", body: `{"activity_type":"a","timestamp":"2024-07-01T00:00:00Z"}`,
			wantFields: []string{"user_id"}},
		{name: "should report an oversized user_id",
			body:       `{"user_id":"` + longUser + `","activity_type":"a","timestamp":"2024-07-01T00:00:00Z"}`,
			wantFields: []string{"user_id"}},
		{name: "should report an empty activity_type",
			body:       `{"user_id":"u","activity_type":"","timestamp":"2024-07-01T00:00:00Z"}`,
			wantFields: []string{"activity_type"}},
		{name: "should report a missing timestamp", body: `{"user_id":"u","activity_type":"a"}`,
			wantFields: []string{"timestamp"}},
		{name: "should report a timestamp that is not RFC 3339",
			body:       `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01 12:00:00"}`,
			wantFields: []string{"timestamp"}},
		{name: "should report a timestamp whose UTC year is 10000",
			body:       `{"user_id":"u","activity_type":"a","timestamp":"9999-12-31T23:59:59-01:00"}`,
			wantFields: []string{"timestamp"}},
		{name: "should report a timestamp whose UTC year is 0",
			body:       `{"user_id":"u","activity_type":"a","timestamp":"0000-01-01T00:00:00+01:00"}`,
			wantFields: []string{"timestamp"}},
		{name: "should report a timestamp of the wrong JSON type",
			body:       `{"user_id":"u","activity_type":"a","timestamp":1719835200}`,
			wantFields: []string{"timestamp"}},
		{name: "should report a user_id of the wrong JSON type",
			body:       `{"user_id":5,"activity_type":"a","timestamp":"2024-07-01T00:00:00Z"}`,
			wantFields: []string{"user_id"}},
		{name: "should report non-string metadata values",
			body:       `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","metadata":{"n":1}}`,
			wantFields: []string{"metadata"}},
		{name: "should report too many metadata entries",
			body:       `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","metadata":` + manyEntries(domain.MaxMetadataEntries+1) + `}`,
			wantFields: []string{"metadata"}},
		{name: "should list every violated field",
			body:       `{"user_id":"","activity_type":"","timestamp":"nope"}`,
			wantFields: []string{"user_id", "activity_type", "timestamp"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, nil)
			rec := f.do(http.MethodPost, "/v1/activities", "application/json", tt.body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400; body %s", rec.Code, rec.Body)
			}
			p := decodeProblem(t, rec)
			var gotFields []string
			for _, e := range p.Errors {
				gotFields = append(gotFields, e.Field)
				if e.Rule == "" {
					t.Errorf("error for %q has no rule", e.Field)
				}
			}
			if diff := cmp.Diff(tt.wantFields, gotFields); diff != "" {
				t.Errorf("error fields mismatch (-want +got):\n%s", diff)
			}
			if tt.wantDetail != "" && p.Detail != tt.wantDetail {
				t.Errorf("detail = %q, want %q", p.Detail, tt.wantDetail)
			}
			if n := len(f.reg.received()); n != 0 {
				t.Errorf("registrar called %d times for an invalid request", n)
			}
			if got := f.count(t, telemetry.ResultInvalid); got != 1 {
				t.Errorf("invalid counter = %v, want 1", got)
			}
		})
	}
}

func manyEntries(n int) string {
	var b strings.Builder
	b.WriteByte('{')
	for i := range n {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`"k` + strings.Repeat("x", i%3) + string(rune('a'+i%26)) + string(rune('a'+i/26)) + `":"v"`)
	}
	b.WriteByte('}')
	return b.String()
}

func TestPostActivityBadRequestDoesNotEchoInput(t *testing.T) {
	t.Parallel()
	f := newFixture(t, nil)
	rec := f.do(http.MethodPost, "/v1/activities", "application/json",
		`{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","SECRET-FIELD":1}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "SECRET-FIELD") {
		t.Errorf("response echoes input: %s", rec.Body)
	}
}

func TestPostActivityPayloadTooLarge(t *testing.T) {
	t.Parallel()
	f := newFixture(t, nil)
	big := `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","metadata":{"k":"` +
		strings.Repeat("x", maxBodyBytes) + `"}}`

	rec := f.do(http.MethodPost, "/v1/activities", "application/json", big)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	decodeProblem(t, rec)
	if n := len(f.reg.received()); n != 0 {
		t.Errorf("registrar called %d times", n)
	}
}

func TestPostActivityUnsupportedMediaType(t *testing.T) {
	t.Parallel()
	for _, ct := range []string{"", "text/plain", "application/xml", "application/jsonx", ";;bad"} {
		t.Run("content type "+ct, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, nil)
			rec := f.do(http.MethodPost, "/v1/activities", ct, validBody)
			if rec.Code != http.StatusUnsupportedMediaType {
				t.Fatalf("status = %d, want 415", rec.Code)
			}
			decodeProblem(t, rec)
			if n := len(f.reg.received()); n != 0 {
				t.Errorf("registrar called %d times", n)
			}
		})
	}
}

func TestPostActivityPublishFailures(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name           string
		err            error
		wantStatus     int
		wantRetryAfter string
		wantLogLevel   string
	}{
		{
			name:           "should return 503 when ErrPublishUnavailable is wrapped",
			err:            joinUnavailable("dial tcp 10.0.0.5:9092: i/o timeout"),
			wantStatus:     http.StatusServiceUnavailable,
			wantRetryAfter: "1",
			wantLogLevel:   "WARN",
		},
		{
			name:         "should return a generic 500 for any other error",
			err:          errors.New("secret internal failure at broker-1.internal:9092"),
			wantStatus:   http.StatusInternalServerError,
			wantLogLevel: "ERROR",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, nil)
			f.reg.err = tt.err

			rec := f.do(http.MethodPost, "/v1/activities", "application/json", validBody)

			if rec.Code != tt.wantStatus {
				t.Fatalf("status = %d, want %d", rec.Code, tt.wantStatus)
			}
			if got := rec.Header().Get("Retry-After"); got != tt.wantRetryAfter {
				t.Errorf("Retry-After = %q, want %q", got, tt.wantRetryAfter)
			}
			p := decodeProblem(t, rec)
			for _, leak := range []string{"10.0.0.5", "broker-1", "i/o timeout", "secret internal"} {
				if strings.Contains(rec.Body.String(), leak) {
					t.Errorf("response leaks %q: %s", leak, rec.Body)
				}
			}
			if p.Detail == "" {
				t.Error("problem has no detail")
			}
			if got := f.count(t, telemetry.ResultKafkaError); got != 1 {
				t.Errorf("kafka_error counter = %v, want 1", got)
			}
			if tt.wantLogLevel != "" && !strings.Contains(f.logs.String(), `"level":"`+tt.wantLogLevel+`"`) {
				t.Errorf("no %s log record in %s", tt.wantLogLevel, f.logs)
			}
		})
	}
}

func joinUnavailable(cause string) error {
	return errors.Join(ports.ErrPublishUnavailable, errors.New(cause))
}

func TestPostActivityDoesNotLogPayload(t *testing.T) {
	t.Parallel()
	f := newFixture(t, nil)
	f.reg.err = joinUnavailable("broker down")
	f.do(http.MethodPost, "/v1/activities", "application/json", validBody)
	f.reg.err = nil
	f.do(http.MethodPost, "/v1/activities", "application/json", validBody)

	logs := f.logs.String()
	for _, secret := range []string{"https://example.com/home", "google.com", "12345", "page_view"} {
		if strings.Contains(logs, secret) {
			t.Errorf("logs contain %q: %s", secret, logs)
		}
	}
	if !strings.Contains(logs, `"request_id"`) || !strings.Contains(logs, `"pattern":"POST /v1/activities"`) {
		t.Errorf("access log is missing request_id or pattern: %s", logs)
	}
}

func TestPublishTimeoutBoundsTheRegistrarContext(t *testing.T) {
	t.Parallel()
	var deadline time.Time
	var hasDeadline bool
	h, err := NewRouter(Deps{
		Registrar: registrarFunc(func(ctx context.Context, _ domain.ActivityEvent) (uuid.UUID, error) {
			deadline, hasDeadline = ctx.Deadline()
			return fixedID, nil
		}),
		Metrics:        noMetrics{},
		Ready:          func(context.Context) error { return nil },
		MetricsHandler: http.NotFoundHandler(),
		Logger:         slog.New(slog.DiscardHandler),
		PublishTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/v1/activities", strings.NewReader(validBody))
	req.Header.Set("Content-Type", "application/json")
	h.ServeHTTP(httptest.NewRecorder(), req)

	if !hasDeadline {
		t.Fatal("registrar context has no deadline")
	}
	if d := deadline.Sub(start); d <= 0 || d > 3*time.Second+time.Second {
		t.Errorf("deadline in %v, want about 3s", d)
	}
}

type registrarFunc func(context.Context, domain.ActivityEvent) (uuid.UUID, error)

func (f registrarFunc) Register(ctx context.Context, ev domain.ActivityEvent) (uuid.UUID, error) {
	return f(ctx, ev)
}

type noMetrics struct{}

func (noMetrics) ActivityPublished(string) {}

func TestRoutingAndMethods(t *testing.T) {
	t.Parallel()
	f := newFixture(t, nil)
	tests := []struct {
		method, target string
		want           int
		wantAllow      string
	}{
		{http.MethodGet, "/v1/activities", http.StatusMethodNotAllowed, "POST"},
		{http.MethodPut, "/v1/activities", http.StatusMethodNotAllowed, "POST"},
		{http.MethodPost, "/healthz", http.StatusMethodNotAllowed, "GET, HEAD"},
		{http.MethodGet, "/nope", http.StatusNotFound, ""},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.target, func(t *testing.T) {
			t.Parallel()
			rec := f.do(tt.method, tt.target, "", "")
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if got := rec.Header().Get("Allow"); got != tt.wantAllow {
				t.Errorf("Allow = %q, want %q", got, tt.wantAllow)
			}
		})
	}
}

func TestHealthz(t *testing.T) {
	t.Parallel()
	f := newFixture(t, func(context.Context) error { return errors.New("kafka down") })
	rec := f.do(http.MethodGet, "/healthz", "", "")
	if rec.Code != http.StatusOK || strings.TrimSpace(rec.Body.String()) != `{"status":"ok"}` {
		t.Errorf("healthz = %d %s; liveness must not depend on dependencies", rec.Code, rec.Body)
	}
}

func TestReadyz(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		ready    func(context.Context) error
		want     int
		wantBody string
	}{
		{"should be 200 when ready", func(context.Context) error { return nil }, http.StatusOK, `{"status":"ready"}`},
		{"should be 503 without leaking the reason when not ready",
			func(context.Context) error { return errors.New("dial kafka:9092 refused") },
			http.StatusServiceUnavailable, `{"status":"unavailable"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, tt.ready)
			rec := f.do(http.MethodGet, "/readyz", "", "")
			if rec.Code != tt.want || strings.TrimSpace(rec.Body.String()) != tt.wantBody {
				t.Errorf("readyz = %d %s, want %d %s", rec.Code, rec.Body, tt.want, tt.wantBody)
			}
		})
	}
}

func TestReadyzCheckHasTimeout(t *testing.T) {
	t.Parallel()
	var hasDeadline bool
	f := newFixture(t, func(ctx context.Context) error { _, hasDeadline = ctx.Deadline(); return nil })
	f.do(http.MethodGet, "/readyz", "", "")
	if !hasDeadline {
		t.Error("readiness check context has no deadline")
	}
}

func TestMetricsEndpoint(t *testing.T) {
	t.Parallel()
	f := newFixture(t, nil)
	f.do(http.MethodPost, "/v1/activities", "application/json", validBody)

	rec := f.do(http.MethodGet, "/metrics", "", "")

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `activities_published_total{result="ok"} 1`) {
		t.Errorf("metrics output lacks the ok counter:\n%s", rec.Body)
	}
}
