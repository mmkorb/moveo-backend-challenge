package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

// newClient returns a client whose idle connections are closed on cleanup,
// so goleak sees no leftover connection goroutines.
func newClient(t *testing.T) *http.Client {
	t.Helper()
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr, Timeout: 5 * time.Second}
}

func get(t *testing.T, c *http.Client, url string) (int, string) {
	t.Helper()
	resp, err := c.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestRunServesAndShutsDownGracefully drives the whole composition root with
// no broker: liveness is up, readiness and publishing report Kafka as
// unavailable, and cancelling the context (the SIGTERM path) stops every
// component cleanly with a nil error and no leaked goroutines.
func TestRunServesAndShutsDownGracefully(t *testing.T) {
	addr := freeAddr(t)
	t.Setenv("HTTP_ADDR", addr)
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("PUBLISH_TIMEOUT", "1s")
	t.Setenv("LOG_LEVEL", "debug")
	logs := &lockedBuffer{}
	client := newClient(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- run(ctx, logs) }()

	base := "http://" + addr
	waitFor(t, "the server to listen", func() bool {
		resp, err := client.Get(base + "/healthz")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	if code, _ := get(t, client, base+"/readyz"); code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d with no broker, want 503", code)
	}
	resp, err := client.Post(base+"/v1/activities", "application/json",
		strings.NewReader(`{"user_id":"u","activity_type":"page_view","timestamp":"2024-07-01T12:00:00Z"}`))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || resp.Header.Get("Retry-After") != "1" {
		t.Errorf("POST with no broker = %d Retry-After=%q, want 503 and 1", resp.StatusCode, resp.Header.Get("Retry-After"))
	}
	if code, body := get(t, client, base+"/metrics"); code != http.StatusOK ||
		!strings.Contains(body, `activities_published_total{result="kafka_error"} 1`) ||
		!strings.Contains(body, "retries_total") {
		t.Errorf("/metrics = %d, lacks the kafka_error counter or retries_total", code)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned %v after a shutdown signal, want nil", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not stop within 15s of cancellation")
	}
	for _, want := range []string{`"stage":"http_drain"`, `"stage":"flush"`, `"msg":"producer stopped"`, `"service":"producer"`} {
		if !strings.Contains(logs.String(), want) {
			t.Errorf("logs lack %s:\n%s", want, logs)
		}
	}
	if strings.Contains(logs.String(), "page_view") || strings.Contains(logs.String(), `"user_id"`) {
		t.Errorf("logs contain request payload:\n%s", logs)
	}
}

func TestRunFailsFastOnInvalidConfig(t *testing.T) {
	t.Setenv("HTTP_ADDR", "nonsense")
	t.Setenv("PUBLISH_TIMEOUT", "later")
	err := run(context.Background(), io.Discard)
	if err == nil || !strings.Contains(err.Error(), "HTTP_ADDR") || !strings.Contains(err.Error(), "PUBLISH_TIMEOUT") {
		t.Fatalf("err = %v, want every config problem reported", err)
	}
}

func TestRunFailsWhenStartupCheckTimesOut(t *testing.T) {
	t.Setenv("HTTP_ADDR", freeAddr(t))
	t.Setenv("KAFKA_BROKERS", "127.0.0.1:1")
	t.Setenv("STARTUP_TIMEOUT", "300ms")

	done := make(chan error, 1)
	go func() { done <- run(context.Background(), io.Discard) }()

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "kafka startup check") {
			t.Fatalf("err = %v, want a startup check failure", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("run did not give up after STARTUP_TIMEOUT")
	}
}

func TestHealthcheckProbe(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr bool
	}{
		{"should pass on 200", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }, false},
		{"should fail on 503", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }, true},
		{"should fail on 404", func(w http.ResponseWriter, _ *http.Request) { http.NotFound(w, nil) }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()
			err := probe(context.Background(), srv.Client(), srv.URL)
			if (err != nil) != tt.wantErr {
				t.Errorf("probe err = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestHealthcheckFailsWhenNothingListens(t *testing.T) {
	t.Setenv("HTTP_ADDR", ":"+strconv.Itoa(unusedPort(t)))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := healthcheck(ctx); err == nil {
		t.Error("healthcheck succeeded with nothing listening")
	}
}

func unusedPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func TestDispatchRejectsUnknownCommand(t *testing.T) {
	t.Parallel()
	if err := dispatch([]string{"bogus"}); err == nil || !strings.Contains(err.Error(), "bogus") {
		t.Errorf("dispatch(bogus) = %v, want an unknown command error", err)
	}
}

func TestNewServerSetsEveryTimeout(t *testing.T) {
	t.Parallel()
	srv := newServer(":8080", http.NotFoundHandler(), slog.New(slog.DiscardHandler))
	if srv.ReadHeaderTimeout <= 0 || srv.ReadTimeout <= 0 || srv.WriteTimeout <= 0 || srv.IdleTimeout <= 0 || srv.MaxHeaderBytes <= 0 {
		t.Errorf("server limits not all set: %+v", srv)
	}
	if srv.WriteTimeout < 5*time.Second {
		t.Errorf("WriteTimeout %v must exceed PUBLISH_TIMEOUT", srv.WriteTimeout)
	}
	if srv.ErrorLog == nil {
		t.Error("ErrorLog must route through slog")
	}
}
