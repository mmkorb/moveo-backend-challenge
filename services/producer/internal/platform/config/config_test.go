package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

var allKeys = []string{
	"LOG_LEVEL", "HTTP_ADDR", "KAFKA_BROKERS", "KAFKA_TOPIC", "PUBLISH_TIMEOUT",
	"STARTUP_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT", "DRAIN_TIMEOUT",
}

// clearEnv makes every key look unset for the duration of the test.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range allKeys {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{
		LogLevel:            slog.LevelInfo,
		HTTPAddr:            ":8080",
		KafkaBrokers:        []string{"kafka:9092"},
		KafkaTopic:          "incoming.user_activity",
		PublishTimeout:      5 * time.Second,
		StartupTimeout:      60 * time.Second,
		HTTPShutdownTimeout: 12 * time.Second,
		DrainTimeout:        5 * time.Second,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Load() mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearEnv(t)
	t.Setenv("LOG_LEVEL", "debug")
	t.Setenv("HTTP_ADDR", "127.0.0.1:9090")
	t.Setenv("KAFKA_BROKERS", " b1:9092 , b2:9092,")
	t.Setenv("KAFKA_TOPIC", "t1")
	t.Setenv("PUBLISH_TIMEOUT", "2s")
	t.Setenv("STARTUP_TIMEOUT", "10s")
	t.Setenv("HTTP_SHUTDOWN_TIMEOUT", "3s")
	t.Setenv("DRAIN_TIMEOUT", "1s")
	got, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	want := Config{
		LogLevel:            slog.LevelDebug,
		HTTPAddr:            "127.0.0.1:9090",
		KafkaBrokers:        []string{"b1:9092", "b2:9092"},
		KafkaTopic:          "t1",
		PublishTimeout:      2 * time.Second,
		StartupTimeout:      10 * time.Second,
		HTTPShutdownTimeout: 3 * time.Second,
		DrainTimeout:        time.Second,
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Load() mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want []string // substrings that must all appear in the error
	}{
		{name: "should reject an unknown log level", env: map[string]string{"LOG_LEVEL": "loud"},
			want: []string{"LOG_LEVEL"}},
		{name: "should reject a malformed duration", env: map[string]string{"PUBLISH_TIMEOUT": "5"},
			want: []string{"PUBLISH_TIMEOUT"}},
		{name: "should reject a publish timeout below the Kafka client minimum", env: map[string]string{"PUBLISH_TIMEOUT": "500ms"},
			want: []string{"PUBLISH_TIMEOUT: must be at least 1s"}},
		{name: "should reject a non-positive duration", env: map[string]string{"STARTUP_TIMEOUT": "0s"},
			want: []string{"STARTUP_TIMEOUT: must be positive"}},
		{name: "should reject a broker without a port", env: map[string]string{"KAFKA_BROKERS": "kafka"},
			want: []string{"KAFKA_BROKERS"}},
		{name: "should reject an empty broker list", env: map[string]string{"KAFKA_BROKERS": ","},
			want: []string{"at least one broker"}},
		{name: "should reject an address without a port", env: map[string]string{"HTTP_ADDR": "localhost"},
			want: []string{"HTTP_ADDR"}},
		{name: "should reject a shutdown timeout not above the publish timeout",
			env:  map[string]string{"HTTP_SHUTDOWN_TIMEOUT": "5s"},
			want: []string{"must exceed PUBLISH_TIMEOUT"}},
		{name: "should reject stage deadlines above the 20s budget",
			env:  map[string]string{"HTTP_SHUTDOWN_TIMEOUT": "16s", "DRAIN_TIMEOUT": "5s"},
			want: []string{"shutdown budget"}},
		{name: "should report every problem at once",
			env:  map[string]string{"LOG_LEVEL": "loud", "KAFKA_BROKERS": "kafka", "DRAIN_TIMEOUT": "x"},
			want: []string{"LOG_LEVEL", "KAFKA_BROKERS", "DRAIN_TIMEOUT"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tt.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Fatal("Load() error = nil, want error")
			}
			for _, s := range tt.want {
				if !strings.Contains(err.Error(), s) {
					t.Errorf("Load() error = %q, want it to contain %q", err, s)
				}
			}
		})
	}
}

func TestProbeURL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		addr    string
		want    string
		wantErr bool
	}{
		{addr: ":8080", want: "http://127.0.0.1:8080/readyz"},
		{addr: "0.0.0.0:9000", want: "http://127.0.0.1:9000/readyz"},
		{addr: "8080", wantErr: true},
		{addr: ":http", wantErr: true},
		{addr: ":70000", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.addr, func(t *testing.T) {
			t.Parallel()
			got, err := ProbeURL(tt.addr)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ProbeURL(%q) error = %v, wantErr %v", tt.addr, err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("ProbeURL(%q) = %q, want %q", tt.addr, got, tt.want)
			}
		})
	}
}

func TestHealthcheckURL(t *testing.T) {
	clearEnv(t)
	t.Setenv("HTTP_ADDR", ":8181")
	t.Setenv("LOG_LEVEL", "invalid") // unrelated settings must not matter
	got, err := HealthcheckURL()
	if err != nil {
		t.Fatalf("HealthcheckURL() error = %v", err)
	}
	if want := "http://127.0.0.1:8181/readyz"; got != want {
		t.Errorf("HealthcheckURL() = %q, want %q", got, want)
	}
}
