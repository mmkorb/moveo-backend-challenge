package config_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/platform/config"
)

// allKeys lists every variable the loader reads, so each test starts from a
// clean environment regardless of the machine it runs on.
var allKeys = []string{
	"LOG_LEVEL", "HTTP_ADDR", "KAFKA_BROKERS", "KAFKA_TOPIC", "KAFKA_DLQ_TOPIC",
	"CONSUMER_GROUP", "CONSUMER_WORKERS", "FLUSH_INTERVAL", "FLUSH_MAX_RECORDS",
	"REVOKE_FLUSH_TIMEOUT", "RETRY_BASE_DELAY", "RETRY_MAX_DELAY", "STARTUP_TIMEOUT",
	"DRAIN_TIMEOUT", "HTTP_SHUTDOWN_TIMEOUT", "DATABASE_HOST", "DATABASE_PORT",
	"DATABASE_NAME", "DATABASE_USER", "DATABASE_PASSWORD_FILE", "DATABASE_SSLMODE",
	"DATABASE_MAX_CONNS", "DATABASE_MIN_CONNS", "DATABASE_STATEMENT_TIMEOUT",
	"FLUSH_TIMEOUT", "CONSUMER_INSTANCE_ID", "CONSUMER_SESSION_TIMEOUT",
}

// cleanEnv unsets every known variable (an empty value counts as unset) and points
// DATABASE_PASSWORD_FILE at a file holding the given password.
func cleanEnv(t *testing.T, password string) string {
	t.Helper()
	for _, k := range allKeys {
		t.Setenv(k, "")
	}
	path := filepath.Join(t.TempDir(), "password")
	if err := os.WriteFile(path, []byte(password), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	t.Setenv("DATABASE_PASSWORD_FILE", path)
	return path
}

func TestLoadDefaults(t *testing.T) {
	cleanEnv(t, "s3cret\n")

	got, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	host, err := os.Hostname()
	if err != nil {
		t.Fatalf("os.Hostname() error = %v", err)
	}
	want := config.Config{
		LogLevel: slog.LevelInfo,
		HTTPAddr: ":8081",
		Kafka: config.Kafka{
			Brokers: []string{"kafka:9092"}, Topic: "incoming.user_activity", DLQTopic: "incoming.user_activity.dlq", Group: "analytics-consumer",
			InstanceID: host, SessionTimeout: 30 * time.Second,
		},
		ConsumerWorkers:     3,
		FlushInterval:       time.Second,
		FlushMaxRecords:     500,
		FlushTimeout:        10 * time.Second,
		RevokeFlushTimeout:  10 * time.Second,
		RetryBaseDelay:      100 * time.Millisecond,
		RetryMaxDelay:       10 * time.Second,
		StartupTimeout:      60 * time.Second,
		DrainTimeout:        15 * time.Second,
		HTTPShutdownTimeout: 5 * time.Second,
		Postgres: config.Postgres{
			Host: "postgres", Port: 5432, Name: "analytics", User: "analytics_app",
			Password: "s3cret", SSLMode: "disable", MaxConns: 8, MinConns: 1,
			StatementTimeout: 5 * time.Second,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Load() mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadOverrides(t *testing.T) {
	cleanEnv(t, "pw")
	for k, v := range map[string]string{
		"LOG_LEVEL":                  "DEBUG",
		"HTTP_ADDR":                  "127.0.0.1:9100",
		"KAFKA_BROKERS":              "a:9092, b:9093 ,,c:9094",
		"KAFKA_TOPIC":                "t",
		"KAFKA_DLQ_TOPIC":            "t.dlq",
		"CONSUMER_GROUP":             "g",
		"CONSUMER_WORKERS":           "6",
		"FLUSH_INTERVAL":             "250ms",
		"FLUSH_MAX_RECORDS":          "50",
		"FLUSH_TIMEOUT":              "3s",
		"CONSUMER_INSTANCE_ID":       "consumer-a.1_x",
		"CONSUMER_SESSION_TIMEOUT":   "45s",
		"REVOKE_FLUSH_TIMEOUT":       "4s",
		"RETRY_BASE_DELAY":           "10ms",
		"RETRY_MAX_DELAY":            "2s",
		"STARTUP_TIMEOUT":            "30s",
		"DRAIN_TIMEOUT":              "12s",
		"HTTP_SHUTDOWN_TIMEOUT":      "3s",
		"DATABASE_HOST":              "db",
		"DATABASE_PORT":              "6543",
		"DATABASE_NAME":              "metrics",
		"DATABASE_USER":              "analytics_owner",
		"DATABASE_SSLMODE":           "verify-full",
		"DATABASE_MAX_CONNS":         "12",
		"DATABASE_MIN_CONNS":         "2",
		"DATABASE_STATEMENT_TIMEOUT": "750ms",
	} {
		t.Setenv(k, v)
	}

	got, err := config.Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}

	want := config.Config{
		LogLevel: slog.LevelDebug,
		HTTPAddr: "127.0.0.1:9100",
		Kafka: config.Kafka{
			Brokers: []string{"a:9092", "b:9093", "c:9094"}, Topic: "t", DLQTopic: "t.dlq", Group: "g",
			InstanceID: "consumer-a.1_x", SessionTimeout: 45 * time.Second,
		},
		ConsumerWorkers:     6,
		FlushInterval:       250 * time.Millisecond,
		FlushMaxRecords:     50,
		FlushTimeout:        3 * time.Second,
		RevokeFlushTimeout:  4 * time.Second,
		RetryBaseDelay:      10 * time.Millisecond,
		RetryMaxDelay:       2 * time.Second,
		StartupTimeout:      30 * time.Second,
		DrainTimeout:        12 * time.Second,
		HTTPShutdownTimeout: 3 * time.Second,
		Postgres: config.Postgres{
			Host: "db", Port: 6543, Name: "metrics", User: "analytics_owner",
			Password: "pw", SSLMode: "verify-full", MaxConns: 12, MinConns: 2,
			StatementTimeout: 750 * time.Millisecond,
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("Load() mismatch (-want +got):\n%s", diff)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string // substring of the error
	}{
		{"should reject a non-numeric integer", map[string]string{"CONSUMER_WORKERS": "three"}, `CONSUMER_WORKERS: "three" is not an integer`},
		{"should reject a bad duration", map[string]string{"FLUSH_INTERVAL": "soon"}, `FLUSH_INTERVAL: "soon" is not a duration`},
		{"should reject an unknown log level", map[string]string{"LOG_LEVEL": "trace"}, `LOG_LEVEL: "trace"`},
		{"should reject a 33-bit pool size", map[string]string{"DATABASE_MAX_CONNS": "4294967296"}, "DATABASE_MAX_CONNS"},
		{"should reject a listen address without port", map[string]string{"HTTP_ADDR": "localhost"}, "HTTP_ADDR"},
		{"should reject a listen port out of range", map[string]string{"HTTP_ADDR": ":70000"}, "HTTP_ADDR"},
		{"should reject a broker without port", map[string]string{"KAFKA_BROKERS": "kafka"}, `KAFKA_BROKERS: "kafka" is not host:port`},
		{"should reject an empty broker list", map[string]string{"KAFKA_BROKERS": " , "}, "KAFKA_BROKERS: no entries"},
		{"should reject zero workers", map[string]string{"CONSUMER_WORKERS": "0"}, "CONSUMER_WORKERS must be > 0"},
		{"should reject a negative flush size", map[string]string{"FLUSH_MAX_RECORDS": "-1"}, "FLUSH_MAX_RECORDS must be > 0"},
		{"should reject a zero statement timeout", map[string]string{"DATABASE_STATEMENT_TIMEOUT": "0s"}, "DATABASE_STATEMENT_TIMEOUT must be > 0"},
		{"should reject zero max connections", map[string]string{"DATABASE_MAX_CONNS": "0"}, "DATABASE_MAX_CONNS must be > 0"},
		{"should reject negative min connections", map[string]string{"DATABASE_MIN_CONNS": "-1"}, "DATABASE_MIN_CONNS must be >= 0"},
		{"should reject min above max connections", map[string]string{"DATABASE_MIN_CONNS": "9"}, "DATABASE_MIN_CONNS must be <= DATABASE_MAX_CONNS"},
		{"should reject a database port out of range", map[string]string{"DATABASE_PORT": "70000"}, "DATABASE_PORT: 70000 is out of range"},
		{"should reject an unknown sslmode", map[string]string{"DATABASE_SSLMODE": "maybe"}, `DATABASE_SSLMODE: "maybe"`},
		{"should reject retry base above max", map[string]string{"RETRY_BASE_DELAY": "20s"}, "RETRY_BASE_DELAY must be <= RETRY_MAX_DELAY"},
		{"should reject revoke timeout above drain timeout", map[string]string{"REVOKE_FLUSH_TIMEOUT": "16s"}, "REVOKE_FLUSH_TIMEOUT must be <= DRAIN_TIMEOUT"},
		{"should reject a shutdown budget above 20s", map[string]string{"HTTP_SHUTDOWN_TIMEOUT": "6s"}, "DRAIN_TIMEOUT + HTTP_SHUTDOWN_TIMEOUT must be <= 20s"},
		{"should reject identical main and DLQ topics", map[string]string{"KAFKA_DLQ_TOPIC": "incoming.user_activity"}, "KAFKA_DLQ_TOPIC must differ"},
		{"should reject a blank topic", map[string]string{"KAFKA_TOPIC": "   "}, "KAFKA_TOPIC must not be blank"},
		{"should reject a zero flush timeout", map[string]string{"FLUSH_TIMEOUT": "0s"}, "FLUSH_TIMEOUT must be > 0"},
		{"should reject a flush timeout below the statement timeout", map[string]string{"FLUSH_TIMEOUT": "4s"}, "FLUSH_TIMEOUT must be >= DATABASE_STATEMENT_TIMEOUT"},
		{"should reject a session timeout below the broker minimum", map[string]string{"CONSUMER_SESSION_TIMEOUT": "5s"}, "CONSUMER_SESSION_TIMEOUT must be within 6s..30m0s"},
		{"should reject a session timeout above the broker maximum", map[string]string{"CONSUMER_SESSION_TIMEOUT": "31m"}, "CONSUMER_SESSION_TIMEOUT must be within"},
		{"should reject an instance id with illegal characters", map[string]string{"CONSUMER_INSTANCE_ID": "host/1"}, `CONSUMER_INSTANCE_ID: "host/1" may contain only`},
		{"should reject the instance id ..", map[string]string{"CONSUMER_INSTANCE_ID": ".."}, `must not be "." or ".."`},
		{"should reject an instance id that is too long", map[string]string{"CONSUMER_INSTANCE_ID": strings.Repeat("a", 241)}, "must have 1..240 characters"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanEnv(t, "pw")
			for k, v := range tt.env {
				t.Setenv(k, v)
			}

			_, err := config.Load()

			if err == nil {
				t.Fatalf("Load() error = nil, want %q", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Errorf("Load() error = %q, want it to contain %q", err, tt.want)
			}
		})
	}
}

func TestLoadReportsAllProblemsAtOnce(t *testing.T) {
	cleanEnv(t, "pw")
	t.Setenv("CONSUMER_WORKERS", "x")
	t.Setenv("FLUSH_INTERVAL", "y")
	t.Setenv("DATABASE_SSLMODE", "z")

	_, err := config.Load()

	if err == nil {
		t.Fatal("Load() error = nil, want a joined error")
	}
	for _, key := range []string{"CONSUMER_WORKERS", "FLUSH_INTERVAL", "DATABASE_SSLMODE"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("Load() error = %q, want it to mention %s", err, key)
		}
	}
}

func TestPasswordFile(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		want     config.Secret
		wantFail string
	}{
		{name: "should trim one trailing LF", content: "pw\n", want: "pw"},
		{name: "should trim one trailing CRLF", content: "pw\r\n", want: "pw"},
		{name: "should keep inner whitespace", content: " p w ", want: " p w "},
		{name: "should reject an empty file", content: "", wantFail: "is empty"},
		{name: "should reject a newline-only file", content: "\n", wantFail: "is empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cleanEnv(t, tt.content)

			got, err := config.Load()

			if tt.wantFail != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantFail) {
					t.Fatalf("Load() error = %v, want it to contain %q", err, tt.wantFail)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load() error = %v", err)
			}
			if got.Postgres.Password != tt.want {
				t.Errorf("password = %q, want %q", got.Postgres.Password.Value(), tt.want.Value())
			}
		})
	}
}

func TestPasswordFileMissing(t *testing.T) {
	cleanEnv(t, "pw")
	missing := filepath.Join(t.TempDir(), "nope")
	t.Setenv("DATABASE_PASSWORD_FILE", missing)

	_, err := config.Load()

	if err == nil || !strings.Contains(err.Error(), "DATABASE_PASSWORD_FILE: read secret file") {
		t.Fatalf("Load() error = %v, want a read secret file error", err)
	}
}

func TestSecretNeverPrintsItsValue(t *testing.T) {
	cfg := config.Postgres{Password: "hunter2", User: "u"}

	var logs bytes.Buffer
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("connecting", "postgres", cfg, "password", cfg.Password)

	outputs := map[string]string{
		"%v":   fmt.Sprintf("%v", cfg),
		"%+v":  fmt.Sprintf("%+v", cfg),
		"%#v":  fmt.Sprintf("%#v", cfg),
		"%s":   fmt.Sprintf("%s", cfg.Password), //nolint:staticcheck // S1025: exercises the %s verb on purpose
		"slog": logs.String(),
	}
	for verb, out := range outputs {
		if strings.Contains(out, "hunter2") {
			t.Errorf("%s output leaks the secret: %s", verb, out)
		}
	}
	if !strings.Contains(outputs["%s"], "REDACTED") {
		t.Errorf("%%s of a secret = %q, want a REDACTED marker", outputs["%s"])
	}
	if got := cfg.Password.Value(); got != "hunter2" {
		t.Errorf("Value() = %q, want the real secret", got)
	}
}

func TestLoadMigrate(t *testing.T) {
	t.Run("should read the owner credentials and ignore unrelated variables", func(t *testing.T) {
		cleanEnv(t, "owner-pw")
		t.Setenv("KAFKA_BROKERS", "not-a-broker")
		t.Setenv("FLUSH_INTERVAL", "never")
		t.Setenv("DATABASE_USER", "analytics_owner")

		got, err := config.LoadMigrate()
		if err != nil {
			t.Fatalf("LoadMigrate() error = %v", err)
		}

		pg := got.Postgres
		if pg.User != "analytics_owner" || pg.Password.Value() != "owner-pw" || pg.Host != "postgres" {
			t.Errorf("LoadMigrate().Postgres = %+v, want owner credentials and default host", pg)
		}
		if got.StartupTimeout != 60*time.Second || got.RetryBaseDelay != 100*time.Millisecond || got.RetryMaxDelay != 10*time.Second || got.LogLevel != slog.LevelInfo {
			t.Errorf("LoadMigrate() = %+v, want the documented defaults", got)
		}
	})
	t.Run("should report every invalid database or startup setting", func(t *testing.T) {
		cleanEnv(t, "pw")
		t.Setenv("DATABASE_PORT", "0")
		t.Setenv("DATABASE_SSLMODE", "bogus")
		t.Setenv("STARTUP_TIMEOUT", "-1s")
		t.Setenv("RETRY_BASE_DELAY", "1m")

		_, err := config.LoadMigrate()

		if err == nil {
			t.Fatal("LoadMigrate() error = nil")
		}
		for _, want := range []string{"DATABASE_PORT", "DATABASE_SSLMODE", "STARTUP_TIMEOUT must be > 0", "RETRY_BASE_DELAY must be <= RETRY_MAX_DELAY"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("LoadMigrate() error = %q, want it to contain %q", err, want)
			}
		}
	})
	t.Run("should fail when the password file is missing", func(t *testing.T) {
		cleanEnv(t, "pw")
		t.Setenv("DATABASE_PASSWORD_FILE", filepath.Join(t.TempDir(), "absent"))

		_, err := config.LoadMigrate()

		if err == nil || !strings.Contains(err.Error(), "DATABASE_PASSWORD_FILE") {
			t.Errorf("LoadMigrate() error = %v, want a password file error", err)
		}
	})
}

func TestLoadHTTPAddr(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		want    string
		wantErr bool
	}{
		{name: "should default to :8081", addr: "", want: ":8081"},
		{name: "should accept host and port", addr: "127.0.0.1:9100", want: "127.0.0.1:9100"},
		{name: "should accept an ephemeral port", addr: ":0", want: ":0"},
		{name: "should reject a missing port", addr: "localhost", wantErr: true},
		{name: "should reject a non-numeric port", addr: ":http", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("HTTP_ADDR", tt.addr)

			got, err := config.LoadHTTPAddr()

			if (err != nil) != tt.wantErr {
				t.Fatalf("LoadHTTPAddr() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("LoadHTTPAddr() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestLoadHTTPAddrDoesNotNeedTheSecretFile(t *testing.T) {
	for _, k := range allKeys {
		t.Setenv(k, "")
	}
	t.Setenv("DATABASE_PASSWORD_FILE", filepath.Join(t.TempDir(), "absent"))

	if _, err := config.LoadHTTPAddr(); err != nil {
		t.Fatalf("LoadHTTPAddr() error = %v, want none: a probe must not depend on secrets", err)
	}
}
