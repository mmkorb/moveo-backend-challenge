// Package config loads and validates the consumer's configuration from the
// environment (ADR 0003 §4). It is parsed once, in main, and passed down as a
// plain struct. Load reports every problem at once via errors.Join.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// MaxShutdownBudget is the upper bound for DRAIN_TIMEOUT + HTTP_SHUTDOWN_TIMEOUT
// (ADR 0012: 20 s budget inside Compose's 30 s stop_grace_period).
const MaxShutdownBudget = 20 * time.Second

// DefaultHTTPAddr is the consumer's internal health/metrics address (ADR 0013).
const DefaultHTTPAddr = ":8081"

// Bounds of CONSUMER_SESSION_TIMEOUT: the broker defaults of
// group.min.session.timeout.ms and group.max.session.timeout.ms (Kafka 4.3).
const (
	MinSessionTimeout = 6 * time.Second
	MaxSessionTimeout = 30 * time.Minute
)

// maxInstanceIDLen leaves room for the "-<member>" suffix within Kafka's
// 249-character limit on group.instance.id.
const maxInstanceIDLen = 240

// Secret is a string that never prints its value (fmt, %v, slog).
type Secret string

// String implements fmt.Stringer without revealing the value.
func (Secret) String() string { return "[REDACTED]" }

// GoString implements fmt.GoStringer without revealing the value.
func (Secret) GoString() string { return `"[REDACTED]"` }

// LogValue implements slog.LogValuer without revealing the value.
func (Secret) LogValue() slog.Value { return slog.StringValue("[REDACTED]") }

// MarshalText implements encoding.TextMarshaler so that JSON encoders (including
// slog's JSON handler, which marshals struct values) never reveal the value.
func (Secret) MarshalText() ([]byte, error) { return []byte("[REDACTED]"), nil }

// Value returns the secret itself; call it only where the secret is consumed.
func (s Secret) Value() string { return string(s) }

// Kafka holds the broker, topic and group settings (ADR 0007, 0008, 0011,
// 0012).
type Kafka struct {
	Brokers  []string
	Topic    string
	DLQTopic string
	Group    string
	// InstanceID is the stable identity of this replica for static group
	// membership (CONSUMER_INSTANCE_ID, default: the hostname). Member i of the
	// process joins as "<InstanceID>-<i>".
	InstanceID string
	// SessionTimeout is the group session timeout (CONSUMER_SESSION_TIMEOUT).
	SessionTimeout time.Duration
}

// Postgres holds the database connection settings (ADR 0010, 0002).
type Postgres struct {
	Host             string
	Port             int
	Name             string
	User             string
	Password         Secret
	SSLMode          string
	MaxConns         int32
	MinConns         int32
	StatementTimeout time.Duration
}

// Config is the complete, validated consumer configuration.
type Config struct {
	LogLevel            slog.Level
	HTTPAddr            string
	Kafka               Kafka
	ConsumerWorkers     int
	FlushInterval       time.Duration
	FlushMaxRecords     int
	FlushTimeout        time.Duration
	RevokeFlushTimeout  time.Duration
	RetryBaseDelay      time.Duration
	RetryMaxDelay       time.Duration
	StartupTimeout      time.Duration
	DrainTimeout        time.Duration
	HTTPShutdownTimeout time.Duration
	Postgres            Postgres
}

var sslModes = []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}

// Load reads every variable of the ADR 0003 table, applies defaults for unset or
// empty ones, reads DATABASE_PASSWORD_FILE and validates the result. All problems
// are returned together.
func Load() (Config, error) {
	var errs []error
	cfg := Config{
		LogLevel: logLevelVar("LOG_LEVEL", slog.LevelInfo, &errs),
		HTTPAddr: addrVar("HTTP_ADDR", DefaultHTTPAddr, &errs),
		Kafka: Kafka{
			Brokers:  listVar("KAFKA_BROKERS", []string{"kafka:9092"}, &errs),
			Topic:    stringVar("KAFKA_TOPIC", "incoming.user_activity"),
			DLQTopic: stringVar("KAFKA_DLQ_TOPIC", "incoming.user_activity.dlq"),
			Group:    stringVar("CONSUMER_GROUP", "analytics-consumer"),
			// Static membership (ADR 0012): a replica that restarts within the
			// session timeout gets its partitions back at once; one that never
			// returns after a crash holds them for the session timeout.
			InstanceID:     instanceIDVar("CONSUMER_INSTANCE_ID", &errs),
			SessionTimeout: durationVar("CONSUMER_SESSION_TIMEOUT", 30*time.Second, &errs),
		},
		ConsumerWorkers:     intVar("CONSUMER_WORKERS", 3, &errs),
		FlushInterval:       durationVar("FLUSH_INTERVAL", time.Second, &errs),
		FlushMaxRecords:     intVar("FLUSH_MAX_RECORDS", 500, &errs),
		FlushTimeout:        durationVar("FLUSH_TIMEOUT", 10*time.Second, &errs),
		RevokeFlushTimeout:  durationVar("REVOKE_FLUSH_TIMEOUT", 10*time.Second, &errs),
		RetryBaseDelay:      durationVar("RETRY_BASE_DELAY", 100*time.Millisecond, &errs),
		RetryMaxDelay:       durationVar("RETRY_MAX_DELAY", 10*time.Second, &errs),
		StartupTimeout:      durationVar("STARTUP_TIMEOUT", 60*time.Second, &errs),
		DrainTimeout:        durationVar("DRAIN_TIMEOUT", 15*time.Second, &errs),
		HTTPShutdownTimeout: durationVar("HTTP_SHUTDOWN_TIMEOUT", 5*time.Second, &errs),
		Postgres:            postgresFromEnv(&errs),
	}
	errs = append(errs, cfg.validate()...)
	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return cfg, nil
}

// Migrate is the subset of the configuration the migrate subcommand needs.
type Migrate struct {
	LogLevel       slog.Level
	Postgres       Postgres
	StartupTimeout time.Duration
	RetryBaseDelay time.Duration
	RetryMaxDelay  time.Duration
}

// LoadMigrate reads and validates only what `consumer migrate` uses: the DATABASE_*
// settings (the owner credentials in Compose), the log level and the startup
// retry budget. An unrelated bad variable (Kafka, flush settings) cannot block a
// schema migration.
func LoadMigrate() (Migrate, error) {
	var errs []error
	m := Migrate{
		LogLevel:       logLevelVar("LOG_LEVEL", slog.LevelInfo, &errs),
		Postgres:       postgresFromEnv(&errs),
		StartupTimeout: durationVar("STARTUP_TIMEOUT", 60*time.Second, &errs),
		RetryBaseDelay: durationVar("RETRY_BASE_DELAY", 100*time.Millisecond, &errs),
		RetryMaxDelay:  durationVar("RETRY_MAX_DELAY", 10*time.Second, &errs),
	}
	errs = append(errs, m.Postgres.validate()...)
	for _, d := range []struct {
		name string
		v    time.Duration
	}{{"STARTUP_TIMEOUT", m.StartupTimeout}, {"RETRY_BASE_DELAY", m.RetryBaseDelay}, {"RETRY_MAX_DELAY", m.RetryMaxDelay}} {
		if d.v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be > 0", d.name))
		}
	}
	if m.RetryBaseDelay > m.RetryMaxDelay {
		errs = append(errs, errors.New("RETRY_BASE_DELAY must be <= RETRY_MAX_DELAY"))
	}
	if err := errors.Join(errs...); err != nil {
		return Migrate{}, fmt.Errorf("invalid configuration: %w", err)
	}
	return m, nil
}

func postgresFromEnv(errs *[]error) Postgres {
	return Postgres{
		Host:             stringVar("DATABASE_HOST", "postgres"),
		Port:             intVar("DATABASE_PORT", 5432, errs),
		Name:             stringVar("DATABASE_NAME", "analytics"),
		User:             stringVar("DATABASE_USER", "analytics_app"),
		Password:         secretFileVar("DATABASE_PASSWORD_FILE", "/run/secrets/analytics_app_password", errs),
		SSLMode:          stringVar("DATABASE_SSLMODE", "disable"),
		MaxConns:         int32Var("DATABASE_MAX_CONNS", 8, errs),
		MinConns:         int32Var("DATABASE_MIN_CONNS", 1, errs),
		StatementTimeout: durationVar("DATABASE_STATEMENT_TIMEOUT", 5*time.Second, errs),
	}
}

// LoadHTTPAddr reads only HTTP_ADDR. The healthcheck subcommand uses it so that a
// probe never depends on secrets or unrelated settings.
func LoadHTTPAddr() (string, error) {
	var errs []error
	addr := addrVar("HTTP_ADDR", DefaultHTTPAddr, &errs)
	if err := errors.Join(errs...); err != nil {
		return "", fmt.Errorf("invalid configuration: %w", err)
	}
	return addr, nil
}

func (c Config) validate() []error {
	var errs []error
	positive := func(name string, v int64) {
		if v <= 0 {
			errs = append(errs, fmt.Errorf("%s must be > 0", name))
		}
	}
	positive("CONSUMER_WORKERS", int64(c.ConsumerWorkers))
	positive("FLUSH_INTERVAL", int64(c.FlushInterval))
	positive("FLUSH_MAX_RECORDS", int64(c.FlushMaxRecords))
	positive("FLUSH_TIMEOUT", int64(c.FlushTimeout))
	positive("REVOKE_FLUSH_TIMEOUT", int64(c.RevokeFlushTimeout))
	positive("RETRY_BASE_DELAY", int64(c.RetryBaseDelay))
	positive("RETRY_MAX_DELAY", int64(c.RetryMaxDelay))
	positive("STARTUP_TIMEOUT", int64(c.StartupTimeout))
	positive("DRAIN_TIMEOUT", int64(c.DrainTimeout))
	positive("HTTP_SHUTDOWN_TIMEOUT", int64(c.HTTPShutdownTimeout))

	if c.RetryBaseDelay > c.RetryMaxDelay {
		errs = append(errs, errors.New("RETRY_BASE_DELAY must be <= RETRY_MAX_DELAY"))
	}
	if c.FlushTimeout < c.Postgres.StatementTimeout {
		// A slow statement must hit the server-side timeout first (a clean
		// SQLSTATE 57014); FLUSH_TIMEOUT bounds a server that does not answer
		// at all (ADR 0011).
		errs = append(errs, errors.New("FLUSH_TIMEOUT must be >= DATABASE_STATEMENT_TIMEOUT"))
	}
	if c.Kafka.SessionTimeout < MinSessionTimeout || c.Kafka.SessionTimeout > MaxSessionTimeout {
		errs = append(errs, fmt.Errorf("CONSUMER_SESSION_TIMEOUT must be within %s..%s (broker group.min/max.session.timeout.ms)",
			MinSessionTimeout, MaxSessionTimeout))
	}
	if err := validInstanceID(c.Kafka.InstanceID); err != nil {
		errs = append(errs, err)
	}
	if c.RevokeFlushTimeout > c.DrainTimeout {
		errs = append(errs, errors.New("REVOKE_FLUSH_TIMEOUT must be <= DRAIN_TIMEOUT"))
	}
	if c.DrainTimeout+c.HTTPShutdownTimeout > MaxShutdownBudget {
		errs = append(errs, fmt.Errorf("DRAIN_TIMEOUT + HTTP_SHUTDOWN_TIMEOUT must be <= %s", MaxShutdownBudget))
	}
	for _, f := range []struct{ name, value string }{
		{"KAFKA_TOPIC", c.Kafka.Topic},
		{"KAFKA_DLQ_TOPIC", c.Kafka.DLQTopic},
		{"CONSUMER_GROUP", c.Kafka.Group},
	} {
		if strings.TrimSpace(f.value) == "" {
			errs = append(errs, fmt.Errorf("%s must not be blank", f.name))
		}
	}
	if c.Kafka.Topic != "" && c.Kafka.Topic == c.Kafka.DLQTopic {
		errs = append(errs, errors.New("KAFKA_DLQ_TOPIC must differ from KAFKA_TOPIC"))
	}
	for _, b := range c.Kafka.Brokers {
		if _, port, err := net.SplitHostPort(b); err != nil || !validPort(port, false) {
			errs = append(errs, fmt.Errorf("KAFKA_BROKERS: %q is not host:port", b))
		}
	}
	return append(errs, c.Postgres.validate()...)
}

func (p Postgres) validate() []error {
	var errs []error
	for _, f := range []struct{ name, value string }{
		{"DATABASE_HOST", p.Host},
		{"DATABASE_NAME", p.Name},
		{"DATABASE_USER", p.User},
	} {
		if strings.TrimSpace(f.value) == "" {
			errs = append(errs, fmt.Errorf("%s must not be blank", f.name))
		}
	}
	if p.Port < 1 || p.Port > 65535 {
		errs = append(errs, fmt.Errorf("DATABASE_PORT: %d is out of range 1..65535", p.Port))
	}
	if p.MaxConns <= 0 {
		errs = append(errs, errors.New("DATABASE_MAX_CONNS must be > 0"))
	}
	if p.MinConns < 0 {
		errs = append(errs, errors.New("DATABASE_MIN_CONNS must be >= 0"))
	}
	if p.MinConns > p.MaxConns {
		errs = append(errs, errors.New("DATABASE_MIN_CONNS must be <= DATABASE_MAX_CONNS"))
	}
	if p.StatementTimeout <= 0 {
		errs = append(errs, errors.New("DATABASE_STATEMENT_TIMEOUT must be > 0"))
	}
	if !slices.Contains(sslModes, p.SSLMode) {
		errs = append(errs, fmt.Errorf("DATABASE_SSLMODE: %q is not one of %s", p.SSLMode, strings.Join(sslModes, ", ")))
	}
	return errs
}

func lookup(key string) (string, bool) {
	raw, ok := os.LookupEnv(key)
	if !ok || raw == "" {
		return "", false
	}
	return raw, true
}

func stringVar(key, def string) string {
	if raw, ok := lookup(key); ok {
		return raw
	}
	return def
}

func intVar(key string, def int, errs *[]error) int {
	raw, ok := lookup(key)
	if !ok {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not an integer", key, raw))
		return def
	}
	return v
}

func int32Var(key string, def int32, errs *[]error) int32 {
	raw, ok := lookup(key)
	if !ok {
		return def
	}
	v, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a 32-bit integer", key, raw))
		return def
	}
	return int32(v)
}

func durationVar(key string, def time.Duration, errs *[]error) time.Duration {
	raw, ok := lookup(key)
	if !ok {
		return def
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a duration (e.g. 500ms, 10s)", key, raw))
		return def
	}
	return v
}

func listVar(key string, def []string, errs *[]error) []string {
	raw, ok := lookup(key)
	if !ok {
		return def
	}
	var out []string
	for item := range strings.SplitSeq(raw, ",") {
		if item = strings.TrimSpace(item); item != "" {
			out = append(out, item)
		}
	}
	if len(out) == 0 {
		*errs = append(*errs, fmt.Errorf("%s: no entries in %q", key, raw))
		return def
	}
	return out
}

func logLevelVar(key string, def slog.Level, errs *[]error) slog.Level {
	raw, ok := lookup(key)
	if !ok {
		return def
	}
	switch strings.ToLower(raw) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	}
	*errs = append(*errs, fmt.Errorf("%s: %q is not one of debug, info, warn, error", key, raw))
	return def
}

// addrVar reads a listen address ("host:port" or ":port"; port 0 is allowed).
func addrVar(key, def string, errs *[]error) string {
	v := stringVar(key, def)
	if _, port, err := net.SplitHostPort(v); err != nil || !validPort(port, true) {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a listen address like :8081", key, v))
		return def
	}
	return v
}

// secretFileVar reads a secret from the file named by key (the *_FILE convention,
// ADR 0002). One trailing newline is trimmed. The file content never appears in
// errors.
func secretFileVar(key, defPath string, errs *[]error) Secret {
	path := stringVar(key, defPath)
	b, err := os.ReadFile(path) //nolint:gosec // G304: the path comes from the operator-controlled *_FILE variable (ADR 0002)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: read secret file: %w", key, err))
		return ""
	}
	s := strings.TrimSuffix(strings.TrimSuffix(string(b), "\n"), "\r")
	if s == "" {
		*errs = append(*errs, fmt.Errorf("%s: secret file %q is empty", key, path))
		return ""
	}
	return Secret(s)
}

// instanceIDVar reads CONSUMER_INSTANCE_ID, defaulting to the hostname. In
// Compose every replica, including each one of --scale, has its own hostname,
// and a restarted container keeps it.
func instanceIDVar(key string, errs *[]error) string {
	if raw, ok := lookup(key); ok {
		return raw
	}
	h, err := os.Hostname()
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: unset and the hostname is unavailable: %w", key, err))
		return ""
	}
	return h
}

// validInstanceID applies Kafka's rules for group.instance.id (characters
// [a-zA-Z0-9._-], not "." or "..") and leaves room for the member suffix.
func validInstanceID(id string) error {
	if id == "" || len(id) > maxInstanceIDLen || id == "." || id == ".." {
		return fmt.Errorf("CONSUMER_INSTANCE_ID: %q must have 1..%d characters and must not be \".\" or \"..\"", id, maxInstanceIDLen)
	}
	for _, c := range id {
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return fmt.Errorf("CONSUMER_INSTANCE_ID: %q may contain only a-z, A-Z, 0-9, '.', '_' and '-'", id)
		}
	}
	return nil
}

func validPort(port string, allowZero bool) bool {
	n, err := strconv.Atoi(port)
	if err != nil {
		return false
	}
	if allowZero {
		return n >= 0 && n <= 65535
	}
	return n >= 1 && n <= 65535
}
