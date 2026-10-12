// Package config loads and validates the producer's configuration from the
// environment. It is read once, in main, and passed down explicitly.
package config

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Defaults for local Compose (ADR 0003 environment table, ADR 0012 producer
// shutdown stages).
const (
	DefaultHTTPAddr            = ":8080"
	DefaultKafkaBroker         = "kafka:9092"
	DefaultKafkaTopic          = "incoming.user_activity"
	DefaultPublishTimeout      = 5 * time.Second
	DefaultStartupTimeout      = 60 * time.Second
	DefaultHTTPShutdownTimeout = 12 * time.Second
	DefaultDrainTimeout        = 5 * time.Second

	// MinPublishTimeout is the smallest PUBLISH_TIMEOUT the Kafka client
	// accepts as its record delivery timeout.
	MinPublishTimeout = time.Second

	// MaxShutdownBudget is the total time the shutdown stages may take; it
	// leaves 10 s of margin before Compose's 30 s stop_grace_period ends.
	MaxShutdownBudget = 20 * time.Second
)

// Config is the validated producer configuration.
type Config struct {
	LogLevel slog.Level
	// HTTPAddr is the listen address of the API, health and metrics server.
	HTTPAddr     string
	KafkaBrokers []string
	KafkaTopic   string
	// PublishTimeout bounds one publish, from the request to the broker ack.
	PublishTimeout time.Duration
	// StartupTimeout bounds the startup retry of the Kafka topic check.
	StartupTimeout time.Duration
	// HTTPShutdownTimeout bounds srv.Shutdown: in-flight requests finish
	// (shutdown stage 2). It must exceed PublishTimeout.
	HTTPShutdownTimeout time.Duration
	// DrainTimeout bounds the final Kafka client Flush (shutdown stage 3).
	DrainTimeout time.Duration
}

// Load reads the environment, applies defaults and validates everything,
// returning all problems at once.
func Load() (Config, error) {
	var errs []error
	cfg := Config{
		LogLevel:            levelVar("LOG_LEVEL", slog.LevelInfo, &errs),
		HTTPAddr:            stringVar("HTTP_ADDR", DefaultHTTPAddr),
		KafkaBrokers:        listVar("KAFKA_BROKERS", []string{DefaultKafkaBroker}),
		KafkaTopic:          stringVar("KAFKA_TOPIC", DefaultKafkaTopic),
		PublishTimeout:      durationVar("PUBLISH_TIMEOUT", DefaultPublishTimeout, &errs),
		StartupTimeout:      durationVar("STARTUP_TIMEOUT", DefaultStartupTimeout, &errs),
		HTTPShutdownTimeout: durationVar("HTTP_SHUTDOWN_TIMEOUT", DefaultHTTPShutdownTimeout, &errs),
		DrainTimeout:        durationVar("DRAIN_TIMEOUT", DefaultDrainTimeout, &errs),
	}
	errs = append(errs, cfg.validate()...)
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("invalid configuration: %w", errors.Join(errs...))
	}
	return cfg, nil
}

func (c Config) validate() []error {
	var errs []error
	if _, err := ProbeURL(c.HTTPAddr); err != nil {
		errs = append(errs, err)
	}
	if len(c.KafkaBrokers) == 0 {
		errs = append(errs, errors.New("KAFKA_BROKERS: at least one broker is required"))
	}
	for _, b := range c.KafkaBrokers {
		if _, _, err := net.SplitHostPort(b); err != nil {
			errs = append(errs, fmt.Errorf("KAFKA_BROKERS: %q is not host:port", b))
		}
	}
	if c.KafkaTopic == "" {
		errs = append(errs, errors.New("KAFKA_TOPIC: must not be empty"))
	}
	for _, d := range []struct {
		name string
		val  time.Duration
	}{
		{"PUBLISH_TIMEOUT", c.PublishTimeout},
		{"STARTUP_TIMEOUT", c.StartupTimeout},
		{"HTTP_SHUTDOWN_TIMEOUT", c.HTTPShutdownTimeout},
		{"DRAIN_TIMEOUT", c.DrainTimeout},
	} {
		if d.val <= 0 {
			errs = append(errs, fmt.Errorf("%s: must be positive", d.name))
		}
	}
	if c.PublishTimeout > 0 && c.PublishTimeout < MinPublishTimeout {
		errs = append(errs, fmt.Errorf("PUBLISH_TIMEOUT: must be at least %s (Kafka client minimum)", MinPublishTimeout))
	}
	if c.HTTPShutdownTimeout <= c.PublishTimeout {
		errs = append(errs, fmt.Errorf("HTTP_SHUTDOWN_TIMEOUT (%s) must exceed PUBLISH_TIMEOUT (%s) so in-flight publishes can finish",
			c.HTTPShutdownTimeout, c.PublishTimeout))
	}
	if total := c.HTTPShutdownTimeout + c.DrainTimeout; total > MaxShutdownBudget {
		errs = append(errs, fmt.Errorf("HTTP_SHUTDOWN_TIMEOUT + DRAIN_TIMEOUT = %s exceeds the %s shutdown budget",
			total, MaxShutdownBudget))
	}
	return errs
}

// ProbeURL returns the loopback readiness URL for a listen address such as
// ":8080" or "0.0.0.0:8080". The healthcheck subcommand probes it.
func ProbeURL(httpAddr string) (string, error) {
	_, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return "", fmt.Errorf("HTTP_ADDR: %q is not [host]:port", httpAddr)
	}
	if n, err := strconv.Atoi(port); err != nil || n < 1 || n > 65535 {
		return "", fmt.Errorf("HTTP_ADDR: %q has an invalid port", httpAddr)
	}
	return "http://" + net.JoinHostPort("127.0.0.1", port) + "/readyz", nil
}

// HealthcheckURL resolves the readiness URL from HTTP_ADDR alone, so the
// healthcheck subcommand does not depend on the rest of the configuration.
func HealthcheckURL() (string, error) {
	return ProbeURL(stringVar("HTTP_ADDR", DefaultHTTPAddr))
}

func lookup(key string) (string, bool) {
	raw, ok := os.LookupEnv(key)
	raw = strings.TrimSpace(raw)
	return raw, ok && raw != ""
}

func stringVar(key, def string) string {
	if v, ok := lookup(key); ok {
		return v
	}
	return def
}

func listVar(key string, def []string) []string {
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
	return out
}

func durationVar(key string, def time.Duration, errs *[]error) time.Duration {
	raw, ok := lookup(key)
	if !ok {
		return def
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not a duration (e.g. 5s)", key, raw))
		return def
	}
	return d
}

func levelVar(key string, def slog.Level, errs *[]error) slog.Level {
	raw, ok := lookup(key)
	if !ok {
		return def
	}
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(raw)); err != nil {
		*errs = append(*errs, fmt.Errorf("%s: %q is not one of debug, info, warn, error", key, raw))
		return def
	}
	return lvl
}
