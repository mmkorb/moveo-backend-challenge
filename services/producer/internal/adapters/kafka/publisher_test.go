//go:build !integration

package kafka

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.uber.org/goleak"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func newTestPublisher(t *testing.T, cfg Config) *Publisher {
	t.Helper()
	p, err := NewPublisher(cfg, nil)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	t.Cleanup(p.Close)
	return p
}

// unreachableConfig points at a port nothing listens on.
func unreachableConfig() Config {
	return Config{Brokers: []string{"127.0.0.1:1"}, Topic: "incoming.user_activity", PublishTimeout: MinPublishTimeout}
}

func TestNewPublisherKeepsDurabilityDefaults(t *testing.T) {
	t.Parallel()
	p := newTestPublisher(t, unreachableConfig())

	if got := p.client.OptValue(kgo.RequiredAcks); got != kgo.AllISRAcks() {
		t.Errorf("RequiredAcks = %v, want acks=all", got)
	}
	if got := p.client.OptValue(kgo.DisableIdempotentWrite); got != false {
		t.Errorf("DisableIdempotentWrite = %v, want false (idempotent producer on)", got)
	}
	if got := p.client.OptValue(kgo.AllowIdempotentProduceCancellation); got != true {
		t.Errorf("AllowIdempotentProduceCancellation = %v, want true so a publish cannot outlive its context", got)
	}
	if got := p.client.OptValue(kgo.DefaultProduceTopic); got != "incoming.user_activity" {
		t.Errorf("DefaultProduceTopic = %v", got)
	}
	if got := p.client.OptValue(kgo.RecordDeliveryTimeout); got != MinPublishTimeout {
		t.Errorf("RecordDeliveryTimeout = %v, want the publish timeout", got)
	}
	if got := p.client.OptValue(kgo.ProducerLinger); got != defaultLinger {
		t.Errorf("ProducerLinger = %v, want %v", got, defaultLinger)
	}
}

func TestNewPublisherValidatesConfig(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cfg  Config
	}{
		{"should reject no brokers", Config{Topic: "t", PublishTimeout: time.Second}},
		{"should reject no topic", Config{Brokers: []string{"b:1"}, PublishTimeout: time.Second}},
		{"should reject a zero publish timeout", Config{Brokers: []string{"b:1"}, Topic: "t"}},
		{"should reject a publish timeout below the franz-go minimum", Config{Brokers: []string{"b:1"}, Topic: "t", PublishTimeout: 500 * time.Millisecond}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if p, err := NewPublisher(tt.cfg, nil); err == nil {
				p.Close()
				t.Error("NewPublisher succeeded, want an error")
			}
		})
	}
}

func TestNewPublisherRegistersClientMetrics(t *testing.T) {
	t.Parallel()
	reg := prometheus.NewRegistry()
	p, err := NewPublisher(unreachableConfig(), reg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	_ = p.Ping(ctx) // fails, but makes the client dial and the hooks count

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, f := range families {
		if strings.HasPrefix(f.GetName(), "kafka_") {
			found = true
		}
	}
	if !found {
		t.Error("no kafka_* client metrics on the injected registry")
	}
}

func TestPublishMapsUnreachableBrokerToUnavailable(t *testing.T) {
	t.Parallel()
	p := newTestPublisher(t, unreachableConfig())
	ev, err := domain.NewActivityEvent("u1", domain.ActivityPageView, time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	err = p.Publish(ctx, uuid.NewV7(), ev)

	if !errors.Is(err, ports.ErrPublishUnavailable) {
		t.Fatalf("err = %v, want ports.ErrPublishUnavailable", err)
	}
}

func TestPublishHonorsACancelledContext(t *testing.T) {
	t.Parallel()
	p := newTestPublisher(t, unreachableConfig())
	ev, err := domain.NewActivityEvent("u1", domain.ActivityPageView, time.Now(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = p.Publish(ctx, uuid.NewV7(), ev)

	if !errors.Is(err, ports.ErrPublishUnavailable) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want ErrPublishUnavailable wrapping context.Canceled", err)
	}
}

func TestPingAndEnsureTopicFailWhenBrokerIsDown(t *testing.T) {
	t.Parallel()
	p := newTestPublisher(t, unreachableConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	if err := p.Ping(ctx); err == nil {
		t.Error("Ping succeeded against an unreachable broker")
	}
	if err := p.EnsureTopic(ctx); err == nil {
		t.Error("EnsureTopic succeeded against an unreachable broker")
	}
}

func TestClassify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		err             error
		wantUnavailable bool
	}{
		{"should treat a context deadline as unavailable", context.DeadlineExceeded, true},
		{"should treat a record timeout as unavailable", kgo.ErrRecordTimeout, true},
		{"should treat a closed client as unavailable", kgo.ErrClientClosed, true},
		{"should treat a retriable broker error as unavailable", kerr.NotLeaderForPartition, true},
		{"should not treat a non-retriable broker error as unavailable", kerr.MessageTooLarge, false},
		{"should not treat a non-retriable broker error as unavailable when wrapped",
			errors.Join(errors.New("ctx"), kerr.InvalidTopicException), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := classify(tt.err)
			if is := errors.Is(got, ports.ErrPublishUnavailable); is != tt.wantUnavailable {
				t.Errorf("errors.Is(ErrPublishUnavailable) = %v, want %v (err: %v)", is, tt.wantUnavailable, got)
			}
			if !errors.Is(got, tt.err) {
				t.Errorf("classify dropped the cause: %v", got)
			}
		})
	}
}
