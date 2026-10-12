// Package kafka is the producer's outbound adapter: it implements
// ports.ActivityPublisher on top of franz-go and owns the wire DTO.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/plugin/kprom"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

// defaultLinger lets small batches form without noticeably adding latency.
const defaultLinger = 5 * time.Millisecond

// MinPublishTimeout is the smallest PublishTimeout franz-go accepts for
// RecordDeliveryTimeout.
const MinPublishTimeout = time.Second

// Config configures a Publisher.
type Config struct {
	Brokers []string
	Topic   string
	// PublishTimeout bounds how long a record may wait for its broker ack
	// (franz-go RecordDeliveryTimeout). The caller's context bounds it too.
	PublishTimeout time.Duration
	// Linger is how long a partition waits for more records before sending.
	// Zero selects a few milliseconds.
	Linger time.Duration
}

func (c Config) validate() error {
	var errs []error
	if len(c.Brokers) == 0 {
		errs = append(errs, errors.New("at least one broker is required"))
	}
	if c.Topic == "" {
		errs = append(errs, errors.New("topic is required"))
	}
	if c.PublishTimeout < MinPublishTimeout {
		errs = append(errs, fmt.Errorf("publish timeout must be at least %s", MinPublishTimeout))
	}
	return errors.Join(errs...)
}

// Publisher publishes ActivityEvents to Kafka with acks=all and the
// idempotent producer (both franz-go defaults, asserted in tests). The record
// key is the user_id so one user's events stay ordered on one partition.
type Publisher struct {
	client *kgo.Client
	topic  string
}

var _ ports.ActivityPublisher = (*Publisher)(nil)

// NewPublisher creates the Kafka client. It does not contact the brokers; use
// Ping or EnsureTopic for that. When reg is not nil the client's kprom metrics
// are registered on it. The adapter does not log: the HTTP edge logs each
// failure once, with its request ID.
func NewPublisher(cfg Config, reg prometheus.Registerer) (*Publisher, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("kafka publisher config: %w", err)
	}
	linger := cfg.Linger
	if linger == 0 {
		linger = defaultLinger
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(linger),
		kgo.RecordDeliveryTimeout(cfg.PublishTimeout),
		// Without this, a record already sent to a broker that then dies
		// cannot be failed: ProduceSync would ignore the context and block
		// until the broker returns, and the HTTP edge could never answer 503.
		// Idempotency stays on; only a cancelled record that the client
		// retries may be written twice, which at-least-once delivery
		// already allows (ADR 0000, ADR 0011).
		kgo.AllowIdempotentProduceCancellation(),
		kgo.ClientID("moveo-producer"),
	}
	if reg != nil {
		opts = append(opts, kgo.WithHooks(kprom.NewMetrics("kafka", kprom.Registerer(reg))))
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &Publisher{client: client, topic: cfg.Topic}, nil
}

// Publish encodes ev and produces it synchronously, returning after the
// broker acknowledged it. Failures that a retry may fix (broker unreachable,
// timeout, cancelled context, retriable broker errors) wrap
// ports.ErrPublishUnavailable. Publish never retries itself.
func (p *Publisher) Publish(ctx context.Context, eventID uuid.UUID, ev domain.ActivityEvent) error {
	value, err := encodeActivity(ev)
	if err != nil {
		return err
	}
	rec := &kgo.Record{
		Topic:   p.topic,
		Key:     []byte(ev.UserID),
		Value:   value,
		Headers: recordHeaders(ctx, eventID),
	}
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return classify(err)
	}
	return nil
}

// classify marks every produce error as unavailable except broker errors that
// a retry cannot fix (for example a record the broker rejects as too large).
func classify(err error) error {
	var ke *kerr.Error
	if errors.As(err, &ke) && !ke.Retriable {
		return fmt.Errorf("produce record: %w", err)
	}
	return fmt.Errorf("%w: %w", ports.ErrPublishUnavailable, err)
}

// Ping checks that a broker is reachable. It backs /readyz.
func (p *Publisher) Ping(ctx context.Context) error {
	if err := p.client.Ping(ctx); err != nil {
		return fmt.Errorf("ping kafka: %w", err)
	}
	return nil
}

// EnsureTopic checks that the configured topic exists. It never creates it:
// topics are created by deploy/kafka/create-topics.sh (ADR 0007).
func (p *Publisher) EnsureTopic(ctx context.Context) error {
	req := kmsg.NewPtrMetadataRequest()
	topic := kmsg.NewMetadataRequestTopic()
	topic.Topic = kmsg.StringPtr(p.topic)
	req.Topics = append(req.Topics, topic)
	req.AllowAutoTopicCreation = false

	resp, err := req.RequestWith(ctx, p.client)
	if err != nil {
		return fmt.Errorf("fetch metadata for topic %q: %w", p.topic, err)
	}
	if len(resp.Topics) == 0 {
		return fmt.Errorf("topic %q: not in metadata response", p.topic)
	}
	for _, t := range resp.Topics {
		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			return fmt.Errorf("topic %q: %w", p.topic, err)
		}
	}
	return nil
}

// Flush waits until every buffered record is delivered or ctx ends
// (shutdown stage 3).
func (p *Publisher) Flush(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("flush kafka client: %w", err)
	}
	return nil
}

// Close releases the client's connections. Call Flush first (stage 4).
func (p *Publisher) Close() { p.client.Close() }
