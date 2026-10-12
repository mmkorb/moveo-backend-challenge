package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

const (
	// dlqLinger lets the publisher pack the dead letters of one flush together.
	dlqLinger = 5 * time.Millisecond
	// dlqDeliveryTimeout fails a publish while the broker is unreachable, so the
	// worker's retry loop (not the client) decides what happens next.
	dlqDeliveryTimeout = 10 * time.Second
	// dlqCloseTimeout bounds the Flush that runs before Close.
	dlqCloseTimeout = 5 * time.Second
)

// errDeadLetterPublish marks DLQ publish failures. The partition worker uses it
// to label retries with dependency="kafka" (ADR 0013).
var errDeadLetterPublish = errors.New("dead letter publish failed")

// DeadLetterPublisher parks permanently failing records on the DLQ topic
// (ADR 0011). It implements ports.DeadLetterPublisher and never retries itself.
type DeadLetterPublisher struct {
	cl     *kgo.Client
	topic  string
	logger *slog.Logger
	total  *prometheus.CounterVec // dlq_records_total{error_class}
	now    func() time.Time
}

var _ ports.DeadLetterPublisher = (*DeadLetterPublisher)(nil)

// NewDeadLetterPublisher returns an idempotent, acks=all producer for
// cfg.DLQTopic (franz-go defaults, ADR 0007). It registers dlq_records_total and
// the client metrics on reg. Close releases it.
func NewDeadLetterPublisher(cfg Config, logger *slog.Logger, reg prometheus.Registerer) (*DeadLetterPublisher, error) {
	if err := cfg.validateClient(); err != nil {
		return nil, fmt.Errorf("dlq publisher config: %w", err)
	}
	if logger == nil || reg == nil {
		return nil, errors.New("dlq publisher: nil logger or registerer")
	}
	total := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "dlq_records_total",
		Help: "Records published to the dead-letter topic, by error class.",
	}, []string{"error_class"})
	if err := reg.Register(total); err != nil {
		return nil, fmt.Errorf("register dlq metrics: %w", err)
	}
	total.WithLabelValues(string(ports.ErrorClassDecode))
	total.WithLabelValues(string(ports.ErrorClassValidation))

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ClientID(clientIDPrefix(cfg)+"-dlq"),
		kgo.ProducerLinger(dlqLinger),
		kgo.RecordDeliveryTimeout(dlqDeliveryTimeout),
		// Without it, an idempotent record already in flight ignores context
		// cancellation and RecordDeliveryTimeout, so a flush attempt would
		// outlive FLUSH_TIMEOUT and block shutdown and revokes when the broker
		// dies. A duplicate dead letter is already accepted (ADR 0007, 0011).
		kgo.AllowIdempotentProduceCancellation(),
		kgo.WithHooks(newClientMetrics(reg)),
	)
	if err != nil {
		reg.Unregister(total)
		return nil, fmt.Errorf("create dlq client: %w", err)
	}
	return &DeadLetterPublisher{
		cl:     cl,
		topic:  cfg.DLQTopic,
		logger: logger.With("component", "dlq-publisher"),
		total:  total,
		now:    time.Now,
	}, nil
}

// Publish writes every letter to the DLQ topic and returns once the broker has
// acknowledged all of them. Any failure is transient: it wraps
// ports.ErrUnavailable, and the caller retries the whole flush (ADR 0011).
func (p *DeadLetterPublisher) Publish(ctx context.Context, letters []ports.DeadLetter) error {
	if len(letters) == 0 {
		return nil
	}
	failedAt := p.now()
	records := make([]*kgo.Record, len(letters))
	for i := range letters {
		records[i] = deadLetterRecord(p.topic, &letters[i], failedAt)
	}
	if err := p.cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("%w: %w: %w", ports.ErrUnavailable, errDeadLetterPublish, err)
	}
	for i := range letters {
		l := &letters[i]
		p.total.WithLabelValues(string(l.Class)).Inc()
		attrs := append([]slog.Attr{
			slog.String("topic", l.Topic),
			slog.Int("partition", int(l.Partition)),
			slog.Int64("offset", l.Offset),
			slog.String("error_class", string(l.Class)),
			slog.String("event_id", loggableEventID(l.Headers)),
		}, traceAttrs(l.Headers)...)
		p.logger.LogAttrs(ctx, slog.LevelWarn, "record dead-lettered", attrs...)
	}
	return nil
}

// Close flushes anything still buffered (bounded) and releases the client.
func (p *DeadLetterPublisher) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), dlqCloseTimeout)
	defer cancel()
	if err := p.cl.Flush(ctx); err != nil {
		p.logger.Warn("dlq flush on close failed", "error_type", errorType(err))
	}
	p.cl.Close()
}

// deadLetterRecord builds the DLQ record: original key and value bytes, the
// original headers, and exactly the x-* headers of ADR 0011 (original headers
// that reuse those names are dropped so the added ones are unambiguous). No
// error text is ever added.
func deadLetterRecord(topic string, l *ports.DeadLetter, failedAt time.Time) *kgo.Record {
	headers := make([]kgo.RecordHeader, 0, len(l.Headers)+5)
	for _, h := range l.Headers {
		switch h.Key {
		case headerErrorClass, headerOriginalTopic, headerOriginalPartition, headerOriginalOffset, headerFailedAt:
			continue
		}
		headers = append(headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	headers = append(headers,
		kgo.RecordHeader{Key: headerErrorClass, Value: []byte(l.Class)},
		kgo.RecordHeader{Key: headerOriginalTopic, Value: []byte(l.Topic)},
		kgo.RecordHeader{Key: headerOriginalPartition, Value: strconv.AppendInt(nil, int64(l.Partition), 10)},
		kgo.RecordHeader{Key: headerOriginalOffset, Value: strconv.AppendInt(nil, l.Offset, 10)},
		kgo.RecordHeader{Key: headerFailedAt, Value: []byte(failedAt.UTC().Format(time.RFC3339))},
	)
	return &kgo.Record{Topic: topic, Key: l.Key, Value: l.Value, Headers: headers}
}

// clientIDPrefix names this process's clients; kprom labels series by it.
func clientIDPrefix(cfg Config) string {
	if cfg.Group != "" {
		return cfg.Group
	}
	return "consumer"
}
