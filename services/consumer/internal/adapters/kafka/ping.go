package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Ping checks that the brokers answer and that both cfg.Topic and cfg.DLQTopic
// exist (ADR 0007: applications never create topics). It makes one attempt;
// callers retry it at startup with retry.Do under STARTUP_TIMEOUT (ADR 0011).
func Ping(ctx context.Context, cfg Config) error {
	if err := cfg.validateClient(); err != nil {
		return fmt.Errorf("kafka ping config: %w", err)
	}
	cl, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...), kgo.ClientID(clientIDPrefix(cfg)+"-ping"))
	if err != nil {
		return fmt.Errorf("kafka ping: create client: %w", err)
	}
	defer cl.Close()

	topics, err := kadm.NewClient(cl).ListTopics(ctx, cfg.Topic, cfg.DLQTopic)
	if err != nil {
		return fmt.Errorf("kafka ping: list topics: %w", err)
	}
	var errs []error
	for _, t := range []string{cfg.Topic, cfg.DLQTopic} {
		d, ok := topics[t]
		switch {
		case !ok:
			errs = append(errs, fmt.Errorf("topic %q not found", t))
		case d.Err != nil:
			errs = append(errs, fmt.Errorf("topic %q: %w", t, d.Err))
		}
	}
	if err := errors.Join(errs...); err != nil {
		return fmt.Errorf("kafka ping: %w", err)
	}
	return nil
}
