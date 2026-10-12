package kafka

import (
	"fmt"
	"reflect"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/plugin/kprom"
)

// Metric label values (ADR 0013). Labels never carry user IDs, event IDs,
// offsets or error strings.
const (
	resultOK  = "ok"
	resultDLQ = "dlq"

	dependencyPostgres = "postgres"
	dependencyKafka    = "kafka"

	errorClassTransient     = "transient"
	errorClassDataException = "data_exception"
	errorClassUnknown       = "unknown"
)

// consumerMetrics are the pipeline metrics recorded by the partition workers.
type consumerMetrics struct {
	consumed       *prometheus.CounterVec // records_consumed_total{result}
	flushDuration  prometheus.Histogram   // aggregate_flush_duration_seconds
	retries        *prometheus.CounterVec // retries_total{dependency,error_class}
	commitFailures prometheus.Counter     // offset_commit_failures_total
	fenced         prometheus.Counter     // offset_fence_rejections_total
	resyncs        prometheus.Counter     // offset_fence_resyncs_total
	lag            *prometheus.GaugeVec   // consumer_lag_records{partition}
}

func newConsumerMetrics(reg prometheus.Registerer) (*consumerMetrics, error) {
	m := &consumerMetrics{
		consumed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "records_consumed_total",
			Help: "Records whose batch was flushed, by outcome (ok = aggregated, dlq = dead-lettered).",
		}, []string{"result"}),
		flushDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "aggregate_flush_duration_seconds",
			Help:    "Duration of one flush attempt (DLQ publish plus aggregate upsert transaction).",
			Buckets: prometheus.DefBuckets,
		}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "retries_total",
			Help: "Retried flush attempts, by failing dependency and error class.",
		}, []string{"dependency", "error_class"}),
		commitFailures: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "offset_commit_failures_total",
			Help: "Kafka offset commits that failed after the batch was already persisted. Informational: the position stored in PostgreSQL is the source of truth, so the batch is not applied again.",
		}),
		fenced: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "offset_fence_rejections_total",
			Help: "Batches rolled back because another owner had moved the partition's stored position; the partition worker stopped.",
		}),
		resyncs: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "offset_fence_resyncs_total",
			Help: "Fenced partitions resumed in place from the reloaded stored position, without waiting for a rebalance.",
		}),
		lag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "consumer_lag_records",
			Help: "Records between the group's committed offset and the end of each partition.",
		}, []string{"partition"}),
	}
	for _, c := range []prometheus.Collector{m.consumed, m.flushDuration, m.retries, m.commitFailures, m.fenced, m.resyncs, m.lag} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register consumer metrics: %w", err)
		}
	}
	// Pre-create the fixed label values so the series exist at zero.
	m.consumed.WithLabelValues(resultOK)
	m.consumed.WithLabelValues(resultDLQ)
	for _, dep := range []string{dependencyPostgres, dependencyKafka} {
		for _, class := range []string{errorClassTransient, errorClassDataException, errorClassUnknown} {
			m.retries.WithLabelValues(dep, class)
		}
	}
	return m, nil
}

// newClientMetrics returns franz-go client metrics on reg (ADR 0013). Every
// client of the process registers the same metric names, so the client_id label
// (set from kgo.ClientID) keeps their series apart.
func newClientMetrics(reg prometheus.Registerer) *kprom.Metrics {
	return kprom.NewMetrics("kafka", kprom.Registerer(reg), kprom.WithClientLabel())
}

// errorType names the innermost error's Go type. Logs carry it instead of the
// error text, which may echo payload data (ADR 0011, 0002). For a joined or
// multi-%w error it follows the last one: adapters wrap as
// fmt.Errorf("%w: %w", ports.ErrUnavailable, driverErr).
func errorType(err error) string {
	if err == nil {
		return ""
	}
	for {
		switch u := err.(type) { //nolint:errorlint // walks the Unwrap chain by hand on purpose: errors.As cannot reach the innermost type
		case interface{ Unwrap() error }:
			next := u.Unwrap()
			if next == nil {
				return reflect.TypeOf(err).String()
			}
			err = next
		case interface{ Unwrap() []error }:
			errs := u.Unwrap()
			if len(errs) == 0 {
				return reflect.TypeOf(err).String()
			}
			err = errs[len(errs)-1]
		default:
			return reflect.TypeOf(err).String()
		}
	}
}
