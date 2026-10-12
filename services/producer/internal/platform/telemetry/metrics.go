package telemetry

import (
	"fmt"
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Label values of activities_published_total{result}.
const (
	ResultOK         = "ok"
	ResultInvalid    = "invalid"
	ResultKafkaError = "kafka_error"
)

// Label values of retries_total{dependency}.
const (
	DependencyKafka = "kafka"
)

// Metric names (ADR 0013).
const (
	MetricActivitiesPublished = "activities_published_total"
	MetricRetries             = "retries_total"
)

// NewRegistry returns a fresh registry with the Go runtime and process
// collectors installed.
func NewRegistry() (*prometheus.Registry, error) {
	reg := prometheus.NewRegistry()
	if err := reg.Register(collectors.NewGoCollector()); err != nil {
		return nil, fmt.Errorf("register go collector: %w", err)
	}
	if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
		return nil, fmt.Errorf("register process collector: %w", err)
	}
	return reg, nil
}

// Handler serves the registry in the Prometheus text format.
func Handler(g prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(g, promhttp.HandlerOpts{})
}

// Metrics holds the producer's typed collectors. Labels come from the fixed
// sets above, never from user input.
type Metrics struct {
	published *prometheus.CounterVec
	retries   *prometheus.CounterVec
}

// NewMetrics registers the producer's collectors on reg.
func NewMetrics(reg prometheus.Registerer) (*Metrics, error) {
	m := &Metrics{
		published: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: MetricActivitiesPublished,
			Help: "Activity publish requests by outcome.",
		}, []string{"result"}),
		retries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: MetricRetries,
			Help: "Retries of a dependency call by dependency.",
		}, []string{"dependency"}),
	}
	for _, r := range []string{ResultOK, ResultInvalid, ResultKafkaError} {
		m.published.WithLabelValues(r)
	}
	m.retries.WithLabelValues(DependencyKafka)
	for _, c := range []prometheus.Collector{m.published, m.retries} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register metrics: %w", err)
		}
	}
	return m, nil
}

// ActivityPublished counts one publish request with the given result.
func (m *Metrics) ActivityPublished(result string) {
	m.published.WithLabelValues(result).Inc()
}

// Retry counts one retry of a call to the given dependency.
func (m *Metrics) Retry(dependency string) {
	m.retries.WithLabelValues(dependency).Inc()
}
