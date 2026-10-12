//go:build integration

package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"
	"uuid"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/platform/tracectx"
	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/ports"
)

// kafkaImage matches compose.yaml (ADR 0015, ADR 0001).
const kafkaImage = "apache/kafka:4.3.1"

const integrationTopic = "incoming.user_activity"

// startKafka runs ONE single-node KRaft broker with memory and CPU limits
// and returns its host address. The broker advertises a free loopback port
// that is published on the same host port, so the client can reconnect to
// the address the broker hands out.
func startKafka(ctx context.Context, t *testing.T) (broker string, ctr testcontainers.Container) {
	t.Helper()
	port := freePort(t)
	broker = net.JoinHostPort("127.0.0.1", strconv.Itoa(port))

	ctr, err := testcontainers.Run(ctx, kafkaImage,
		testcontainers.WithEnv(map[string]string{
			"CLUSTER_ID":                                     "MkU3OEVBNTcwNTJENDM2Qk",
			"KAFKA_NODE_ID":                                  "1",
			"KAFKA_PROCESS_ROLES":                            "broker,controller",
			"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@localhost:9093",
			"KAFKA_LISTENERS":                                "INTERNAL://:9092,EXTERNAL://:9094,CONTROLLER://:9093",
			"KAFKA_ADVERTISED_LISTENERS":                     "INTERNAL://localhost:9092,EXTERNAL://" + broker,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
			"KAFKA_INTER_BROKER_LISTENER_NAME":               "INTERNAL",
			"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
			"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
			"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
			"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
			"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS":         "0",
			"KAFKA_HEAP_OPTS":                                "-Xms384m -Xmx512m",
		}),
		testcontainers.WithExposedPorts("9094/tcp"),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = network.PortMap{
				network.MustParsePort("9094/tcp"): {{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(port)}},
			}
			hc.Memory = 1 << 30
			hc.NanoCPUs = 1_500_000_000
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute)),
	)
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	return broker, ctr
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func createTopic(ctx context.Context, t *testing.T, admin *kgo.Client, topic string, partitions int32) {
	t.Helper()
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = partitions
	rt.ReplicationFactor = 1
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	for _, rtr := range resp.Topics {
		if err := kerr.ErrorForCode(rtr.ErrorCode); err != nil {
			t.Fatalf("create topic %s: %v", rtr.Topic, err)
		}
	}
}

func integrationEvent(t *testing.T, user string, typ domain.ActivityType, md map[string]string) domain.ActivityEvent {
	t.Helper()
	return mustEvent(t, user, typ, time.Date(2024, 7, 1, 23, 30, 0, 0, time.FixedZone("BRT", -3*3600)), md)
}

// TestPublisherAgainstRealKafka covers produce -> consume with key and
// headers, readiness, the startup topic check, and the error mapping when the
// broker goes away. One broker container is shared by the subtests.
func TestPublisherAgainstRealKafka(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	schema := loadSchema(t)
	broker, ctr := startKafka(ctx, t)

	admin, err := kgo.NewClient(kgo.SeedBrokers(broker))
	if err != nil {
		t.Fatalf("admin client: %v", err)
	}
	t.Cleanup(admin.Close)

	pub, err := NewPublisher(Config{Brokers: []string{broker}, Topic: integrationTopic, PublishTimeout: 10 * time.Second}, nil)
	if err != nil {
		t.Fatalf("NewPublisher: %v", err)
	}
	pubClosed := false
	t.Cleanup(func() {
		if !pubClosed {
			pub.Close()
		}
	})

	t.Run("should report ready and a missing topic before it is created", func(t *testing.T) {
		pingCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := pub.Ping(pingCtx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		err := pub.EnsureTopic(pingCtx)
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Fatalf("EnsureTopic before creation = %v, want UnknownTopicOrPartition", err)
		}
	})

	createTopic(ctx, t, admin, integrationTopic, 3)

	t.Run("should find the topic once it exists", func(t *testing.T) {
		if err := pub.EnsureTopic(ctx); err != nil {
			t.Fatalf("EnsureTopic: %v", err)
		}
	})

	t.Run("should deliver keyed records with headers and a schema-valid value", func(t *testing.T) {
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(broker),
			kgo.ConsumeTopics(integrationTopic),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
		if err != nil {
			t.Fatalf("consumer: %v", err)
		}
		t.Cleanup(consumer.Close)

		type sent struct {
			id    uuid.UUID
			user  string
			trace tracectx.Context
		}
		var all []sent
		users := []string{"alice", "bob", "alice", "carol", "alice", "bob"}
		for _, u := range users {
			id := uuid.NewV7()
			md := map[string]string{"page_url": "https://example.com/" + u}
			trace := tracectx.NewTrace()
			trace.State = "vendor=opaque,other=1"
			if err := pub.Publish(tracectx.NewContext(ctx, trace), id, integrationEvent(t, u, domain.ActivityPageView, md)); err != nil {
				t.Fatalf("Publish(%s): %v", u, err)
			}
			all = append(all, sent{id: id, user: u, trace: trace})
		}

		fetchCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		got := map[string]kgo.Record{} // by event-id
		partitionOf := map[string]int32{}
		for len(got) < len(all) {
			fetches := consumer.PollFetches(fetchCtx)
			if err := fetches.Err0(); err != nil {
				t.Fatalf("poll: %v (got %d of %d records)", err, len(got), len(all))
			}
			fetches.EachRecord(func(r *kgo.Record) {
				got[headerValue(r, HeaderEventID)] = *r
			})
		}

		for _, s := range all {
			r, ok := got[s.id.String()]
			if !ok {
				t.Errorf("record for event %s not consumed", s.id)
				continue
			}
			if string(r.Key) != s.user {
				t.Errorf("key = %q, want user_id %q", r.Key, s.user)
			}
			if v := headerValue(&r, HeaderSchemaVersion); v != SchemaVersion {
				t.Errorf("schema-version = %q, want %q", v, SchemaVersion)
			}
			if v := headerValue(&r, tracectx.HeaderTraceparent); v != s.trace.Traceparent() {
				t.Errorf("traceparent = %q, want %q", v, s.trace.Traceparent())
			}
			if v := headerValue(&r, tracectx.HeaderTracestate); v != s.trace.State {
				t.Errorf("tracestate = %q, want %q", v, s.trace.State)
			}
			if p, seen := partitionOf[s.user]; seen && p != r.Partition {
				t.Errorf("user %s landed on partitions %d and %d, want one", s.user, p, r.Partition)
			}
			partitionOf[s.user] = r.Partition

			var doc any
			if err := json.Unmarshal(r.Value, &doc); err != nil {
				t.Fatalf("value is not JSON: %v", err)
			}
			if problems := validateSchema(schema, doc, "$"); len(problems) > 0 {
				t.Errorf("value %s violates the schema: %v", r.Value, problems)
			}
			m := doc.(map[string]any)
			if m["timestamp"] != "2024-07-02T02:30:00Z" {
				t.Errorf("timestamp = %v, want it normalized to UTC", m["timestamp"])
			}
		}
	})

	t.Run("should flush and close cleanly", func(t *testing.T) {
		flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := pub.Flush(flushCtx); err != nil {
			t.Fatalf("Flush: %v", err)
		}
	})

	t.Run("should map a broker outage to ErrPublishUnavailable", func(t *testing.T) {
		stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := ctr.Terminate(stopCtx); err != nil {
			t.Fatalf("terminate kafka: %v", err)
		}
		publishCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		err := pub.Publish(publishCtx, uuid.NewV7(), integrationEvent(t, "alice", domain.ActivityPageView, nil))

		if !errors.Is(err, ports.ErrPublishUnavailable) {
			t.Fatalf("Publish after outage = %v, want ErrPublishUnavailable", err)
		}
		pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		if err := pub.Ping(pingCtx); err == nil {
			t.Error("Ping succeeded after the broker was terminated")
		}
	})

	pub.Close()
	pubClosed = true
}

func headerValue(r *kgo.Record, key string) string {
	for _, h := range r.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}
