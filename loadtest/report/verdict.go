package main

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// overcountBound is the largest allowed DB − accepted, before adding
// events_unknown, in EVERY scenario. The consumer stores its Kafka positions
// in PostgreSQL in the same transaction as the aggregates (ADR 0008, option A),
// so a crash, a failed offset commit or a rebalance can no longer apply a batch
// twice. events_unknown stays: a request that timed out or got a 5xx may still
// have reached Kafka, and then it is counted once, legitimately.
const overcountBound = 0.0

// check is one row of the verdict table.
type check struct {
	Name     string
	Expected string
	Actual   string
	Pass     bool
}

// Status renders the check result.
func (c check) Status() string {
	if c.Pass {
		return "PASS"
	}
	return "FAIL"
}

// verdictInput holds everything the pass/fail rules look at.
type verdictInput struct {
	Scenario string

	K6Available bool
	Thresholds  []thresholdResult

	Accepted   num // events_accepted (202)
	PVAccepted num // page_views_accepted
	Unknown    num // events_unknown: timeouts/5xx that may or may not be in Kafka

	DBActivity  num // SUM(activity_count)
	DBPageViews num // SUM(page_view_count)

	InspectAvailable bool
	Containers       []containerState

	DrainAvailable bool
	DrainTimedOut  bool

	DLQ          num // records in the DLQ topic
	TopicRecords num // records in the input topic (sum of its end offsets)

	StopExitCode num // consumer exit code after SIGTERM (crash-consumer-stop)
	K6ExitCode   num // 0 ok, 99 thresholds crossed, 108 aborted by threshold

	// crash-commit-gap: what the resume lines of logs/consumer.log show.
	GapAvailable  bool // consumer.log has resume events after the first start
	GapPartitions int  // partitions that resumed with stored > committed
	GapRecords    num  // sum of the positive gaps

	// poison-records: what was injected (timeline.jsonl) and what the DLQ holds.
	PoisonInjected num
	PoisonClasses  map[string]float64 // injected, by expected error class; nil when not injected
	DLQByClass     map[string]float64 // dlq_records_total{error_class}; nil when unavailable
	FinalLag       num                // consumer-group lag after the drain
}

// Scenarios with their own checks.
const (
	scenarioCommitGap = "crash-commit-gap"
	scenarioPoison    = "poison-records"
)

// isCrash reports whether the scenario runs a chaos action.
func isCrash(scenario string) bool { return strings.HasPrefix(scenario, "crash-") }

// hasTimeline reports whether run.sh writes scenario events (chaos actions or
// the poison injection) that the report lists as a timeline.
func hasTimeline(scenario string) bool { return isCrash(scenario) || scenario == scenarioPoison }

// evaluate applies the pass/fail rules. Missing inputs fail the check that
// needs them: a report that cannot prove correctness must not say PASS.
func evaluate(in verdictInput) []check {
	var cs []check

	if !in.K6Available {
		cs = append(cs, check{Name: "k6 thresholds", Expected: "all pass", Actual: "k6-summary.json not available"})
	} else if len(in.Thresholds) == 0 {
		cs = append(cs, check{Name: "k6 thresholds", Expected: "all pass", Actual: "none defined", Pass: true})
	}
	for _, t := range in.Thresholds {
		actual := "ok"
		if !t.OK {
			actual = "crossed"
		}
		cs = append(cs, check{Name: "k6 threshold " + t.Metric, Expected: t.Expr, Actual: actual, Pass: t.OK})
	}

	if in.K6ExitCode.OK {
		// Thresholds have their own rows; this catches script errors and aborts.
		cs = append(cs, check{
			Name:     "k6 run completed",
			Expected: "exit code 0 (99 = thresholds crossed)",
			Actual:   in.K6ExitCode.Int(),
			Pass:     in.K6ExitCode.V == 0,
		})
	}

	cs = append(cs, check{
		Name:     "Events accepted",
		Expected: "> 0",
		Actual:   in.Accepted.Int(),
		Pass:     in.Accepted.OK && in.Accepted.V > 0,
	})

	bound := overcountBound
	unknown := 0.0
	if in.Unknown.OK {
		unknown = in.Unknown.V
	}
	cs = append(cs,
		countCheck("No lost events (activity_count)", in.Accepted, in.DBActivity, bound, unknown, checkLoss),
		countCheck("Overcount within bound (activity_count)", in.Accepted, in.DBActivity, bound, unknown, checkOver),
		countCheck("Page views: no loss, overcount within bound", in.PVAccepted, in.DBPageViews, bound, unknown, checkLoss|checkOver),
		effectivelyOnceCheck(in),
	)

	if !in.InspectAvailable {
		cs = append(cs, check{Name: "No container OOMKilled", Expected: "none", Actual: "inspect.jsonl not available"})
	} else {
		var oom []string
		for _, c := range in.Containers {
			if c.OOMKilled {
				oom = append(oom, c.Name)
			}
		}
		actual := "none"
		if len(oom) > 0 {
			actual = strings.Join(oom, ", ")
		}
		cs = append(cs, check{Name: "No container OOMKilled", Expected: "none", Actual: actual, Pass: len(oom) == 0})
	}

	switch {
	case !in.DrainAvailable:
		cs = append(cs, check{Name: "Consumer drained (lag 0)", Expected: "before timeout", Actual: "drain.json not available"})
	case in.DrainTimedOut:
		cs = append(cs, check{Name: "Consumer drained (lag 0)", Expected: "before timeout", Actual: "timed out"})
	default:
		cs = append(cs, check{Name: "Consumer drained (lag 0)", Expected: "before timeout", Actual: "drained", Pass: true})
	}

	switch in.Scenario {
	case "crash-postgres-pause":
		cs = append(cs, check{
			Name:     "No DLQ records",
			Expected: "0",
			Actual:   in.DLQ.Int(),
			Pass:     in.DLQ.OK && in.DLQ.V == 0,
		})
	case "crash-consumer-stop":
		cs = append(cs, check{
			Name:     "Consumer exit code on SIGTERM",
			Expected: "0",
			Actual:   in.StopExitCode.Int(),
			Pass:     in.StopExitCode.OK && in.StopExitCode.V == 0,
		})
	case scenarioCommitGap:
		cs = append(cs, commitGapCheck(in))
	case scenarioPoison:
		cs = append(cs, poisonChecks(in)...)
	}
	return cs
}

// commitGapCheck proves that the scenario exercised what it claims: the DB
// commit succeeded, the Kafka commit did not, so the consumer found its stored
// position ahead of the committed offset on resume. Without a positive gap the
// run only repeated a plain kill, and the position table was never tested.
func commitGapCheck(in verdictInput) check {
	c := check{Name: "Commit gap exercised (stored > committed on resume)", Expected: "gap > 0 on at least one partition"}
	switch {
	case !in.GapAvailable:
		c.Actual = "no resume lines in logs/consumer.log after the first start: the scenario did not test what it claims"
	case in.GapPartitions == 0:
		c.Actual = "stored == committed on every partition: the scenario did not test what it claims"
	default:
		c.Actual = fmt.Sprintf("%s records on %d partitions", in.GapRecords.Int(), in.GapPartitions)
		c.Pass = true
	}
	return c
}

// poisonChecks verifies the DLQ path end to end: every injected record is
// dead-lettered once, in the class the consumer's decode/validation split
// predicts, and no partition stalls on them.
func poisonChecks(in verdictInput) []check {
	var cs []check
	n := check{Name: "DLQ records = injected poison records", Expected: "equal"}
	switch {
	case !in.PoisonInjected.OK:
		n.Actual = "poison_injected event missing from timeline.jsonl"
	case !in.DLQ.OK:
		n.Actual = "DLQ offsets not available"
	default:
		n.Actual = fmt.Sprintf("dlq %s, injected %s", in.DLQ.Int(), in.PoisonInjected.Int())
		n.Pass = in.DLQ.V == in.PoisonInjected.V
	}
	cs = append(cs, n)

	k := check{Name: "DLQ by error class matches the injected mix", Expected: "per class, dlq_records_total = injected"}
	switch {
	case in.PoisonClasses == nil:
		k.Actual = "poison_injected event missing from timeline.jsonl"
	case in.DLQByClass == nil:
		k.Actual = "dlq_records_total not available"
	default:
		var parts []string
		k.Pass = true
		for _, class := range classUnion(in.PoisonClasses, in.DLQByClass) {
			want, got := in.PoisonClasses[class], in.DLQByClass[class]
			parts = append(parts, fmt.Sprintf("%s %s/%s", class, some(got).Int(), some(want).Int()))
			if got != want {
				k.Pass = false
			}
		}
		k.Actual = strings.Join(parts, ", ") + " (dlq/injected)"
	}
	cs = append(cs, k)

	l := check{Name: "No partition stalled (lag 0 after the poison)", Expected: "final lag 0 before the timeout"}
	switch {
	case !in.DrainAvailable:
		l.Actual = "drain.json not available"
	case in.DrainTimedOut:
		l.Actual = "timed out"
	case in.FinalLag.OK && in.FinalLag.V != 0:
		l.Actual = "final lag " + in.FinalLag.Int()
	default:
		l.Actual = "lag 0"
		l.Pass = true
	}
	return append(cs, l)
}

// classUnion returns the sorted union of the keys of a and b.
func classUnion(a, b map[string]float64) []string {
	set := map[string]bool{}
	for k := range a {
		set[k] = true
	}
	for k := range b {
		set[k] = true
	}
	return slices.Sorted(maps.Keys(set))
}

// countMode selects which side of the accepted-vs-database comparison a
// countCheck asserts.
type countMode int

const (
	checkLoss countMode = 1 << iota // db ≥ accepted
	checkOver                       // db − accepted ≤ bound + unknown
)

// countCheck compares accepted events with the database total.
func countCheck(name string, accepted, db num, bound, unknown float64, mode countMode) check {
	c := check{Name: name}
	switch mode {
	case checkLoss:
		c.Expected = "db ≥ accepted"
	case checkOver:
		c.Expected = "db − accepted ≤ " + boundText(bound, unknown)
	default:
		c.Expected = "0 ≤ db − accepted ≤ " + boundText(bound, unknown)
	}
	if !accepted.OK || !db.OK {
		c.Actual = "not available"
		return c
	}
	over := db.V - accepted.V
	c.Actual = fmt.Sprintf("db %s, accepted %s (%s)", db.Int(), accepted.Int(), some(over).Signed())
	c.Pass = true
	if mode&checkLoss != 0 && over < 0 {
		c.Pass = false
		c.Actual += " LOSS"
	}
	if mode&checkOver != 0 && over > bound+unknown {
		c.Pass = false
	}
	return c
}

// effectivelyOnceCheck compares the database with what Kafka holds: every
// record of the input topic is either aggregated once or parked once in the
// DLQ, so SUM(activity_count) = input records − DLQ records, whatever crashed.
// The topic is the ground truth for "produced": unlike the accepted count it
// includes the requests that got a 5xx or timed out but reached Kafka, so no
// events_unknown allowance is needed. It assumes the stack started empty and
// only this run produced to it (run.sh brings it up with fresh volumes).
func effectivelyOnceCheck(in verdictInput) check {
	c := check{Name: "Effectively once (db = input topic − DLQ)", Expected: "db = input topic records − DLQ records"}
	if !in.DBActivity.OK || !in.TopicRecords.OK || !in.DLQ.OK {
		c.Actual = "not available"
		return c
	}
	want := in.TopicRecords.V - in.DLQ.V
	diff := in.DBActivity.V - want
	c.Actual = fmt.Sprintf("db %s, topic %s, dlq %s (%s)", in.DBActivity.Int(), in.TopicRecords.Int(), in.DLQ.Int(), some(diff).Signed())
	c.Pass = diff == 0
	return c
}

func boundText(bound, unknown float64) string {
	s := some(bound).Int()
	if unknown > 0 {
		s += " + " + some(unknown).Int() + " unknown"
	}
	return s
}

// passed reports whether every check passed.
func passed(cs []check) bool {
	for _, c := range cs {
		if !c.Pass {
			return false
		}
	}
	return len(cs) > 0
}
