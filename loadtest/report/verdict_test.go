package main

import (
	"strings"
	"testing"
)

// baseInput is a passing run; each case changes one thing.
func baseInput(scenario string) verdictInput {
	return verdictInput{
		Scenario:         scenario,
		K6Available:      true,
		Thresholds:       []thresholdResult{{Metric: "http_req_duration", Expr: "p(95)<300", OK: true}},
		Accepted:         some(10000),
		PVAccepted:       some(6000),
		Unknown:          some(0),
		DBActivity:       some(10000),
		DBPageViews:      some(6000),
		InspectAvailable: true,
		Containers:       []containerState{{Name: "consumer-1"}},
		DrainAvailable:   true,
		DLQ:              some(0),
		TopicRecords:     some(10000),
		StopExitCode:     some(0),
		K6ExitCode:       some(0),
	}
}

// gapInput is a passing crash-commit-gap run with the given resume outcome.
func gapInput(available bool, partitions int, records float64) verdictInput {
	in := baseInput("crash-commit-gap")
	in.GapAvailable, in.GapPartitions, in.GapRecords = available, partitions, some(records)
	return in
}

// poisonInput is a poison-records run: 10000 events accepted plus injected
// malformed records on the topic, dlq of them dead-lettered.
func poisonInput(injected, dlq, decode, validation float64) verdictInput {
	in := baseInput("poison-records")
	in.TopicRecords = some(10000 + injected)
	in.DLQ = some(dlq)
	in.PoisonInjected = some(injected)
	in.PoisonClasses = map[string]float64{"decode": 26, "validation": 24}
	in.DLQByClass = map[string]float64{"decode": decode, "validation": validation}
	in.FinalLag = some(0)
	return in
}

func TestEvaluate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		in         func() verdictInput
		wantPass   bool
		failedName string // a check that must fail when wantPass is false
	}{
		{"should pass an exact load run", func() verdictInput { return baseInput("load") }, true, ""},
		{"should fail any overcount in a non-crash run", func() verdictInput {
			in := baseInput("load")
			in.DBActivity = some(10001)
			return in
		}, false, "Overcount within bound (activity_count)"},
		{"should fail loss in any scenario", func() verdictInput {
			in := baseInput("crash-consumer-kill")
			in.DBActivity = some(9999)
			return in
		}, false, "No lost events (activity_count)"},
		{"should fail page-view loss", func() verdictInput {
			in := baseInput("crash-consumer-kill")
			in.DBPageViews = some(5999)
			return in
		}, false, "Page views: no loss, overcount within bound"},
		{"should pass an exact SIGKILL run", func() verdictInput { return baseInput("crash-consumer-kill") }, true, ""},
		{"should fail any replay after a SIGKILL", func() verdictInput {
			in := baseInput("crash-consumer-kill")
			in.DBActivity, in.DBPageViews = some(10001), some(6001)
			return in
		}, false, "Overcount within bound (activity_count)"},
		{"should fail any replay after a kafka stop beyond events_unknown", func() verdictInput {
			in := baseInput("crash-kafka-stop")
			in.Unknown, in.DBActivity = some(40), some(10041)
			return in
		}, false, "Overcount within bound (activity_count)"},
		{"should fail any replay after a postgres pause", func() verdictInput {
			in := baseInput("crash-postgres-pause")
			in.DBActivity = some(10001)
			return in
		}, false, "Overcount within bound (activity_count)"},
		{"should fail a database total above the input topic", func() verdictInput {
			in := baseInput("crash-kafka-stop")
			in.Unknown, in.DBActivity = some(40), some(10025) // within the unknown allowance...
			return in
		}, false, "Effectively once (db = input topic − DLQ)"}, // ...but 25 more than Kafka holds
		{"should fail a database total below the input topic", func() verdictInput {
			in := baseInput("load")
			in.TopicRecords = some(10001)
			return in
		}, false, "Effectively once (db = input topic − DLQ)"},
		{"should subtract the DLQ records from the input topic", func() verdictInput {
			in := baseInput("load")
			in.TopicRecords, in.DLQ = some(10003), some(3)
			return in
		}, true, ""},
		{"should fail when the input topic count is missing", func() verdictInput {
			in := baseInput("load")
			in.TopicRecords = num{}
			return in
		}, false, "Effectively once (db = input topic − DLQ)"},
		{"should widen the bound by events_unknown", func() verdictInput {
			in := baseInput("load")
			in.Unknown, in.DBActivity, in.TopicRecords = some(5), some(10005), some(10005)
			return in
		}, true, ""},
		{"should fail any overcount after a graceful stop", func() verdictInput {
			in := baseInput("crash-consumer-stop")
			in.DBActivity = some(10001)
			return in
		}, false, "Overcount within bound (activity_count)"},
		{"should fail a non-zero exit code after SIGTERM", func() verdictInput {
			in := baseInput("crash-consumer-stop")
			in.StopExitCode = some(2)
			return in
		}, false, "Consumer exit code on SIGTERM"},
		{"should fail an unknown SIGTERM exit code", func() verdictInput {
			in := baseInput("crash-consumer-stop")
			in.StopExitCode = num{}
			return in
		}, false, "Consumer exit code on SIGTERM"},
		{"should fail DLQ records after a postgres pause", func() verdictInput {
			in := baseInput("crash-postgres-pause")
			in.DLQ = some(1)
			return in
		}, false, "No DLQ records"},
		{"should not check the DLQ in other scenarios", func() verdictInput {
			in := baseInput("load")
			in.DLQ, in.TopicRecords = some(3), some(10003)
			return in
		}, true, ""},
		{"should accept unknown requests that reached Kafka after a kafka stop", func() verdictInput {
			in := baseInput("crash-kafka-stop")
			in.Unknown, in.DBActivity, in.TopicRecords = some(40), some(10025), some(10025)
			return in
		}, true, ""},
		{"should fail when the database totals are missing", func() verdictInput {
			in := baseInput("load")
			in.DBActivity, in.DBPageViews = num{}, num{}
			return in
		}, false, "No lost events (activity_count)"},
		{"should fail when nothing was accepted", func() verdictInput {
			in := baseInput("load")
			in.Accepted, in.PVAccepted, in.DBActivity, in.DBPageViews = some(0), some(0), some(0), some(0)
			return in
		}, false, "Events accepted"},
		{"should fail an OOMKilled container", func() verdictInput {
			in := baseInput("load")
			in.Containers = append(in.Containers, containerState{Name: "kafka-1", OOMKilled: true})
			return in
		}, false, "No container OOMKilled"},
		{"should fail a drain timeout", func() verdictInput {
			in := baseInput("load")
			in.DrainTimedOut = true
			return in
		}, false, "Consumer drained (lag 0)"},
		{"should fail a crossed k6 threshold", func() verdictInput {
			in := baseInput("load")
			in.Thresholds = append(in.Thresholds, thresholdResult{Metric: "http_req_failed", Expr: "rate<0.01"})
			return in
		}, false, "k6 threshold http_req_failed"},
		{"should fail a missing k6 summary", func() verdictInput {
			in := baseInput("load")
			in.K6Available, in.Thresholds = false, nil
			return in
		}, false, "k6 thresholds"},
		{"should fail an aborted k6 run", func() verdictInput {
			in := baseInput("stress")
			in.K6ExitCode = some(108)
			return in
		}, false, "k6 run completed"},
		{"should pass a commit-gap run with a positive gap", func() verdictInput {
			return gapInput(true, 2, 61)
		}, true, ""},
		{"should fail a commit-gap run with stored == committed", func() verdictInput {
			return gapInput(true, 0, 0)
		}, false, "Commit gap exercised (stored > committed on resume)"},
		{"should fail a commit-gap run with no resume lines", func() verdictInput {
			return gapInput(false, 0, 0)
		}, false, "Commit gap exercised (stored > committed on resume)"},
		{"should not require a gap in other crash scenarios", func() verdictInput {
			in := gapInput(true, 0, 0)
			in.Scenario = "crash-consumer-kill"
			return in
		}, true, ""},
		{"should pass poison records that match the injected mix", func() verdictInput {
			return poisonInput(50, 50, 26, 24)
		}, true, ""},
		{"should fail when the DLQ holds fewer records than injected", func() verdictInput {
			return poisonInput(50, 49, 25, 24)
		}, false, "DLQ records = injected poison records"},
		{"should fail when the DLQ class mix differs", func() verdictInput {
			return poisonInput(50, 50, 24, 26)
		}, false, "DLQ by error class matches the injected mix"},
		{"should fail an unexpected DLQ class", func() verdictInput {
			in := poisonInput(50, 50, 26, 24)
			in.DLQByClass["unknown"] = 1
			return in
		}, false, "DLQ by error class matches the injected mix"},
		{"should fail poison records when nothing was injected", func() verdictInput {
			in := poisonInput(50, 50, 26, 24)
			in.PoisonInjected, in.PoisonClasses = num{}, nil
			return in
		}, false, "DLQ records = injected poison records"},
		{"should fail poison records when the lag does not drain", func() verdictInput {
			in := poisonInput(50, 50, 26, 24)
			in.FinalLag = some(120)
			return in
		}, false, "No partition stalled (lag 0 after the poison)"},
		{"should fail poison records when the topic minus the DLQ differs from the database", func() verdictInput {
			in := poisonInput(50, 50, 26, 24)
			in.DBActivity = some(10001)
			return in
		}, false, "Effectively once (db = input topic − DLQ)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cs := evaluate(tt.in())
			if got := passed(cs); got != tt.wantPass {
				t.Fatalf("passed = %v, want %v; checks:\n%s", got, tt.wantPass, dumpChecks(cs))
			}
			if tt.failedName == "" {
				return
			}
			for _, c := range cs {
				if c.Name == tt.failedName {
					if c.Pass {
						t.Errorf("check %q passed, want FAIL", c.Name)
					}
					return
				}
			}
			t.Errorf("check %q not found in:\n%s", tt.failedName, dumpChecks(cs))
		})
	}
}

func TestEvaluateLossIsLabelled(t *testing.T) {
	t.Parallel()
	in := baseInput("crash-consumer-kill")
	in.DBActivity = some(9990)
	for _, c := range evaluate(in) {
		if c.Name == "No lost events (activity_count)" && !strings.Contains(c.Actual, "LOSS") {
			t.Errorf("loss check actual = %q, want it to say LOSS", c.Actual)
		}
	}
}

func dumpChecks(cs []check) string {
	var b strings.Builder
	for _, c := range cs {
		b.WriteString(c.Status() + " " + c.Name + ": expected " + c.Expected + ", actual " + c.Actual + "\n")
	}
	return b.String()
}
