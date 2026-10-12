package main

import (
	"bufio"
	"encoding/json"
	"io"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

// Messages of the consumer's resume logging (services/consumer, member.go
// adjustOffsets) and of its start (cmd/consumer, run.go).
const (
	msgResumeStored = "resuming from the stored position"
	msgResumeNone   = "no stored position; resuming from the Kafka committed offset"
	msgStarting     = "starting"
)

// resumeEvent is one resume line of logs/consumer.log: a partition assigned
// to a member that looked up its stored position.
type resumeEvent struct {
	At        time.Time
	Member    string
	Partition int
	// HasStored is false for "no stored position": the Kafka committed offset
	// was used and there is no gap to measure.
	HasStored bool
	Stored    num
	Committed num // negative in the log when nothing was committed
}

// Gap is stored − committed: the records the database already holds past the
// last Kafka commit. A partition without a committed offset counts from 0.
func (e resumeEvent) Gap() (float64, bool) {
	if !e.HasStored || !e.Stored.OK {
		return 0, false
	}
	committed := 0.0
	if e.Committed.OK && e.Committed.V > 0 {
		committed = e.Committed.V
	}
	return e.Stored.V - committed, true
}

// recoveryRow is a resume event ready to print.
type recoveryRow struct {
	Time      string
	Member    string
	Partition string
	Stored    string
	Committed string
	Gap       string
}

// recoverySection reports what the stored positions did after a consumer
// start that was not the first one.
type recoverySection struct {
	Show bool
	// Available is true when the log had resume events to report.
	Available bool
	Starts    int // "starting" lines in logs/consumer.log
	Rows      []recoveryRow
	// Partitions with a positive gap, and the sum of those gaps: records the
	// database held past the Kafka commit, which the stored position kept the
	// consumer from applying a second time.
	GapPartitions int
	Protected     num
	// Fence counters (consumer /metrics, after − before).
	FenceRejections num
	FenceResyncs    num
	FenceNote       string
}

// RecoveryNum is the number of the Recovery section (after the optional timeline).
func (r *report) RecoveryNum() int { return 8 + b2i(r.Timeline.Show) }

// ArtifactsNum is the number of the last section.
func (r *report) ArtifactsNum() int { return 8 + b2i(r.Timeline.Show) + b2i(r.Recovery.Show) }

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// recoveryLogLine is the subset of a consumer slog line the report reads.
type recoveryLogLine struct {
	Time      string   `json:"time"`
	Msg       string   `json:"msg"`
	Member    *float64 `json:"member"`
	Partition *float64 `json:"partition"`
	Stored    *float64 `json:"stored_offset"`
	Committed *float64 `json:"committed_offset"`
}

// parseConsumerLog extracts the start lines and the resume events from the
// consumer's JSON log. Lines that are not JSON are skipped.
func parseConsumerLog(r io.Reader) (starts []time.Time, events []resumeEvent, err error) {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, "{") {
			continue
		}
		var l recoveryLogLine
		if json.Unmarshal([]byte(line), &l) != nil {
			continue
		}
		at, _ := parseTime(l.Time)
		switch l.Msg {
		case msgStarting, "consumer " + msgStarting:
			starts = append(starts, at)
		case msgResumeStored, msgResumeNone:
			if l.Partition == nil {
				continue
			}
			e := resumeEvent{At: at, Partition: int(*l.Partition), Member: "-"}
			if l.Member != nil {
				e.Member = strconv.Itoa(int(*l.Member))
			}
			if l.Committed != nil {
				e.Committed = some(*l.Committed)
			}
			if l.Msg == msgResumeStored && l.Stored != nil {
				e.HasStored = true
				e.Stored = some(*l.Stored)
			}
			events = append(events, e)
		}
	}
	return starts, events, sc.Err()
}

// buildRecovery fills the Recovery section from logs/consumer.log and the
// consumer metrics. It is shown for every run in which the consumer started
// more than once (a restart, or a second replica), and always for the
// crash-commit-gap scenario, whose check depends on it.
func (l *loader) buildRecovery(rc *recoverySection, scenario string, restarted bool, p pipelineSection) *recoveryOutcome {
	out := &recoveryOutcome{}
	var (
		starts []time.Time
		events []resumeEvent
	)
	f, err := os.Open(l.path(dirLogs + "/consumer.log"))
	if err != nil {
		l.missing(dirLogs+"/consumer.log", err)
	} else {
		defer func() { _ = f.Close() }() // opened read-only: a failed close loses nothing
		if starts, events, err = parseConsumerLog(f); err != nil {
			l.note("%s/consumer.log: %v", dirLogs, err)
		}
	}
	rc.Starts = len(starts)
	rc.Show = len(starts) > 1 || restarted || scenario == scenarioCommitGap
	if !rc.Show {
		return out
	}

	// Events of the first start are the initial assignment: no restart, no gap.
	from := time.Time{}
	if len(starts) > 1 {
		from = starts[1]
	}
	for _, e := range events {
		if !from.IsZero() && e.At.Before(from) {
			continue
		}
		out.events++
		row := recoveryRow{
			Time:      "n/a",
			Member:    e.Member,
			Partition: strconv.Itoa(e.Partition),
			Stored:    "none",
			Committed: "none",
			Gap:       "n/a",
		}
		if !e.At.IsZero() {
			row.Time = e.At.UTC().Format("15:04:05.000")
		}
		if e.HasStored {
			row.Stored = e.Stored.Int()
		}
		if e.Committed.OK && e.Committed.V >= 0 {
			row.Committed = e.Committed.Int()
		}
		if gap, ok := e.Gap(); ok {
			row.Gap = some(gap).Signed()
			if gap > 0 {
				out.gapPartitions++
				out.gapRecords += gap
			}
		}
		rc.Rows = append(rc.Rows, row)
	}
	rc.Available = out.events > 0
	rc.GapPartitions = out.gapPartitions
	rc.Protected = some(out.gapRecords)

	rc.FenceRejections, rc.FenceResyncs = p.Fenced, p.Resyncs
	if p.Reset && (rc.FenceRejections.OK || rc.FenceResyncs.OK) {
		rc.FenceNote = "the consumer restarted, so these counters cover the last process only"
	}
	return out
}

// recoveryOutcome carries the numbers the verdict needs.
type recoveryOutcome struct {
	events        int
	gapPartitions int
	gapRecords    float64
}

// parsePoison reads the poison_injected timeline event:
// {"count":50,"rc":0,"classes":{"decode":24,"validation":26},"kinds":{"invalid_json":7,...}}.
// Counts are what run.sh sent; whether they arrived is for the DLQ checks.
func parsePoison(o object) *poisonInfo {
	p := &poisonInfo{Count: maybe(o.num("count")), Classes: map[string]float64{}}
	classes := o.obj("classes")
	for class := range classes {
		if n, ok := classes.num(class); ok {
			p.Classes[class] = n
		}
	}
	kinds := o.obj("kinds")
	for _, k := range slices.Sorted(maps.Keys(kinds)) {
		if n, ok := kinds.num(k); ok {
			p.Kinds = append(p.Kinds, kv{k, some(n).Int()})
		}
	}
	return p
}
