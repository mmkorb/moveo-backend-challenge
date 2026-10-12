package main

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"
)

// buildResources aggregates stats.jsonl per container and joins inspect.jsonl.
// CPU and memory figures cover only the samples between the k6_start and k6_end
// timeline events (container start-up would otherwise dominate the peaks); when
// either event is missing, all samples are used and WindowNote says so. The
// charts always show the full timeline, with the k6 window marked.
func (l *loader) buildResources(rs *resourcesSection, project string, origin time.Time, markers []event, k6Start, k6End time.Time) {
	windowed := !k6Start.IsZero() && !k6End.IsZero() && k6End.After(k6Start)
	if windowed {
		rs.WindowNote = "CPU and memory figures cover the k6 window only (" + k6Start.Format("15:04:05") + " to " +
			k6End.Format("15:04:05") + " UTC); start-up and drain are excluded."
		markers = append(slices.Clone(markers), event{T: k6Start, Label: "k6 start"}, event{T: k6End, Label: "k6 end"})
	} else {
		rs.WindowNote = "The timeline has no k6_start/k6_end events, so CPU and memory figures cover all samples, start-up included."
	}
	objs, bad, err := readJSONLines(l.path(fileStats))
	if err != nil {
		l.missing(fileStats, err)
	}
	samples, skipped := parseStats(objs)
	rs.Skipped = skipped + bad
	if rs.Skipped > 0 {
		// Containers that are stopped (chaos) report "--"; that is expected.
		l.note("%s: %d samples skipped (stopped containers or malformed lines)", fileStats, rs.Skipped)
	}

	inspObjs, inspBad, err := readJSONLines(l.path(fileInspect))
	if err != nil {
		l.missing(fileInspect, err)
	} else {
		rs.inspectOK = true
		rs.inspect = parseInspect(inspObjs)
		if inspBad > 0 {
			l.note("%s: %d malformed lines skipped", fileInspect, inspBad)
		}
	}

	byName := map[string][]statSample{}
	for _, s := range samples {
		name := shortName(s.Name, project)
		byName[name] = append(byName[name], s)
	}
	inspByName := map[string]containerState{}
	for _, c := range rs.inspect {
		inspByName[shortName(c.Name, project)] = c
	}
	names := slices.Sorted(maps.Keys(byName))
	for n := range inspByName {
		if _, ok := byName[n]; !ok {
			names = append(names, n)
		}
	}
	slices.SortFunc(names, func(a, b string) int {
		return cmp.Or(cmp.Compare(serviceRank(a), serviceRank(b)), strings.Compare(a, b))
	})

	var cpu, mem []seriesData
	for i, name := range names {
		row := containerRow{Name: name, OOMKilled: notAvailable, Restarts: notAvailable, ExitCode: notAvailable, Status: notAvailable}
		ss := byName[name]
		slices.SortFunc(ss, func(a, b statSample) int { return a.Time.Compare(b.Time) })
		if len(ss) > 0 {
			var cpuSum, memSum, cpuMax, memMax, limit float64
			inWindow := 0
			cs := seriesData{Name: name, Slot: i + 1}
			ms := seriesData{Name: name, Slot: i + 1}
			for _, s := range ss {
				limit = max(limit, s.MemLimit)
				cs.Pts = append(cs.Pts, point{s.Time, s.CPUPerc})
				ms.Pts = append(ms.Pts, point{s.Time, s.MemUsed / (1 << 20)})
				if windowed && (s.Time.Before(k6Start) || s.Time.After(k6End)) {
					continue
				}
				inWindow++
				cpuSum += s.CPUPerc
				memSum += s.MemUsed
				cpuMax = max(cpuMax, s.CPUPerc)
				memMax = max(memMax, s.MemUsed)
			}
			row.Samples = inWindow
			if inWindow > 0 {
				cnt := float64(inWindow)
				row.CPUAvg, row.CPUPeak = some(cpuSum/cnt), some(cpuMax)
				row.MemAvg, row.MemPeak = some(memSum/cnt), some(memMax)
				if limit > 0 {
					row.MemPct = some(memMax / limit)
				}
			}
			if limit > 0 {
				row.MemLimit = some(limit)
			}
			last := ss[len(ss)-1]
			row.NetRx, row.NetTx = some(last.NetRx), some(last.NetTx)
			cpu, mem = append(cpu, cs), append(mem, ms)
		}
		if c, ok := inspByName[name]; ok {
			row.OOMKilled = strconv.FormatBool(c.OOMKilled)
			row.Restarts = some(c.RestartCount).Int()
			row.ExitCode = some(c.ExitCode).Int()
			row.Status = c.Status
		}
		rs.Containers = append(rs.Containers, row)
	}
	rs.Available = len(rs.Containers) > 0
	pct := func(v float64) string { return strconv.FormatFloat(v, 'f', 0, 64) + "%" }
	mib := func(v float64) string { return strconv.FormatFloat(v, 'f', 0, 64) + " MiB" }
	rs.CPUChart = lineChart("CPU % per container (100% = one core)", cpu, origin, markers, pct)
	rs.MemChart = lineChart("Memory per container (MiB)", mem, origin, markers, mib)
}

// serviceRank orders the core services first so they keep the leading
// palette slots across runs.
func serviceRank(name string) int {
	for i, p := range []string{"producer", "consumer", "kafka", "postgres", "k6"} {
		if strings.HasPrefix(name, p) {
			return i
		}
	}
	return 99
}

// lagChart renders total consumer lag over time.
func lagChart(lag []lagSample, origin time.Time, markers []event) *chart {
	s := seriesData{Name: "lag", Slot: 1}
	for _, x := range lag {
		s.Pts = append(s.Pts, point{x.Time, x.Lag})
	}
	return lineChart("Consumer lag (records, all partitions)", []seriesData{s}, origin, markers, func(v float64) string {
		return some(v).Int()
	})
}
