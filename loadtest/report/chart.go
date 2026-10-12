package main

import (
	"fmt"
	"math"
	"slices"
	"strings"
	"time"
)

// Chart geometry (SVG user units; the SVG scales to its container width).
const (
	chartW      = 720
	chartH      = 240
	chartLeft   = 56
	chartRight  = 12
	chartTop    = 12
	chartBottom = 28
	maxSlots    = 8 // categorical palette size; extra series fold into the last slot
)

// point is one observation of a series.
type point struct {
	T time.Time
	V float64
}

// seriesData is the input of lineChart. Slot selects the categorical colour
// (1-based) and must stay the same for an entity across charts.
type seriesData struct {
	Name string
	Slot int
	Pts  []point
}

// chartSeries is a rendered series: one polyline per run of samples without
// a gap, so an outage (container stopped, /metrics down) shows as a break
// instead of an interpolated line.
type chartSeries struct {
	Name     string
	Slot     int
	Segments []string
}

// tick is an axis tick with its position and label.
type tick struct {
	Pos   float64
	Label string
}

// hit is an invisible hover band with a native SVG tooltip that lists every
// series at one sample time.
type hit struct {
	X, W  float64
	Title string
}

// event is a labelled instant, such as a chaos action.
type event struct {
	T     time.Time
	Label string
}

// marker is a rendered vertical line for an event.
type marker struct {
	X, LabelX, LabelY float64
	Label             string
}

// chart is the view model of an inline SVG line chart.
type chart struct {
	Title            string
	W, H             int
	X0, Y0, X1, Y1   float64 // plot area
	PlotH            float64
	YLabelX, XLabelY float64
	Series           []chartSeries
	YTicks, XTicks   []tick
	Hits             []hit
	Markers          []marker
	Legend           bool
	AriaLabel        string
}

// lineChart renders series sharing one time axis and one y axis starting at
// zero. origin labels the x axis as seconds relative to it (k6 start); when it
// is zero the first sample is used. It returns nil when there is no data.
func lineChart(title string, series []seriesData, origin time.Time, markers []event, yFmt func(float64) string) *chart {
	var tmin, tmax time.Time
	ymax := 0.0
	n := 0
	for _, s := range series {
		for _, p := range s.Pts {
			if tmin.IsZero() || p.T.Before(tmin) {
				tmin = p.T
			}
			if p.T.After(tmax) {
				tmax = p.T
			}
			ymax = max(ymax, p.V)
			n++
		}
	}
	if n < 2 || !tmax.After(tmin) {
		return nil
	}
	if origin.IsZero() {
		origin = tmin
	}
	ymax = niceCeil(ymax)
	c := &chart{
		Title: title, W: chartW, H: chartH,
		X0: chartLeft, Y0: chartTop, X1: chartW - chartRight, Y1: chartH - chartBottom,
		Legend: len(series) > 1,
	}
	c.PlotH = c.Y1 - c.Y0
	c.YLabelX, c.XLabelY = c.X0-6, c.Y1+16
	span := tmax.Sub(tmin).Seconds()
	xOf := func(t time.Time) float64 { return round1(c.X0 + (c.X1-c.X0)*t.Sub(tmin).Seconds()/span) }
	yOf := func(v float64) float64 { return round1(c.Y1 - (c.Y1-c.Y0)*v/ymax) }

	for _, s := range series {
		cs := chartSeries{Name: s.Name, Slot: min(max(s.Slot, 1), maxSlots)}
		gap := gapThreshold(s.Pts)
		var b strings.Builder
		for i, p := range s.Pts {
			if i > 0 && p.T.Sub(s.Pts[i-1].T) > gap {
				cs.Segments = append(cs.Segments, b.String())
				b.Reset()
			}
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			fmt.Fprintf(&b, "%.1f,%.1f", xOf(p.T), yOf(p.V))
		}
		cs.Segments = append(cs.Segments, b.String())
		c.Series = append(c.Series, cs)
	}
	for i := range 5 {
		v := ymax * float64(i) / 4
		c.YTicks = append(c.YTicks, tick{Pos: yOf(v), Label: yFmt(v)})
	}
	step := niceStep(span / 6)
	first := math.Ceil(tmin.Sub(origin).Seconds()/step) * step
	for rel := first; ; rel += step {
		t := origin.Add(time.Duration(rel * float64(time.Second)))
		if t.After(tmax) {
			break
		}
		c.XTicks = append(c.XTicks, tick{Pos: xOf(t), Label: fmt.Sprintf("%+.0fs", rel)})
	}
	for _, m := range markers {
		if m.T.Before(tmin) || m.T.After(tmax) {
			continue
		}
		// Alternate label rows so close actions (kill, start) do not overlap.
		row := float64(len(c.Markers) % 2)
		c.Markers = append(c.Markers, marker{X: xOf(m.T), LabelX: xOf(m.T) + 3, LabelY: c.Y0 + 10 + 13*row, Label: m.Label})
	}
	c.Hits = hits(series, xOf, c, origin, yFmt)
	c.AriaLabel = fmt.Sprintf("%s: %d series from %s to %s, y axis 0 to %s", title, len(series),
		tmin.Format("15:04:05"), tmax.Format("15:04:05"), yFmt(ymax))
	return c
}

// hits builds one hover band per distinct sample time.
func hits(series []seriesData, xOf func(time.Time) float64, c *chart, origin time.Time, yFmt func(float64) string) []hit {
	byTime := map[int64][]string{}
	for _, s := range series {
		for _, p := range s.Pts {
			k := p.T.Unix()
			byTime[k] = append(byTime[k], s.Name+": "+yFmt(p.V))
		}
	}
	keys := make([]int64, 0, len(byTime))
	for k := range byTime {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	xs := make([]float64, len(keys))
	for i, k := range keys {
		xs[i] = xOf(time.Unix(k, 0))
	}
	out := make([]hit, 0, len(keys))
	for i, k := range keys {
		left, right := c.X0, c.X1
		if i > 0 {
			left = (xs[i-1] + xs[i]) / 2
		}
		if i < len(keys)-1 {
			right = (xs[i] + xs[i+1]) / 2
		}
		t := time.Unix(k, 0).UTC()
		title := fmt.Sprintf("%s (%+.0fs)\n%s", t.Format("15:04:05"), t.Sub(origin).Seconds(), strings.Join(byTime[k], "\n"))
		out = append(out, hit{X: round1(left), W: round1(max(right-left, 0.5)), Title: title})
	}
	return out
}

// gapThreshold returns the interval above which two consecutive samples are
// treated as a gap: 2.5 × the median sampling interval.
func gapThreshold(pts []point) time.Duration {
	if len(pts) < 3 {
		return time.Duration(math.MaxInt64)
	}
	d := make([]time.Duration, 0, len(pts)-1)
	for i := 1; i < len(pts); i++ {
		d = append(d, pts[i].T.Sub(pts[i-1].T))
	}
	slices.Sort(d)
	return d[len(d)/2] * 5 / 2
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }

// niceCeil rounds v up to 1, 2, 2.5 or 5 times a power of ten.
func niceCeil(v float64) float64 {
	if v <= 0 {
		return 1
	}
	p := math.Pow(10, math.Floor(math.Log10(v)))
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if v <= m*p {
			return m * p
		}
	}
	return 10 * p
}

// niceStep picks an x-axis step in seconds.
func niceStep(v float64) float64 {
	for _, s := range []float64{5, 10, 15, 30, 60, 120, 300, 600, 900, 1800, 3600} {
		if v <= s {
			return s
		}
	}
	return 3600
}
