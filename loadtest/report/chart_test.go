package main

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestLineChart(t *testing.T) {
	t.Parallel()
	t0 := time.Date(2026, 10, 12, 3, 0, 0, 0, time.UTC)
	at := func(s int) time.Time { return t0.Add(time.Duration(s) * time.Second) }
	fmtV := func(v float64) string { return strconv.FormatFloat(v, 'f', 0, 64) }

	t.Run("should return nil without enough data", func(t *testing.T) {
		t.Parallel()
		if c := lineChart("x", []seriesData{{Name: "a", Pts: []point{{at(0), 1}}}}, time.Time{}, nil, fmtV); c != nil {
			t.Errorf("lineChart with one point = %+v, want nil", c)
		}
	})

	t.Run("should break the line at a sampling gap", func(t *testing.T) {
		t.Parallel()
		var pts []point
		for _, s := range []int{0, 2, 4, 6, 8, 24, 26, 28} { // container stopped between 8 s and 24 s
			pts = append(pts, point{at(s), float64(s)})
		}
		c := lineChart("cpu", []seriesData{{Name: "consumer-1", Slot: 2, Pts: pts}}, at(0), []event{{at(10), "kill consumer"}}, fmtV)
		if c == nil {
			t.Fatal("lineChart returned nil")
		}
		if got := len(c.Series[0].Segments); got != 2 {
			t.Errorf("segments = %d, want 2: %q", got, c.Series[0].Segments)
		}
		if c.Series[0].Slot != 2 || c.Legend {
			t.Errorf("slot = %d, legend = %v; want 2, false (single series)", c.Series[0].Slot, c.Legend)
		}
		if len(c.Markers) != 1 || c.Markers[0].Label != "kill consumer" {
			t.Errorf("markers = %+v", c.Markers)
		}
		if len(c.Hits) != len(pts) || !strings.Contains(c.Hits[0].Title, "consumer-1: 0") {
			t.Errorf("hits = %+v", c.Hits)
		}
		if top := c.YTicks[len(c.YTicks)-1].Label; top != "50" { // niceCeil(28)
			t.Errorf("top y tick = %s, want 50", top)
		}
	})
}

func TestNiceCeil(t *testing.T) {
	t.Parallel()
	tests := []struct{ in, want float64 }{
		{0, 1}, {0.7, 1}, {1, 1}, {1.2, 2}, {2.2, 2.5}, {3, 5}, {7, 10}, {95.1, 100}, {4087, 5000}, {727, 1000},
	}
	for _, tt := range tests {
		if got := niceCeil(tt.in); got != tt.want {
			t.Errorf("niceCeil(%v) = %v, want %v", tt.in, got, tt.want)
		}
	}
}
