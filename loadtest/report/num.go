package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// num is an optional measurement. Templates print "n/a" for values that the
// run did not produce, instead of a misleading zero.
type num struct {
	V  float64
	OK bool
}

func some(v float64) num { return num{V: v, OK: true} }

func maybe(v float64, ok bool) num { return num{V: v, OK: ok} }

const notAvailable = "n/a"

// Int formats the value as an integer with thousands separators.
func (n num) Int() string {
	if !n.OK {
		return notAvailable
	}
	return groupThousands(int64(math.Round(n.V)))
}

// F formats the value with prec decimals.
func (n num) F(prec int) string {
	if !n.OK {
		return notAvailable
	}
	return strconv.FormatFloat(n.V, 'f', prec, 64)
}

// Ms formats a k6 duration (milliseconds).
func (n num) Ms() string {
	if !n.OK {
		return notAvailable
	}
	if n.V >= 1000 {
		return strconv.FormatFloat(n.V/1000, 'f', 2, 64) + " s"
	}
	return strconv.FormatFloat(n.V, 'f', 1, 64) + " ms"
}

// Sec formats a duration in seconds.
func (n num) Sec() string {
	if !n.OK {
		return notAvailable
	}
	switch {
	case n.V < 1:
		return strconv.FormatFloat(n.V*1000, 'f', 1, 64) + " ms"
	case n.V < 120:
		return strconv.FormatFloat(n.V, 'f', 1, 64) + " s"
	}
	m := int(n.V) / 60
	return fmt.Sprintf("%dm%02ds", m, int(n.V)-m*60)
}

// Pct formats a 0..1 ratio as a percentage.
func (n num) Pct() string {
	if !n.OK {
		return notAvailable
	}
	return strconv.FormatFloat(n.V*100, 'f', 2, 64) + " %"
}

// Perc formats a value that already is a percentage (docker's CPU %).
func (n num) Perc() string {
	if !n.OK {
		return notAvailable
	}
	return strconv.FormatFloat(n.V, 'f', 1, 64) + " %"
}

// Bytes formats a byte count with binary units.
func (n num) Bytes() string {
	if !n.OK {
		return notAvailable
	}
	return formatBytes(n.V)
}

// Signed formats an integer with an explicit sign.
func (n num) Signed() string {
	if !n.OK {
		return notAvailable
	}
	if n.V > 0 {
		return "+" + n.Int()
	}
	return n.Int()
}

func formatBytes(v float64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	if i == 0 {
		return strconv.FormatFloat(v, 'f', 0, 64) + " B"
	}
	return strconv.FormatFloat(v, 'f', 1, 64) + " " + units[i]
}

func groupThousands(v int64) string {
	s := strconv.FormatInt(v, 10)
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	if neg {
		return "-" + b.String()
	}
	return b.String()
}
