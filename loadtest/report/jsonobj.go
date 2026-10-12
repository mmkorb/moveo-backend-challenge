package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// object is a decoded JSON object with tolerant, case- and separator-insensitive
// key lookup. The files written by run.sh are produced by shell tools, so key
// spelling (run_id, runId, RunID) is normalised instead of being a contract.
type object map[string]any

// normKey lowercases k and drops '_', '-', ' ' and '.' so that "OOMKilled",
// "oom_killed" and "oom-killed" compare equal.
func normKey(k string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(k) {
		switch r {
		case '_', '-', ' ', '.':
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// lookup returns the first value found among keys.
func (o object) lookup(keys ...string) (any, bool) {
	if o == nil {
		return nil, false
	}
	for _, k := range keys {
		if v, ok := o[k]; ok && v != nil {
			return v, true
		}
	}
	for _, k := range keys {
		nk := normKey(k)
		for ok, v := range o {
			if normKey(ok) == nk && v != nil {
				return v, true
			}
		}
	}
	return nil, false
}

// str returns the value of the first present key rendered as a string.
func (o object) str(keys ...string) string {
	v, ok := o.lookup(keys...)
	if !ok {
		return ""
	}
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(t)
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return fmt.Sprint(t)
		}
		return string(b)
	}
}

// num returns the value of the first present key as a number. Numeric strings
// are accepted, because shell-generated JSON often quotes numbers.
func (o object) num(keys ...string) (float64, bool) {
	v, ok := o.lookup(keys...)
	if !ok {
		return 0, false
	}
	switch t := v.(type) {
	case float64:
		return t, true
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		return f, err == nil
	case bool:
		if t {
			return 1, true
		}
		return 0, true
	}
	return 0, false
}

// boolean returns the value of the first present key as a bool.
func (o object) boolean(keys ...string) (bool, bool) {
	v, ok := o.lookup(keys...)
	if !ok {
		return false, false
	}
	switch t := v.(type) {
	case bool:
		return t, true
	case string:
		b, err := strconv.ParseBool(strings.TrimSpace(t))
		return b, err == nil
	case float64:
		return t != 0, true
	}
	return false, false
}

// obj returns the nested object at the first present key.
func (o object) obj(keys ...string) object {
	v, ok := o.lookup(keys...)
	if !ok {
		return nil
	}
	m, _ := v.(map[string]any)
	return object(m)
}

// time returns the value of the first present key parsed as a timestamp.
// RFC 3339 strings and Unix seconds/milliseconds are accepted.
func (o object) time(keys ...string) (time.Time, bool) {
	v, ok := o.lookup(keys...)
	if !ok {
		return time.Time{}, false
	}
	return parseTime(v)
}

func parseTime(v any) (time.Time, bool) {
	switch t := v.(type) {
	case string:
		s := strings.TrimSpace(t)
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05Z0700", "2006-01-02 15:04:05Z07:00", "2006-01-02 15:04:05"} {
			if ts, err := time.Parse(layout, s); err == nil {
				return ts.UTC(), true
			}
		}
		if f, err := strconv.ParseFloat(s, 64); err == nil {
			return unixTime(f), true
		}
	case float64:
		return unixTime(t), true
	}
	return time.Time{}, false
}

func unixTime(f float64) time.Time {
	if f > 1e12 { // milliseconds
		return time.UnixMilli(int64(f)).UTC()
	}
	sec := int64(f)
	return time.Unix(sec, int64((f-float64(sec))*1e9)).UTC()
}

// readObject decodes a JSON object file.
func readObject(path string) (object, error) {
	b, err := os.ReadFile(path) //nolint:gosec // G304: path is under the results directory given on the operator's command line
	if err != nil {
		return nil, err
	}
	var o object
	if err := json.Unmarshal(b, &o); err != nil {
		return nil, fmt.Errorf("decode %s: %w", path, err)
	}
	return o, nil
}

// readJSONLines decodes a JSON-lines file. Blank lines are skipped and lines
// that are not JSON objects are counted, not fatal: a sampler killed mid-write
// leaves a truncated last line.
func readJSONLines(path string) (objs []object, bad int, err error) {
	f, err := os.Open(path) //nolint:gosec // G304: path is under the results directory given on the operator's command line
	if err != nil {
		return nil, 0, err
	}
	defer func() { _ = f.Close() }() // opened read-only: a failed close loses nothing
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var o object
		if err := json.Unmarshal(line, &o); err != nil {
			bad++
			continue
		}
		objs = append(objs, o)
	}
	if err := sc.Err(); err != nil {
		return objs, bad, fmt.Errorf("scan %s: %w", path, err)
	}
	return objs, bad, nil
}
