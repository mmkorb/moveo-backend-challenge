package kafka

import (
	"bytes"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/domain"
	"github.com/mmkorb/moveo-backend-challenge/services/consumer/internal/ports"
)

// addContractSeeds adds every contracts/examples/**/*.json file to the fuzz seed
// corpus (ADR 0014). Missing fixtures are not an error: the inline seeds remain.
func addContractSeeds(f *testing.F) {
	f.Helper()
	files, err := filepath.Glob(filepath.Join(contractsDir, "examples", "*", "*.json"))
	if err != nil {
		f.Fatal(err)
	}
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b, "1", []byte("user-1"))
	}
}

// FuzzDecodeRecord feeds arbitrary record bytes to decodeRecord. It must never
// panic; it returns exactly one of an event or a dead letter; an accepted event
// satisfies the domain rules; a dead letter has class decode or validation and
// keeps the original key, value, headers and coordinates byte for byte.
func FuzzDecodeRecord(f *testing.F) {
	addContractSeeds(f)
	f.Add([]byte(`{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z","new_field":42}`), "", []byte(nil))
	f.Add([]byte(`{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z"}`), "2", []byte("k"))
	f.Add([]byte(`{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z","metadata":{"n":1}}`), "1", []byte("k"))
	f.Add([]byte(`null`), "1", []byte("k"))
	f.Add([]byte{0xff, 0xfe, '{'}, "1", []byte{0x00})

	f.Fuzz(func(t *testing.T, value []byte, schemaVersion string, key []byte) {
		headers := []kgo.RecordHeader{{Key: headerEventID, Value: []byte("e-1")}}
		if schemaVersion != "" {
			headers = append(headers, kgo.RecordHeader{Key: headerSchemaVersion, Value: []byte(schemaVersion)})
		}
		r := &kgo.Record{Topic: testTopic, Partition: 3, Offset: 42, Key: slices.Clone(key), Value: slices.Clone(value), Headers: headers}

		ev, dl := decodeRecord(r)

		if !bytes.Equal(r.Value, value) || !bytes.Equal(r.Key, key) {
			t.Fatal("decodeRecord modified the input record")
		}
		if dl == nil {
			assertValidEvent(t, ev)
			return
		}
		if ev.UserID != "" || !ev.Timestamp.IsZero() {
			t.Fatalf("dead-lettered record also returned an event: %+v", ev)
		}
		if dl.Class != ports.ErrorClassDecode && dl.Class != ports.ErrorClassValidation {
			t.Fatalf("unexpected error class %q", dl.Class)
		}
		if !bytes.Equal(dl.Value, value) || !bytes.Equal(dl.Key, key) {
			t.Fatalf("dead letter changed the original bytes: key %q value %q", dl.Key, dl.Value)
		}
		if dl.Topic != testTopic || dl.Partition != 3 || dl.Offset != 42 {
			t.Fatalf("dead letter lost the coordinates: %+v", dl)
		}
		if len(dl.Headers) != len(headers) {
			t.Fatalf("dead letter has %d headers, want %d", len(dl.Headers), len(headers))
		}
		for i, h := range headers {
			if dl.Headers[i].Key != h.Key || !bytes.Equal(dl.Headers[i].Value, h.Value) {
				t.Fatalf("dead letter header %d changed", i)
			}
		}
	})
}

func assertValidEvent(t *testing.T, ev domain.ActivityEvent) {
	t.Helper()
	if n := len(ev.UserID); n == 0 || n > domain.MaxUserIDBytes || !utf8.ValidString(ev.UserID) {
		t.Fatalf("accepted event has invalid user_id %q", ev.UserID)
	}
	if n := len(ev.ActivityType); n == 0 || n > domain.MaxActivityTypeBytes || !utf8.ValidString(string(ev.ActivityType)) {
		t.Fatalf("accepted event has invalid activity_type %q", ev.ActivityType)
	}
	for _, r := range ev.UserID + string(ev.ActivityType) {
		if unicode.IsControl(r) {
			t.Fatalf("accepted event contains control character %q", r)
		}
	}
	if ev.Timestamp.IsZero() {
		t.Fatal("accepted event has a zero timestamp")
	}
	if ev.Timestamp.Location() != time.UTC {
		t.Fatalf("accepted event timestamp is not UTC: %v", ev.Timestamp.Location())
	}
	if len(ev.Metadata) > domain.MaxMetadataEntries {
		t.Fatalf("accepted event has %d metadata entries", len(ev.Metadata))
	}
	if _, err := domain.NewActivityEvent(ev.UserID, ev.ActivityType, ev.Timestamp, ev.Metadata); err != nil {
		t.Fatalf("accepted event fails domain validation: %v", err)
	}
}
