package httpapi

import (
	"bytes"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
)

// contractExamples is the repo-root contracts/examples directory seen from this
// package. It is absent in the Docker test stage, where only the inline seeds run.
const contractExamples = "../../../../../contracts/examples"

// addContractSeeds adds every contracts/examples/**/*.json file to the fuzz seed
// corpus (ADR 0014). Missing fixtures are not an error: the inline seeds remain.
func addContractSeeds(f *testing.F) {
	f.Helper()
	files, err := filepath.Glob(filepath.Join(contractExamples, "*", "*.json"))
	if err != nil {
		f.Fatal(err)
	}
	for _, file := range files {
		b, err := os.ReadFile(file)
		if err != nil {
			f.Fatal(err)
		}
		f.Add(b)
	}
}

// FuzzDecodeActivity feeds arbitrary bytes to the HTTP body decoder. It must never
// panic; an accepted body must yield a valid domain event; a rejected one must be a
// client error (requestError) or an over-limit error, never an internal one.
func FuzzDecodeActivity(f *testing.F) {
	addContractSeeds(f)
	f.Add([]byte(`{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z"}`))
	f.Add([]byte(`{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z"} {}`))
	f.Add([]byte(`{"user_id":"u","activity_type":"click","timestamp":"2024-07-01T12:34:56Z","x":1}`))
	f.Add([]byte(`{"user_id":"u\u0000","activity_type":"click","timestamp":"0001-01-01T00:00:00Z"}`))
	f.Add([]byte(`null`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Same wrapper the handler uses, so the over-limit path is exercised too.
		body := http.MaxBytesReader(nil, readCloser{bytes.NewReader(data)}, maxBodyBytes)
		ev, err := decodeActivity(body)
		if err != nil {
			var reqErr *requestError
			if !errors.As(err, &reqErr) && !tooLarge(err) {
				t.Fatalf("rejected body mapped to an internal error: %v", err)
			}
			if reqErr != nil && reqErr.Detail == "" {
				t.Fatalf("requestError without detail for %q", data)
			}
			if ev.UserID != "" || !ev.Timestamp.IsZero() {
				t.Fatalf("rejected body returned a non-zero event: %+v", ev)
			}
			return
		}
		assertValidEvent(t, ev)
	})
}

type readCloser struct{ *bytes.Reader }

func (readCloser) Close() error { return nil }

// assertValidEvent re-checks the domain rules independently of NewActivityEvent.
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
