package kafka

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

// contractsDir is the repo-root contracts directory seen from this package.
const contractsDir = "../../../../../contracts"

// requireContractFile returns the file's content, or skips (fails when CI=true)
// when the repo-root contracts are not available, e.g. in the Docker test stage
// (ADR 0014).
func requireContractFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		if os.Getenv("CI") == "true" {
			t.Fatalf("contract file %s is missing", path)
		}
		t.Skipf("contract file %s not available", path)
	}
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestContractSchemaMatchesDTO(t *testing.T) {
	raw := requireContractFile(t, filepath.Join(contractsDir, "user_activity.v1.schema.json"))
	var schema struct {
		Required   []string                   `json:"required"`
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}

	fields := map[string]bool{} // json name -> omitempty
	typ := reflect.TypeFor[activityMessage]()
	for f := range typ.Fields() {
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		fields[name] = strings.Contains(opts, "omitempty")
	}

	t.Run("should have exactly the schema's properties", func(t *testing.T) {
		var dto, props []string
		for name := range fields {
			dto = append(dto, name)
		}
		for name := range schema.Properties {
			props = append(props, name)
		}
		slices.Sort(dto)
		slices.Sort(props)
		if !slices.Equal(dto, props) {
			t.Fatalf("DTO fields %v, schema properties %v", dto, props)
		}
	})

	t.Run("should not mark required fields omitempty", func(t *testing.T) {
		for _, name := range schema.Required {
			if fields[name] {
				t.Errorf("required field %q is omitempty in the DTO", name)
			}
		}
	})
}

func TestContractExamples(t *testing.T) {
	dir := filepath.Join(contractsDir, "examples")
	if _, err := os.Stat(dir); errors.Is(err, fs.ErrNotExist) {
		if os.Getenv("CI") == "true" {
			t.Fatalf("contract examples %s are missing", dir)
		}
		t.Skipf("contract examples %s not available", dir)
	}

	for _, kind := range []string{"valid", "invalid"} {
		files, err := filepath.Glob(filepath.Join(dir, kind, "*.json"))
		if err != nil {
			t.Fatal(err)
		}
		if len(files) == 0 {
			t.Fatalf("no %s examples in %s", kind, dir)
		}
		for _, file := range files {
			t.Run(kind+"/"+filepath.Base(file), func(t *testing.T) {
				b, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				_, dl := decodeRecord(&kgo.Record{Topic: testTopic, Value: b})
				if kind == "invalid" {
					if dl == nil {
						t.Fatal("invalid example was accepted")
					}
					return
				}
				if dl != nil {
					t.Fatalf("valid example dead-lettered with class %s", dl.Class)
				}
				dec := json.NewDecoder(bytes.NewReader(b))
				dec.DisallowUnknownFields()
				var msg activityMessage
				if err := dec.Decode(&msg); err != nil {
					t.Fatalf("valid example does not decode strictly into the DTO: %v", err)
				}
			})
		}
	}
}
