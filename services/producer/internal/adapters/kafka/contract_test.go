package kafka

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/google/go-cmp/cmp"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
)

// contractsDir is the repo-root contracts directory, seen from this package.
const contractsDir = "../../../../../contracts"

// loadSchema reads contracts/user_activity.v1.schema.json. The Docker test
// stage only sees the service directory, so the test skips there; with
// CI=true (the host test run) a missing contract is a failure (ADR 0014).
func loadSchema(t *testing.T) map[string]any {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(contractsDir, "user_activity.v1.schema.json"))
	if err != nil {
		if os.Getenv("CI") == "true" {
			t.Fatalf("contract schema is required when CI=true: %v", err)
		}
		t.Skipf("contract schema not available (Docker test stage?): %v", err)
	}
	var schema map[string]any
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	return schema
}

func mustEvent(t *testing.T, user string, typ domain.ActivityType, ts time.Time, md map[string]string) domain.ActivityEvent {
	t.Helper()
	ev, err := domain.NewActivityEvent(user, typ, ts, md)
	if err != nil {
		t.Fatalf("NewActivityEvent: %v", err)
	}
	return ev
}

func contractEvents(t *testing.T) map[string]domain.ActivityEvent {
	t.Helper()
	maxMeta := map[string]string{}
	for i := range domain.MaxMetadataEntries {
		maxMeta[fmt.Sprintf("key-%02d", i)] = strings.Repeat("v", domain.MaxMetadataValBytes)
	}
	return map[string]domain.ActivityEvent{
		"challenge example": mustEvent(t, "12345", domain.ActivityPageView,
			time.Date(2024, 7, 1, 12, 34, 56, 0, time.UTC),
			map[string]string{"page_url": "https://example.com/home", "referrer": "https://google.com"}),
		"no metadata": mustEvent(t, "u1", "click", time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC), nil),
		"non-UTC offset": mustEvent(t, "u2", "click",
			time.Date(2024, 7, 1, 23, 30, 0, 0, time.FixedZone("BRT", -3*3600)), nil),
		"unknown activity type": mustEvent(t, "u3", "something_new", time.Date(2024, 7, 1, 1, 0, 0, 123456789, time.UTC), nil),
		"limits": mustEvent(t, strings.Repeat("u", domain.MaxUserIDBytes), domain.ActivityType(strings.Repeat("t", domain.MaxActivityTypeBytes)),
			time.Date(2024, 7, 1, 1, 0, 0, 0, time.UTC), maxMeta),
		"empty metadata map": mustEvent(t, "u4", "click", time.Date(2024, 7, 1, 1, 0, 0, 0, time.UTC), map[string]string{}),
		"non-ASCII":          mustEvent(t, "usuário-é", "clique", time.Date(2024, 7, 1, 1, 0, 0, 0, time.UTC), map[string]string{"ação": "visualização"}),
	}
}

func TestDTOFieldsMatchTheSchema(t *testing.T) {
	t.Parallel()
	schema := loadSchema(t)

	props, _ := schema["properties"].(map[string]any)
	var schemaFields []string
	for name := range props {
		schemaFields = append(schemaFields, name)
	}
	slices.Sort(schemaFields)

	required := map[string]bool{}
	for _, r := range schema["required"].([]any) {
		required[r.(string)] = true
	}

	var dtoFields []string
	typ := reflect.TypeFor[activityMessage]()
	for f := range typ.Fields() {
		name, opts, _ := strings.Cut(f.Tag.Get("json"), ",")
		dtoFields = append(dtoFields, name)
		if required[name] && strings.Contains(opts, "omitempty") {
			t.Errorf("required field %q must not be omitempty", name)
		}
	}
	slices.Sort(dtoFields)

	if diff := cmp.Diff(schemaFields, dtoFields); diff != "" {
		t.Errorf("DTO json fields differ from schema properties (-schema +dto):\n%s", diff)
	}
}

func TestEncodedRecordsValidateAgainstTheSchema(t *testing.T) {
	t.Parallel()
	schema := loadSchema(t)

	for name, ev := range contractEvents(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value, err := encodeActivity(ev)
			if err != nil {
				t.Fatal(err)
			}
			var doc any
			if err := json.Unmarshal(value, &doc); err != nil {
				t.Fatalf("record value is not JSON: %v", err)
			}
			if problems := validateSchema(schema, doc, "$"); len(problems) > 0 {
				t.Errorf("record %s violates the schema:\n%s", value, strings.Join(problems, "\n"))
			}
			ts, _ := doc.(map[string]any)["timestamp"].(string)
			if !strings.HasSuffix(ts, "Z") {
				t.Errorf("timestamp %q is not normalized to UTC", ts)
			}
		})
	}
}

func TestEncodedRecordsRoundTrip(t *testing.T) {
	t.Parallel()
	for name, ev := range contractEvents(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			value, err := encodeActivity(ev)
			if err != nil {
				t.Fatal(err)
			}
			dec := json.NewDecoder(bytes.NewReader(value))
			dec.DisallowUnknownFields()
			var msg activityMessage
			if err := dec.Decode(&msg); err != nil {
				t.Fatalf("strict decode: %v", err)
			}
			back, err := domain.NewActivityEvent(msg.UserID, domain.ActivityType(msg.ActivityType), msg.Timestamp, msg.Metadata)
			if err != nil {
				t.Fatalf("decoded record is not a valid event: %v", err)
			}
			want := ev
			if len(want.Metadata) == 0 {
				want.Metadata = nil // omitempty drops an empty map
			}
			if len(back.Metadata) == 0 {
				back.Metadata = nil
			}
			if diff := cmp.Diff(want, back); diff != "" {
				t.Errorf("round trip mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

// TestSchemaValidatorRejectsInvalidDocuments guards the test-only validator
// itself, so a silent no-op cannot make the contract tests pass vacuously.
func TestSchemaValidatorRejectsInvalidDocuments(t *testing.T) {
	t.Parallel()
	schema := loadSchema(t)
	tests := map[string]string{
		"missing user_id":      `{"activity_type":"a","timestamp":"2024-07-01T00:00:00Z"}`,
		"missing timestamp":    `{"user_id":"u","activity_type":"a"}`,
		"extra field":          `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","x":1}`,
		"empty user_id":        `{"user_id":"","activity_type":"a","timestamp":"2024-07-01T00:00:00Z"}`,
		"long activity_type":   `{"user_id":"u","activity_type":"` + strings.Repeat("a", 65) + `","timestamp":"2024-07-01T00:00:00Z"}`,
		"timestamp not a date": `{"user_id":"u","activity_type":"a","timestamp":"yesterday"}`,
		"timestamp a number":   `{"user_id":"u","activity_type":"a","timestamp":5}`,
		"metadata not strings": `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","metadata":{"k":1}}`,
		"metadata empty key":   `{"user_id":"u","activity_type":"a","timestamp":"2024-07-01T00:00:00Z","metadata":{"":"v"}}`,
		"not an object":        `[]`,
		"NUL in user_id":       `{"user_id":"a\u0000b","activity_type":"a","timestamp":"2024-07-01T00:00:00Z"}`,
		"newline in type":      `{"user_id":"u","activity_type":"a\nb","timestamp":"2024-07-01T00:00:00Z"}`,
		"C1 control in userid": `{"user_id":"a\u0085b","activity_type":"a","timestamp":"2024-07-01T00:00:00Z"}`,
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var doc any
			if err := json.Unmarshal([]byte(raw), &doc); err != nil {
				t.Fatal(err)
			}
			if len(validateSchema(schema, doc, "$")) == 0 {
				t.Errorf("document %s was accepted", raw)
			}
		})
	}
}

// TestContractExamples runs the golden files in contracts/examples when the
// directory exists (ADR 0014): valid files decode strictly and map to a domain
// event; invalid files are rejected by the schema, the decoder or the domain.
func TestContractExamples(t *testing.T) {
	t.Parallel()
	schema := loadSchema(t)
	dir := filepath.Join(contractsDir, "examples")
	if _, err := os.Stat(dir); err != nil {
		t.Skipf("no golden examples in %s", dir)
	}
	for _, kind := range []string{"valid", "invalid"} {
		files, _ := filepath.Glob(filepath.Join(dir, kind, "*.json"))
		for _, file := range files {
			t.Run(kind+"/"+filepath.Base(file), func(t *testing.T) {
				t.Parallel()
				raw, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				var doc any
				schemaOK := json.Unmarshal(raw, &doc) == nil && len(validateSchema(schema, doc, "$")) == 0
				dec := json.NewDecoder(bytes.NewReader(raw))
				dec.DisallowUnknownFields()
				var msg activityMessage
				decodeErr := dec.Decode(&msg)
				var domainErr error
				if decodeErr == nil {
					_, domainErr = domain.NewActivityEvent(msg.UserID, domain.ActivityType(msg.ActivityType), msg.Timestamp, msg.Metadata)
				}
				accepted := schemaOK && decodeErr == nil && domainErr == nil
				if kind == "valid" && !accepted {
					t.Errorf("valid example rejected: schema ok=%v decode=%v domain=%v", schemaOK, decodeErr, domainErr)
				}
				if kind == "invalid" && accepted {
					t.Error("invalid example was accepted")
				}
			})
		}
	}
}

// ecmaUnicodeEscape matches the \uXXXX escapes of an ECMAScript regular expression.
var ecmaUnicodeEscape = regexp.MustCompile(`\\u([0-9A-Fa-f]{4})`)

// validateSchema checks doc against the subset of JSON Schema 2020-12 that
// contracts/user_activity.v1.schema.json uses: type, required, properties,
// additionalProperties, propertyNames, minLength, maxLength, maxProperties,
// pattern and format date-time. It returns one message per violation. A full
// validator dependency is deliberately not added (ADR 0014).
func validateSchema(schema map[string]any, doc any, path string) []string {
	var out []string
	add := func(format string, args ...any) { out = append(out, path+": "+fmt.Sprintf(format, args...)) }

	switch schema["type"] {
	case "object":
		obj, ok := doc.(map[string]any)
		if !ok {
			add("want object, got %T", doc)
			return out
		}
		out = append(out, validateObject(schema, obj, path)...)
	case "string":
		s, ok := doc.(string)
		if !ok {
			add("want string, got %T", doc)
			return out
		}
		n := utf8.RuneCountInString(s)
		if v, ok := schema["minLength"].(float64); ok && float64(n) < v {
			add("length %d below minLength %v", n, v)
		}
		if v, ok := schema["maxLength"].(float64); ok && float64(n) > v {
			add("length %d above maxLength %v", n, v)
		}
		if p, ok := schema["pattern"].(string); ok {
			// The schema uses ECMAScript \uXXXX escapes; RE2 spells them \x{XXXX}.
			re, err := regexp.Compile(ecmaUnicodeEscape.ReplaceAllString(p, `\x{$1}`))
			if err != nil {
				add("schema pattern %q does not compile: %v", p, err)
			} else if !re.MatchString(s) {
				add("%q does not match pattern %s", s, p)
			}
		}
		if schema["format"] == "date-time" {
			if _, err := time.Parse(time.RFC3339, s); err != nil {
				add("%q is not an RFC 3339 date-time", s)
			}
		}
	}
	return out
}

func validateObject(schema, obj map[string]any, path string) []string {
	var out []string
	props, _ := schema["properties"].(map[string]any)
	if req, ok := schema["required"].([]any); ok {
		for _, r := range req {
			if _, present := obj[r.(string)]; !present {
				out = append(out, fmt.Sprintf("%s: missing required property %q", path, r))
			}
		}
	}
	if v, ok := schema["maxProperties"].(float64); ok && float64(len(obj)) > v {
		out = append(out, fmt.Sprintf("%s: %d properties above maxProperties %v", path, len(obj), v))
	}
	for _, name := range slices.Sorted(maps.Keys(obj)) {
		val := obj[name]
		if names, ok := schema["propertyNames"].(map[string]any); ok {
			named := map[string]any{"type": "string"}
			for k, v := range names {
				named[k] = v
			}
			out = append(out, validateSchema(named, name, path+"<name "+name+">")...)
		}
		if sub, ok := props[name].(map[string]any); ok {
			out = append(out, validateSchema(sub, val, path+"."+name)...)
			continue
		}
		switch ap := schema["additionalProperties"].(type) {
		case bool:
			if !ap {
				out = append(out, fmt.Sprintf("%s: additional property %q not allowed", path, name))
			}
		case map[string]any:
			out = append(out, validateSchema(ap, val, path+"."+name)...)
		}
	}
	return out
}
