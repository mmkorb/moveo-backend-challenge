package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
)

// maxBodyBytes is the request body limit (ADR 0005).
const maxBodyBytes = 64 << 10

// activityRequest is the HTTP request DTO. It mirrors the Kafka wire schema
// but is owned by this adapter: adapters never import each other. Timestamp
// stays a string so a bad value yields a field-level violation.
type activityRequest struct {
	UserID       string            `json:"user_id"`
	ActivityType string            `json:"activity_type"`
	Timestamp    *string           `json:"timestamp"`
	Metadata     map[string]string `json:"metadata"`
}

// requestError is a client mistake that maps to 400. Detail is a fixed text
// (never raw decoder output, which can quote input); Violations lists the
// broken field rules, if any.
type requestError struct {
	Detail     string
	Violations []domain.Violation
}

func (e *requestError) Error() string { return "invalid request: " + e.Detail }

// isJSON reports whether the Content-Type is application/json; parameters
// such as charset=utf-8 are allowed.
func isJSON(contentType string) bool {
	mt, _, err := mime.ParseMediaType(contentType)
	return err == nil && mt == "application/json"
}

// decodeActivity reads exactly one JSON object from body, rejects unknown
// fields and trailing data, and builds a valid domain.ActivityEvent. It
// returns a *requestError for client mistakes and passes through
// *http.MaxBytesError when the body is too large.
func decodeActivity(body io.Reader) (domain.ActivityEvent, error) {
	dec := json.NewDecoder(body)
	dec.DisallowUnknownFields()

	var req activityRequest
	if err := dec.Decode(&req); err != nil {
		return domain.ActivityEvent{}, classifyDecodeError(err)
	}
	var extra json.RawMessage
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		if tooLarge(err) {
			return domain.ActivityEvent{}, err
		}
		return domain.ActivityEvent{}, &requestError{Detail: "body must contain exactly one JSON object"}
	}

	// A missing timestamp is left zero so the domain reports it as required;
	// a malformed one is reported here, with a placeholder keeping the domain
	// from adding a second, misleading violation for the same field.
	var ts time.Time
	var tsViolation *domain.Violation
	if req.Timestamp != nil {
		parsed, err := time.Parse(time.RFC3339, *req.Timestamp)
		if err != nil {
			tsViolation = &domain.Violation{Field: domain.FieldTimestamp, Rule: "must be an RFC 3339 date-time"}
			parsed = time.Unix(1, 0)
		}
		ts = parsed
	}

	ev, err := domain.NewActivityEvent(req.UserID, domain.ActivityType(req.ActivityType), ts, req.Metadata)
	var verr *domain.ValidationError
	switch {
	case errors.As(err, &verr):
		violations := verr.Violations
		if tsViolation != nil {
			violations = append(violations, *tsViolation)
		}
		return domain.ActivityEvent{}, &requestError{Detail: "invalid activity event", Violations: violations}
	case err != nil:
		return domain.ActivityEvent{}, fmt.Errorf("build activity event: %w", err)
	case tsViolation != nil:
		return domain.ActivityEvent{}, &requestError{
			Detail:     "invalid activity event",
			Violations: []domain.Violation{*tsViolation},
		}
	}
	return ev, nil
}

func tooLarge(err error) bool {
	var mbe *http.MaxBytesError
	return errors.As(err, &mbe)
}

// classifyDecodeError maps decoder failures to client errors with fixed
// texts. It never echoes decoder messages: they can quote input bytes.
func classifyDecodeError(err error) error {
	if tooLarge(err) {
		return err
	}
	var ute *json.UnmarshalTypeError
	if errors.As(err, &ute) {
		field, _, _ := strings.Cut(ute.Field, ".")
		if field == "" {
			return &requestError{Detail: "body must be a JSON object"}
		}
		return &requestError{
			Detail:     "invalid activity event",
			Violations: []domain.Violation{{Field: field, Rule: "has the wrong JSON type"}},
		}
	}
	switch {
	case errors.Is(err, io.EOF):
		return &requestError{Detail: "body is empty"}
	case strings.Contains(err.Error(), "unknown field"):
		return &requestError{Detail: "body contains an unknown field"}
	default:
		return &requestError{Detail: "body is not valid JSON"}
	}
}
