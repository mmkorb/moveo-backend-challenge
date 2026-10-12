package httpapi

import (
	"encoding/json"
	"net/http"

	"github.com/mmkorb/moveo-backend-challenge/services/producer/internal/domain"
)

// problem is an RFC 9457 problem details document. It never carries internal
// error text, stack traces or infrastructure addresses.
type problem struct {
	Type   string         `json:"type"`
	Title  string         `json:"title"`
	Status int            `json:"status"`
	Detail string         `json:"detail,omitempty"`
	Errors []problemField `json:"errors,omitempty"`
}

type problemField struct {
	Field string `json:"field"`
	Rule  string `json:"rule"`
}

// genericDetail is the response text for statuses that carry no
// request-specific information.
func genericDetail(status int) string {
	switch status {
	case http.StatusBadRequest:
		return "invalid activity event"
	case http.StatusRequestEntityTooLarge:
		return "request body exceeds the 64 KiB limit"
	case http.StatusUnsupportedMediaType:
		return "Content-Type must be application/json"
	case http.StatusServiceUnavailable:
		return "the event could not be published; retry shortly"
	default:
		return "internal server error"
	}
}

// writeProblem writes an application/problem+json response. detail may be ""
// to use the generic text of the status.
func writeProblem(w http.ResponseWriter, status int, detail string, violations []domain.Violation) {
	if detail == "" {
		detail = genericDetail(status)
	}
	p := problem{
		Type:   "about:blank",
		Title:  http.StatusText(status),
		Status: status,
		Detail: detail,
	}
	for _, v := range violations {
		p.Errors = append(p.Errors, problemField{Field: v.Field, Rule: v.Rule})
	}
	body, err := json.Marshal(p)
	if err != nil { // cannot happen for this struct; stay safe anyway
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/problem+json")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// writeJSON writes v as an application/json response.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "", nil)
		return
	}
	h := w.Header()
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}
