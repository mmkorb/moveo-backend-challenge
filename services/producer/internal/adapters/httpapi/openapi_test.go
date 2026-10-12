package httpapi

import (
	"context"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// specRoute is one documented operation: method, path and status codes.
type specRoute struct {
	method, path string
	statuses     map[int]bool
}

var (
	pathLine   = regexp.MustCompile(`^  (/\S*):\s*$`)
	methodLine = regexp.MustCompile(`^    (get|post|put|delete|patch):\s*$`)
	statusLine = regexp.MustCompile(`^        "?(\d{3})"?:\s*$`)
)

// readSpec extracts the paths section of api/openapi.yaml with a line scan.
// The file is hand-written in a fixed layout, which avoids a YAML dependency.
func readSpec(t *testing.T) []specRoute {
	t.Helper()
	raw, err := os.ReadFile("../../../api/openapi.yaml")
	if err != nil {
		t.Fatalf("read openapi.yaml: %v", err)
	}
	var routes []specRoute
	var cur *specRoute
	inResponses := false
	var path string
	for line := range strings.Lines(string(raw)) {
		line = strings.TrimRight(line, "\r\n")
		if strings.HasPrefix(line, "components:") {
			break
		}
		if m := pathLine.FindStringSubmatch(line); m != nil {
			path, cur, inResponses = m[1], nil, false
			continue
		}
		if m := methodLine.FindStringSubmatch(line); m != nil {
			routes = append(routes, specRoute{method: strings.ToUpper(m[1]), path: path, statuses: map[int]bool{}})
			cur, inResponses = &routes[len(routes)-1], false
			continue
		}
		if cur == nil {
			continue
		}
		if strings.TrimSpace(line) == "responses:" {
			inResponses = true
			continue
		}
		if m := statusLine.FindStringSubmatch(line); m != nil && inResponses {
			code, _ := strconv.Atoi(m[1])
			cur.statuses[code] = true
		}
	}
	if len(routes) == 0 {
		t.Fatal("no operations found in openapi.yaml")
	}
	return routes
}

func TestOpenAPIDocumentsEveryRoute(t *testing.T) {
	t.Parallel()
	f := newFixture(t, nil)
	routes := readSpec(t)

	want := map[string]bool{
		"POST /v1/activities": false, "GET /healthz": false, "GET /readyz": false, "GET /metrics": false,
	}
	for _, r := range routes {
		key := r.method + " " + r.path
		if _, ok := want[key]; !ok {
			t.Errorf("spec documents %s, which the router does not serve", key)
			continue
		}
		want[key] = true
		// The router must serve it: anything but 404/405 proves the route exists.
		rec := f.do(r.method, r.path, "application/json", validBody)
		if rec.Code == http.StatusNotFound || rec.Code == http.StatusMethodNotAllowed {
			t.Errorf("%s: router answered %d", key, rec.Code)
		}
	}
	for key, documented := range want {
		if !documented {
			t.Errorf("router serves %s but the spec does not document it", key)
		}
	}
}

func TestOpenAPIDocumentsEveryStatusTheRouterReturns(t *testing.T) {
	t.Parallel()
	spec := map[string]map[int]bool{}
	for _, r := range readSpec(t) {
		spec[r.method+" "+r.path] = r.statuses
	}
	tooBig := `{"metadata":{"k":"` + strings.Repeat("x", maxBodyBytes) + `"}}`
	tests := []struct {
		route, contentType, body string
		registrarErr             error
		ready                    bool
		want                     int
	}{
		{"POST /v1/activities", "application/json", validBody, nil, true, http.StatusAccepted},
		{"POST /v1/activities", "application/json", `{`, nil, true, http.StatusBadRequest},
		{"POST /v1/activities", "application/json", tooBig, nil, true, http.StatusRequestEntityTooLarge},
		{"POST /v1/activities", "text/plain", validBody, nil, true, http.StatusUnsupportedMediaType},
		{"POST /v1/activities", "application/json", validBody, joinUnavailable("x"), true, http.StatusServiceUnavailable},
		{"POST /v1/activities", "application/json", validBody, errBoom, true, http.StatusInternalServerError},
		{"GET /healthz", "", "", nil, true, http.StatusOK},
		{"GET /readyz", "", "", nil, true, http.StatusOK},
		{"GET /readyz", "", "", nil, false, http.StatusServiceUnavailable},
		{"GET /metrics", "", "", nil, true, http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.route+" -> "+strconv.Itoa(tt.want), func(t *testing.T) {
			t.Parallel()
			f := newFixture(t, nil)
			if !tt.ready {
				f = newFixture(t, notReady)
			}
			f.reg.err = tt.registrarErr
			method, path, _ := strings.Cut(tt.route, " ")

			rec := f.do(method, path, tt.contentType, tt.body)

			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
			if !spec[tt.route][rec.Code] {
				t.Errorf("status %d of %s is not documented in openapi.yaml", rec.Code, tt.route)
			}
		})
	}
}

var errBoom = errors.New("boom")

func notReady(context.Context) error { return errors.New("kafka down") }
