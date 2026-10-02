package linklanding_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/linklanding"
)

const wantLandingBody = `<!doctype html><html lang="en"><body><main>Open this in Telegramd</main></body></html>`

func TestGETPathsReturnIdenticalStaticPageAndSecurityHeaders(t *testing.T) {
	t.Parallel()

	handler := linklanding.NewHandler(slog.New(slog.DiscardHandler))
	paths := []string{
		"/+example",
		"/+live-synthetic-capability",
		"/example",
		"/example/1",
		"/c/1/2",
		"/+%3Cscript%3E",
		"/+revoked-synthetic-capability",
		"/+random-synthetic-capability",
	}
	var first *httptest.ResponseRecorder
	for _, path := range paths {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		if response.Code != http.StatusOK {
			t.Errorf("GET %s status = %d, want %d", path, response.Code, http.StatusOK)
		}
		if got := response.Body.String(); got != wantLandingBody {
			t.Errorf("GET %s body = %q, want static landing page", path, got)
		}
		assertSecurityHeaders(t, response)
		assertNoSensitiveResponseHeaders(t, response)
		if first == nil {
			first = response
			continue
		}
		if !reflect.DeepEqual(headersWithoutDate(response.Header()), headersWithoutDate(first.Header())) {
			t.Errorf("GET %s headers differ from the first landing response", path)
		}
	}
}

func TestHEADMirrorsGETStatusAndHeadersWithoutBody(t *testing.T) {
	t.Parallel()

	handler := linklanding.NewHandler(slog.New(slog.DiscardHandler))
	for _, path := range []string{"/+example", "/example/1", "/c/1/2", "/admin", "/admin/events"} {
		get := httptest.NewRecorder()
		handler.ServeHTTP(get, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))

		head := httptest.NewRecorder()
		handler.ServeHTTP(head, httptest.NewRequestWithContext(context.Background(), http.MethodHead, path, nil))
		if head.Code != get.Code {
			t.Errorf("HEAD %s status = %d, GET status = %d", path, head.Code, get.Code)
		}
		if !reflect.DeepEqual(headersWithoutDate(head.Header()), headersWithoutDate(get.Header())) {
			t.Errorf("HEAD %s headers differ from GET", path)
		}
		if head.Body.Len() != 0 {
			t.Errorf("HEAD %s body = %q, want empty", path, head.Body.String())
		}
	}
}

func TestUnsupportedMethodsReturnGeneric405(t *testing.T) {
	t.Parallel()

	handler := linklanding.NewHandler(slog.New(slog.DiscardHandler))
	var first *httptest.ResponseRecorder
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), method, "/+synthetic-capability", nil))
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s status = %d, want %d", method, response.Code, http.StatusMethodNotAllowed)
		}
		assertSecurityHeaders(t, response)
		assertNoSensitiveResponseHeaders(t, response)
		if strings.Contains(response.Body.String(), "synthetic-capability") {
			t.Errorf("%s response echoed the request path", method)
		}
		if first == nil {
			first = response
			continue
		}
		if response.Body.String() != first.Body.String() {
			t.Errorf("%s body differs from the generic method response", method)
		}
		if !reflect.DeepEqual(headersWithoutDate(response.Header()), headersWithoutDate(first.Header())) {
			t.Errorf("%s headers differ from the generic method response", method)
		}
	}
}

func TestAdminPathsReturnStatic404(t *testing.T) {
	t.Parallel()

	handler := linklanding.NewHandler(slog.New(slog.DiscardHandler))
	for _, path := range []string{"/admin", "/admin/", "/admin/events", "/admin/anything/deep"} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequestWithContext(context.Background(), http.MethodGet, path, nil))
		if response.Code != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want %d", path, response.Code, http.StatusNotFound)
		}
		assertSecurityHeaders(t, response)
		assertNoSensitiveResponseHeaders(t, response)
		if strings.Contains(strings.ToLower(response.Body.String()), "admin") {
			t.Errorf("GET %s returned admin content: %q", path, response.Body.String())
		}
	}
}

func TestHandlerLogsOnlyRouteClassAndStatus(t *testing.T) {
	var logs bytes.Buffer
	handler := linklanding.NewHandler(slog.New(slog.NewJSONHandler(&logs, nil)))
	cases := []struct {
		method, target, referer, routeClass string
		status                              int
	}{
		{http.MethodGet, "/+synthetic-invite-hash?secret=query-secret", "https://referer-secret.example/", "invite", http.StatusOK},
		{http.MethodGet, "/synthetic-username-secret/42?secret=query-secret", "https://referer-secret.example/", "message", http.StatusOK},
		{http.MethodPost, "/username-secret?token=query-secret", "https://referer-secret.example/", "username", http.StatusMethodNotAllowed},
		{http.MethodGet, "/admin/private-route?secret=query-secret", "https://referer-secret.example/", "admin", http.StatusNotFound},
	}
	for _, tc := range cases {
		request := httptest.NewRequestWithContext(context.Background(), tc.method, tc.target, nil)
		request.Header.Set("Referer", tc.referer)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != tc.status {
			t.Errorf("%s %s status = %d, want %d", tc.method, tc.target, response.Code, tc.status)
		}
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != len(cases) {
		t.Fatalf("log lines = %d, want %d: %q", len(lines), len(cases), logs.String())
	}
	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line %d: %v", i, err)
		}
		if record["route_class"] != cases[i].routeClass {
			t.Errorf("log line %d route_class = %v, want %q", i, record["route_class"], cases[i].routeClass)
		}
		if record["status"] != float64(cases[i].status) {
			t.Errorf("log line %d status = %v, want %d", i, record["status"], cases[i].status)
		}
		for _, key := range []string{"time", "level", "msg", "route_class", "status"} {
			if _, ok := record[key]; !ok {
				t.Errorf("log line %d missing %q: %v", i, key, record)
			}
		}
		if len(record) != 5 {
			t.Errorf("log line %d fields = %v, want only standard fields, route_class, and status", i, record)
		}
	}
	for _, marker := range []string{
		"synthetic-invite-hash",
		"synthetic-username-secret",
		"username-secret",
		"private-route",
		"query-secret",
		"referer-secret",
	} {
		if strings.Contains(logs.String(), marker) {
			t.Errorf("logs contain request data %q: %s", marker, logs.String())
		}
	}
}

func TestHandlerLogsBodyWriteFailureAtErrorLevelWithoutSensitiveData(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	handler := linklanding.NewHandler(slog.New(slog.NewJSONHandler(&logs, nil)))
	request := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/+synthetic-invite?secret=query-secret", nil)
	request.Header.Set("Referer", "https://referer-secret.example/path")
	response := &failingResponseWriter{header: make(http.Header)}

	handler.ServeHTTP(response, request)
	if response.status != http.StatusOK {
		t.Errorf("response status = %d, want %d", response.status, http.StatusOK)
	}
	if response.writes != 1 {
		t.Fatalf("body writes = %d, want one failing write", response.writes)
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 1 {
		t.Fatalf("log lines = %d, want one error event: %q", len(lines), logs.String())
	}
	var record map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("decode log event: %v", err)
	}
	if record["level"] != "ERROR" {
		t.Errorf("log level = %v, want ERROR", record["level"])
	}
	if record["msg"] == "landing response" {
		t.Errorf("body write failure was logged as a successful response: %v", record)
	}
	if record["route_class"] != "invite" {
		t.Errorf("route_class = %v, want invite", record["route_class"])
	}
	if record["status"] != float64(http.StatusOK) {
		t.Errorf("status = %v, want %d", record["status"], http.StatusOK)
	}
	if len(record) != 5 {
		t.Errorf("error event fields = %v, want standard fields plus route_class and status", record)
	}
	for _, marker := range []string{"synthetic-invite", "query-secret", "referer-secret", "body-write-secret"} {
		if strings.Contains(logs.String(), marker) {
			t.Errorf("error event contains sensitive value %q: %s", marker, logs.String())
		}
	}
}

type failingResponseWriter struct {
	header http.Header
	status int
	writes int
}

func (w *failingResponseWriter) Header() http.Header {
	return w.header
}

func (w *failingResponseWriter) WriteHeader(status int) {
	w.status = status
}

func (w *failingResponseWriter) Write([]byte) (int, error) {
	w.writes++
	return 0, errors.New("body-write-secret")
}

func assertSecurityHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	want := http.Header{
		"Cache-Control":           {"no-store"},
		"Referrer-Policy":         {"no-referrer"},
		"X-Content-Type-Options":  {"nosniff"},
		"Content-Type":            {"text/html; charset=utf-8"},
		"Content-Security-Policy": {"default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"},
	}
	for name, values := range want {
		if got := response.Header().Values(name); !reflect.DeepEqual(got, values) {
			t.Errorf("header %s = %q, want %q", name, got, values)
		}
	}
}

func assertNoSensitiveResponseHeaders(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	for _, name := range []string{"Location", "Set-Cookie"} {
		if values := response.Header().Values(name); len(values) != 0 {
			t.Errorf("response has forbidden %s header: %q", name, values)
		}
	}
}

func headersWithoutDate(headers http.Header) http.Header {
	cloned := headers.Clone()
	cloned.Del("Date")
	return cloned
}
