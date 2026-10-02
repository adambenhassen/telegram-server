package linkselector_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/linklanding"
	"github.com/adambenhassen/telegram-server/internal/linkselector"
)

const (
	landingBody = `<!doctype html><html lang="en"><body><main>Open this in Telegramd</main></body></html>`
	webBody     = "web-resource"
)

type observedRequest struct {
	method, requestURI string
	header             http.Header
	body               string
}

func TestAllowlistedWebResourcesAndLinkCollisions(t *testing.T) {
	var webRequests, landingRequests []observedRequest
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webRequests = append(webRequests, observeRequest(t, r))
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		w.Header().Set("Content-Type", "application/octet-stream")
		writeTestResponse(t, w, webBody)
	}))
	t.Cleanup(web.Close)
	landing := httptest.NewServer(recordingLanding(t, &landingRequests))
	t.Cleanup(landing.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	webTargets := []string{
		"/",
		"/?version=2",
		"/main.abc123.js",
		"/app.abc.js.map",
		"/assets/img/example@2x.png",
		"/changelogs/lang_v1.md",
		"/.well-known/telegram-web/version.txt",
	}
	for _, target := range webTargets {
		response := serveTarget(handler, http.MethodGet, target, "")
		if response.Code != http.StatusOK || response.Body.String() != webBody {
			t.Errorf("GET %s = %d %q, want Web response", target, response.Code, response.Body.String())
		}
	}
	if got, want := len(webRequests), len(webTargets); got != want {
		t.Fatalf("Web requests = %d, want %d", got, want)
	}
	if got, want := len(landingRequests), 0; got != want {
		t.Fatalf("landing requests for Web targets = %d, want %d", got, want)
	}
	if got := webRequests[1].requestURI; got != "/?version=2" {
		t.Errorf("Web query target = %q, want the original query", got)
	}
	if got := webRequests[4].requestURI; got != "/assets/img/example@2x.png" {
		t.Errorf("Web asset target = %q, want unchanged canonical path", got)
	}
	if got := webRequests[6].requestURI; got != "/.well-known/telegram-web/version.txt" {
		t.Errorf("Web version target = %q, want unchanged path", got)
	}
	for _, request := range webRequests {
		if request.header.Get("Content-Security-Policy") != "" {
			t.Errorf("incoming request unexpectedly has CSP header: %v", request.header)
		}
	}

	for _, target := range []string{"/assets/5", "/changelogs/5", "/healthz", "/version", "/assets", "/changelogs"} {
		response := serveTarget(handler, http.MethodGet, target, "")
		if response.Code != http.StatusOK || response.Body.String() != landingBody {
			t.Errorf("GET %s = %d %q, want landing response", target, response.Code, response.Body.String())
		}
	}
	if got, want := len(webRequests), len(webTargets); got != want {
		t.Errorf("link collision reached Web: requests = %d, want %d", got, want)
	}
}

func TestThreatModelNegativeMatrixNeverReachesWeb(t *testing.T) {
	var webRequests, landingRequests []observedRequest
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webRequests = append(webRequests, observeRequest(t, r))
		writeTestResponse(t, w, webBody)
	}))
	t.Cleanup(web.Close)
	landing := httptest.NewServer(recordingLanding(t, &landingRequests))
	t.Cleanup(landing.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	targets := []string{
		"/+synthetic-capability",
		"/%2Bsynthetic-capability",
		"/%2bsynthetic-capability",
		"/+synthetic-capability/",
		"/+synthetic-capability.",
		"/+synthetic-capability?x=synthetic-capability",
		"/assets/../+synthetic-capability",
		"/assets/%2E%2E/%2Bsynthetic-capability",
		"//+synthetic-capability",
		"/apiws/../+synthetic-capability",
		"/assets/..%2F+synthetic-capability",
		"/assets/..%2F+synthetic-capability?x=1",
		"/assets/img/",
		"/foo.js/",
		"/assets/%2e%2e/logo.svg",
		"/assets/a\\b.svg",
	}
	for _, target := range targets {
		request := rawRequest(http.MethodGet, target, "")
		if strings.Contains(target, "..%2F+") && strings.HasSuffix(target, "?x=1") {
			request.Header.Set("Referer", "https://referer.example/+synthetic-capability")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusOK || response.Body.String() != landingBody {
			t.Errorf("GET %s = %d %q, want the same fixed landing response", target, response.Code, response.Body.String())
		}
	}
	if len(webRequests) != 0 {
		t.Errorf("Web received disallowed paths: %+v", webRequests)
	}
	if len(landingRequests) != len(targets) {
		t.Fatalf("landing requests = %d, want %d", len(landingRequests), len(targets))
	}
	if landingRequests[0].body != landingRequests[1].body {
		t.Errorf("literal and encoded plus routes returned different landing bodies")
	}
}

func TestLandingForwardingDropsRequestDataAndUsesCanonicalPath(t *testing.T) {
	var landingRequests []observedRequest
	landing := httptest.NewServer(recordingLanding(t, &landingRequests))
	t.Cleanup(landing.Close)
	web := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Error("link request unexpectedly reached Web")
	}))
	t.Cleanup(web.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	request := rawRequest(http.MethodPost, "/+synthetic-capability?secret=query-secret", "secret request body")
	request.Header.Set("Referer", "https://referer.example/+synthetic-capability")
	request.Header.Set("Cookie", "session=secret-cookie")
	request.Header.Set("Authorization", "Bearer secret-token")
	request.Header.Set("Tailscale-User-Login", "private-user@example.test")
	request.Header.Set("Tailscale-User-Name", "Private User")
	request.Header.Set("X-Other-Secret", "must-not-forward")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", response.Code)
	}
	if len(landingRequests) != 1 {
		t.Fatalf("landing requests = %d, want one", len(landingRequests))
	}
	got := landingRequests[0]
	if got.method != http.MethodPost || got.requestURI != "/+redacted" {
		t.Errorf("landing received %s %q, want POST /+redacted", got.method, got.requestURI)
	}
	if got.body != "" {
		t.Errorf("landing received request body %q", got.body)
	}
	for name, values := range got.header {
		if name != "Content-Length" || !reflect.DeepEqual(values, []string{"0"}) {
			t.Errorf("landing received application header %s=%q", name, values)
		}
	}
	if got := response.Header().Get("Allow"); got != "GET, HEAD" {
		t.Errorf("Allow = %q, want GET, HEAD", got)
	}
	assertNoRedirectOrCookie(t, response)
}

func TestUsernameAndMessageLandingPathsAreRedacted(t *testing.T) {
	var landingRequests, webRequests []observedRequest
	landing := httptest.NewServer(recordingLanding(t, &landingRequests))
	t.Cleanup(landing.Close)
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webRequests = append(webRequests, observeRequest(t, r))
		writeTestResponse(t, w, webBody)
	}))
	t.Cleanup(web.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	for _, tc := range []struct {
		target, wantLandingPath string
	}{
		{target: "/syntheticname", wantLandingPath: "/redacted"},
		{target: "/syntheticname/12345", wantLandingPath: "/redacted/1"},
		{target: "/c/12345/67", wantLandingPath: "/redacted/1"},
	} {
		landingRequestsBefore := len(landingRequests)
		request := rawRequest(http.MethodGet, tc.target+"?secret=query-secret", "secret request body")
		request.Header.Set("Referer", "https://referer.example/"+tc.target)
		request.Header.Set("Cookie", "session=secret-cookie")
		request.Header.Set("Authorization", "Bearer secret-token")
		request.Header.Set("Tailscale-User-Login", "private-user@example.test")
		request.Header.Set("Tailscale-User-Name", "Private User")
		request.Header.Set("X-Other-Secret", "must-not-forward")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)

		if response.Code != http.StatusOK || response.Body.String() != landingBody {
			t.Errorf("GET %s = %d %q, want fixed landing response", tc.target, response.Code, response.Body.String())
		}
		if len(landingRequests) != landingRequestsBefore+1 {
			t.Errorf("GET %s added %d landing requests, want one", tc.target, len(landingRequests)-landingRequestsBefore)
			continue
		}
		got := landingRequests[landingRequestsBefore]
		if got.method != http.MethodGet || got.requestURI != tc.wantLandingPath {
			t.Errorf("landing received %s %q, want GET %q", got.method, got.requestURI, tc.wantLandingPath)
		}
		if got.body != "" {
			t.Errorf("landing received request body %q", got.body)
		}
		for name, values := range got.header {
			if name != "Content-Length" || !reflect.DeepEqual(values, []string{"0"}) {
				t.Errorf("landing received application header %s=%q", name, values)
			}
		}
	}
	if len(landingRequests) != 3 {
		t.Errorf("landing requests = %d, want 3", len(landingRequests))
	}
	if len(webRequests) != 0 {
		t.Errorf("username/message links reached Web: %+v", webRequests)
	}
}

func TestLandingMethodsAndAdminIsolation(t *testing.T) {
	var landingRequests []observedRequest
	var webRequests []observedRequest
	landing := httptest.NewServer(recordingLanding(t, &landingRequests))
	t.Cleanup(landing.Close)
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webRequests = append(webRequests, observeRequest(t, r))
		writeTestResponse(t, w, webBody)
	}))
	t.Cleanup(web.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	get := serveTarget(handler, http.MethodGet, "/+invite", "")
	head := serveTarget(handler, http.MethodHead, "/+invite", "")
	if head.Code != get.Code || head.Body.Len() != 0 {
		t.Errorf("HEAD = %d body %q, GET = %d; expected matching status and empty body", head.Code, head.Body.String(), get.Code)
	}
	if head.Header().Get("Content-Length") != get.Header().Get("Content-Length") {
		t.Errorf("HEAD Content-Length = %q, GET = %q", head.Header().Get("Content-Length"), get.Header().Get("Content-Length"))
	}
	for _, method := range []string{http.MethodPost, http.MethodOptions, http.MethodConnect} {
		target := "/+invite"
		if method == http.MethodOptions {
			target = "*"
		}
		response := serveTarget(handler, method, target, "ignored body")
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("%s status = %d, want 405", method, response.Code)
		}
		if got := response.Header().Get("Allow"); got != "GET, HEAD" {
			t.Errorf("%s Allow = %q, want GET, HEAD", method, got)
		}
		assertNoRedirectOrCookie(t, response)
	}
	for _, target := range []string{"/", "upstream.example:443"} {
		response := serveTarget(handler, http.MethodConnect, target, "ignored body")
		if response.Code != http.StatusMethodNotAllowed {
			t.Errorf("CONNECT %s status = %d, want landing 405", target, response.Code)
		}
	}
	for _, target := range []string{"/admin", "/admin/", "/admin/events"} {
		response := serveTarget(handler, http.MethodGet, target, "")
		if response.Code != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want static 404", target, response.Code)
		}
		if strings.Contains(strings.ToLower(response.Body.String()), "admin") {
			t.Errorf("GET %s exposed admin response %q", target, response.Body.String())
		}
		assertNoRedirectOrCookie(t, response)
	}
	if len(webRequests) != 0 {
		t.Errorf("landing or admin targets reached Web: %+v", webRequests)
	}
}

func TestWeb404DoesNotChangeRouteAndCSPPassesThrough(t *testing.T) {
	var landingRequests []observedRequest
	landing := httptest.NewServer(recordingLanding(t, &landingRequests))
	t.Cleanup(landing.Close)
	var webRequests []observedRequest
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		webRequests = append(webRequests, observeRequest(t, r))
		w.Header().Set("Content-Security-Policy", "default-src 'self'; object-src 'none'")
		http.NotFound(w, r)
	}))
	t.Cleanup(web.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	response := serveTarget(handler, http.MethodGet, "/missing.js", "")
	if response.Code != http.StatusNotFound {
		t.Fatalf("Web 404 status = %d, want unchanged 404", response.Code)
	}
	if got := response.Header().Get("Content-Security-Policy"); got != "default-src 'self'; object-src 'none'" {
		t.Errorf("CSP = %q, want Web policy unchanged", got)
	}
	if len(webRequests) != 1 || len(landingRequests) != 0 {
		t.Errorf("route changed based on Web status: Web=%d landing=%d", len(webRequests), len(landingRequests))
	}
}

func TestWebHTTPFailureReturnsFixed502WithoutUpstreamBody(t *testing.T) {
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "upstream-error-secret", http.StatusInternalServerError)
	}))
	t.Cleanup(web.Close)
	landing := httptest.NewServer(recordingLanding(t, nil))
	t.Cleanup(landing.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	response := serveTarget(handler, http.MethodGet, "/main.js", "")
	if response.Code != http.StatusBadGateway {
		t.Fatalf("Web 500 status = %d, want fixed 502", response.Code)
	}
	if strings.Contains(response.Body.String(), "upstream-error-secret") {
		t.Errorf("Web error body was echoed: %q", response.Body.String())
	}
	if response.Body.String() != "<!doctype html><html lang=\"en\"><body><main>Temporarily unavailable</main></body></html>" {
		t.Errorf("Web error body = %q, want fixed unavailable page", response.Body.String())
	}
	assertSelectorSecurityHeaders(t, response)
	assertNoRedirectOrCookie(t, response)
}

func TestWebTruncatedContentLengthReturnsFixed502(t *testing.T) {
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "100")
		w.Header().Set("Content-Type", "text/javascript")
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		writeTestResponse(t, w, "partial-web-response")
	}))
	t.Cleanup(web.Close)
	landing := httptest.NewServer(recordingLanding(t, nil))
	t.Cleanup(landing.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	response := serveTarget(handler, http.MethodGet, "/main.js", "")
	if response.Code != http.StatusBadGateway {
		t.Fatalf("truncated Web response status = %d, want fixed 502", response.Code)
	}
	if response.Body.String() != "<!doctype html><html lang=\"en\"><body><main>Temporarily unavailable</main></body></html>" {
		t.Errorf("truncated Web response body = %q, want fixed unavailable page", response.Body.String())
	}
	if response.Header().Get("Content-Type") == "text/javascript" || response.Header().Get("Content-Length") == "100" {
		t.Errorf("truncated Web headers were forwarded: %v", response.Header())
	}
	assertSelectorSecurityHeaders(t, response)
	assertNoRedirectOrCookie(t, response)
}

func TestUpstreamFailuresAreFixedAndLogsContainOnlyClassAndStatus(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&logs, nil))
	web := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	landing := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	webURL, landingURL := web.URL, landing.URL
	web.Close()
	landing.Close()
	handler := newSelector(t, webURL, landingURL, logger)

	for _, tc := range []struct {
		target string
		status int
	}{
		{target: "/+synthetic-capability?secret=query-secret", status: http.StatusServiceUnavailable},
		{target: "/main.js", status: http.StatusBadGateway},
	} {
		response := serveTarget(handler, http.MethodGet, tc.target, "")
		if response.Code != tc.status {
			t.Errorf("GET %s status = %d, want %d", tc.target, response.Code, tc.status)
		}
		if response.Body.String() != "<!doctype html><html lang=\"en\"><body><main>Temporarily unavailable</main></body></html>" {
			t.Errorf("GET %s body = %q, want fixed unavailable page", tc.target, response.Body.String())
		}
		assertSelectorSecurityHeaders(t, response)
		assertNoRedirectOrCookie(t, response)
	}

	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("log lines = %d, want two: %q", len(lines), logs.String())
	}
	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("decode log line: %v", err)
		}
		if record["route_class"] != "unavailable" {
			t.Errorf("log line %d route_class = %v, want unavailable", i, record["route_class"])
		}
		if len(record) != 5 {
			t.Errorf("log line %d contains unexpected fields: %v", i, record)
		}
	}
	for _, marker := range []string{"synthetic-capability", "query-secret", webURL, landingURL, "connection refused"} {
		if strings.Contains(logs.String(), marker) {
			t.Errorf("logs contain sensitive request or proxy data %q: %s", marker, logs.String())
		}
	}
}

func TestCanceledUpstreamOperationReturnsFixedFailure(t *testing.T) {
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	t.Cleanup(web.Close)
	landing := httptest.NewServer(recordingLanding(t, nil))
	t.Cleanup(landing.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	request := rawRequest(http.MethodGet, "/app.js", "").WithContext(ctx)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadGateway {
		t.Errorf("canceled Web operation status = %d, want 502", response.Code)
	}
	assertSelectorSecurityHeaders(t, response)
}

func TestNewHandlerRejectsUpstreamsWithRequestData(t *testing.T) {
	for _, badURL := range []string{"", "/relative", "ftp://web.example", "http://user:pass@web.example", "http://web.example/base", "http://web.example/?secret=1", "http://web.example/#fragment"} {
		if _, err := linkselector.NewHandler(badURL, "http://landing:8082", slog.New(slog.DiscardHandler)); err == nil {
			t.Errorf("NewHandler accepted unsafe Web upstream %q", badURL)
		}
	}
	if _, err := linkselector.NewHandler("http://web:8080/", "http://landing:8082", nil); err != nil {
		t.Errorf("NewHandler rejected plain service origins: %v", err)
	}
}

func TestSelectorSmokeHappyPath(t *testing.T) {
	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Security-Policy", "default-src 'self'")
		writeTestResponse(t, w, "smoke-web-resource")
	}))
	t.Cleanup(web.Close)
	landing := httptest.NewServer(linklanding.NewHandler(slog.New(slog.DiscardHandler)))
	t.Cleanup(landing.Close)
	handler := newSelector(t, web.URL, landing.URL, slog.New(slog.DiscardHandler))

	webResponse := serveTarget(handler, http.MethodGet, "/assets/app.abc.js", "")
	if webResponse.Code != http.StatusOK || webResponse.Body.String() != "smoke-web-resource" {
		t.Fatalf("Web smoke response = %d %q", webResponse.Code, webResponse.Body.String())
	}
	if webResponse.Header().Get("Content-Security-Policy") != "default-src 'self'" {
		t.Fatalf("Web smoke CSP = %q", webResponse.Header().Get("Content-Security-Policy"))
	}
	landingResponse := serveTarget(handler, http.MethodGet, "/+synthetic-smoke-capability", "")
	if landingResponse.Code != http.StatusOK || landingResponse.Body.String() != landingBody {
		t.Fatalf("landing smoke response = %d %q", landingResponse.Code, landingResponse.Body.String())
	}
}

func newSelector(t *testing.T, webURL, landingURL string, logger *slog.Logger) http.Handler {
	t.Helper()
	handler, err := linkselector.NewHandler(webURL, landingURL, logger)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func recordingLanding(t *testing.T, requests *[]observedRequest) http.Handler {
	t.Helper()
	landing := linklanding.NewHandler(slog.New(slog.DiscardHandler))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests != nil {
			*requests = append(*requests, observeRequest(t, r))
		}
		landing.ServeHTTP(w, r)
	})
}

func observeRequest(t *testing.T, r *http.Request) observedRequest {
	t.Helper()
	body, err := io.ReadAll(r.Body)
	if err != nil {
		t.Errorf("read captured request body: %v", err)
	}
	return observedRequest{
		method:     r.Method,
		requestURI: r.RequestURI,
		header:     r.Header.Clone(),
		body:       string(body),
	}
}

func writeTestResponse(t *testing.T, w io.Writer, body string) {
	t.Helper()
	if _, err := io.WriteString(w, body); err != nil {
		t.Errorf("write test response: %v", err)
	}
}

func rawRequest(method, target, body string) *http.Request {
	return &http.Request{
		Method:        method,
		URL:           &url.URL{},
		RequestURI:    target,
		Header:        make(http.Header),
		Body:          io.NopCloser(strings.NewReader(body)),
		ContentLength: int64(len(body)),
	}
}

func serveTarget(handler http.Handler, method, target, body string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, rawRequest(method, target, body))
	return response
}

func assertNoRedirectOrCookie(t *testing.T, response *httptest.ResponseRecorder) {
	t.Helper()
	for _, header := range []string{"Location", "Set-Cookie"} {
		if values := response.Header().Values(header); len(values) > 0 {
			t.Errorf("response contains forbidden %s: %q", header, values)
		}
	}
}

func assertSelectorSecurityHeaders(t *testing.T, response *httptest.ResponseRecorder) {
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
