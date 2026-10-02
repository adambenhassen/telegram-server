package linkselector

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/adambenhassen/telegram-server/internal/linklanding"
)

const (
	upstreamTimeout       = 15 * time.Second
	upstreamDialTimeout   = 5 * time.Second
	maxLandingBodyBytes   = 4096
	maxWebBodyBytes       = 32 << 20 // Bound the staged response size per request.
	landingUnavailable    = `<!doctype html><html lang="en"><body><main>Temporarily unavailable</main></body></html>`
	webErrorStatus        = http.StatusBadGateway
	landingErrorStatus    = http.StatusServiceUnavailable
	defaultRouteClass     = "landing_other"
	inviteRouteClass      = "invite"
	usernameRouteClass    = "username"
	messageRouteClass     = "message"
	webRouteClass         = "web"
	unavailableRouteClass = "unavailable"
)

var (
	rootFilePattern = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_@-]+)+$`)
	assetPart       = regexp.MustCompile(`^[A-Za-z0-9_.@-]+$`)
	usernamePart    = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]{1,31}$`)
	messageIDPart   = regexp.MustCompile(`^[0-9]+$`)
	invitePart      = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
)

type selector struct {
	webURL     *url.URL
	landingURL *url.URL
	client     *http.Client
	logger     *slog.Logger
}

// NewHandler builds a selector that forwards only allowlisted Web paths and
// sends every other request to the isolated landing service.
func NewHandler(webUpstream, landingUpstream string, logger *slog.Logger) (http.Handler, error) {
	webURL, err := parseUpstream(webUpstream)
	if err != nil {
		return nil, errors.New("invalid Web upstream")
	}
	landingURL, err := parseUpstream(landingUpstream)
	if err != nil {
		return nil, errors.New("invalid landing upstream")
	}
	if logger == nil {
		logger = slog.Default()
	}

	dialer := &net.Dialer{Timeout: upstreamDialTimeout, KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            dialer.DialContext,
		MaxIdleConns:           8,
		MaxIdleConnsPerHost:    4,
		MaxConnsPerHost:        8,
		MaxResponseHeaderBytes: 8192,
		IdleConnTimeout:        30 * time.Second,
		TLSHandshakeTimeout:    upstreamDialTimeout,
		ResponseHeaderTimeout:  upstreamDialTimeout,
		ExpectContinueTimeout:  time.Second,
		DisableCompression:     true,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   upstreamTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &selector{webURL: webURL, landingURL: landingURL, client: client, logger: logger}, nil
}

func parseUpstream(raw string) (*url.URL, error) {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil || parsed.Hostname() == "" || parsed.User != nil ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Opaque != "" ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawPath != "" || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || parsed.RawFragment != "" || strings.Contains(raw, "#") {
		return nil, errors.New("upstream must be an HTTP origin")
	}
	return parsed, nil
}

func (s *selector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := classifyTarget(r.Method, r.RequestURI)
	response := &statusWriter{ResponseWriter: w}
	class := target.class
	var failed bool
	if target.web {
		failed = s.serveWeb(response, r, target)
	} else {
		failed = s.serveLanding(response, r, target.landingPath)
	}
	if failed {
		class = unavailableRouteClass
	}
	status := response.status
	if status == 0 {
		status = http.StatusInternalServerError
	}
	if response.writeErr {
		s.logger.Error("selector response write failed", "route_class", class, "status", status)
	}
	s.logger.Info("selector response", "route_class", class, "status", status)
}

func (s *selector) serveLanding(w http.ResponseWriter, incoming *http.Request, path string) bool {
	target := *s.landingURL
	target.Path = path
	request := &http.Request{
		Method:        incoming.Method,
		URL:           &target,
		Header:        make(http.Header),
		Body:          http.NoBody,
		ContentLength: 0,
		Host:          s.landingURL.Host,
	}
	request.Header["User-Agent"] = nil

	response, err := s.client.Do(request.WithContext(incoming.Context()))
	if err != nil {
		writeUnavailable(w, landingErrorStatus)
		return true
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxLandingBodyBytes+1))
	closeErr := response.Body.Close()
	if err != nil || closeErr != nil || len(body) > maxLandingBodyBytes {
		writeUnavailable(w, landingErrorStatus)
		return true
	}
	fixedBody, ok := linklanding.StaticBodyForStatus(response.StatusCode)
	if !ok {
		writeUnavailable(w, landingErrorStatus)
		return true
	}

	linklanding.SetSecurityHeaders(w.Header(), fixedBody)
	if response.StatusCode == http.StatusMethodNotAllowed {
		for _, allow := range response.Header.Values("Allow") {
			w.Header().Add("Allow", allow)
		}
	}
	w.WriteHeader(response.StatusCode)
	if incoming.Method != http.MethodHead {
		if _, err := io.WriteString(w, fixedBody); err != nil {
			return false
		}
	}
	return false
}

func (s *selector) serveWeb(w http.ResponseWriter, incoming *http.Request, target requestTarget) bool {
	upstreamURL := *s.webURL
	upstreamURL.Path = target.path
	upstreamURL.RawQuery = target.query
	upstreamURL.ForceQuery = target.hasQuery && target.query == ""
	request := &http.Request{
		Method:           incoming.Method,
		URL:              &upstreamURL,
		Header:           incoming.Header.Clone(),
		Body:             incoming.Body,
		ContentLength:    incoming.ContentLength,
		TransferEncoding: append([]string(nil), incoming.TransferEncoding...),
		Host:             s.webURL.Host,
		Close:            incoming.Close,
	}
	if request.Body == nil {
		request.Body = http.NoBody
	}
	removeHopByHopHeaders(request.Header)

	response, err := s.client.Do(request.WithContext(incoming.Context()))
	if err != nil {
		writeUnavailable(w, webErrorStatus)
		return true
	}
	if response.StatusCode >= http.StatusInternalServerError {
		if err := response.Body.Close(); err != nil {
			s.logger.Error("selector upstream response close failed", "route_class", unavailableRouteClass, "status", webErrorStatus)
		}
		writeUnavailable(w, webErrorStatus)
		return true
	}
	// Stage the complete response before committing upstream headers so a
	// truncated body or timeout can still become the fixed failure response.
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxWebBodyBytes+1))
	closeErr := response.Body.Close()
	if readErr != nil || closeErr != nil || len(body) > maxWebBodyBytes {
		writeUnavailable(w, webErrorStatus)
		return true
	}

	copyResponseHeaders(w.Header(), response.Header)
	w.WriteHeader(response.StatusCode)
	if incoming.Method != http.MethodHead {
		if _, err := w.Write(body); err != nil {
			return true
		}
	}
	return false
}

func writeUnavailable(w http.ResponseWriter, status int) {
	linklanding.SetSecurityHeaders(w.Header(), landingUnavailable)
	w.WriteHeader(status)
	if _, err := io.WriteString(w, landingUnavailable); err != nil {
		return
	}
}

type requestTarget struct {
	class       string
	web         bool
	path        string
	query       string
	hasQuery    bool
	landingPath string
}

func classifyTarget(method, requestURI string) requestTarget {
	if strings.EqualFold(method, http.MethodConnect) {
		return requestTarget{class: defaultRouteClass, landingPath: "/"}
	}
	rawPath, query, hasQuery, valid := splitRequestTarget(requestURI)
	if !valid {
		return requestTarget{class: defaultRouteClass, landingPath: "/"}
	}
	if isAdminPath(rawPath) {
		return requestTarget{class: defaultRouteClass, landingPath: "/admin"}
	}
	if class, ok := linkClass(rawPath); ok {
		return requestTarget{class: class, landingPath: canonicalLandingPath(class)}
	}
	if isWebPath(rawPath) {
		return requestTarget{
			class:    webRouteClass,
			web:      true,
			path:     rawPath,
			query:    query,
			hasQuery: hasQuery,
		}
	}
	return requestTarget{class: defaultRouteClass, landingPath: "/"}
}

func splitRequestTarget(requestURI string) (path, query string, hasQuery, valid bool) {
	if requestURI == "" {
		return "", "", false, false
	}
	path, query, hasQuery = strings.Cut(requestURI, "?")
	if path == "" || path[0] != '/' || strings.HasPrefix(path, "//") ||
		strings.ContainsAny(path, "\\#\r\n\x00") || strings.Contains(query, "#") {
		return path, query, hasQuery, false
	}
	return path, query, hasQuery, true
}

func linkClass(path string) (string, bool) {
	if strings.HasPrefix(path, "/+") && !strings.Contains(path[2:], "/") && invitePart.MatchString(path[2:]) {
		return inviteRouteClass, true
	}
	segments := strings.Split(path[1:], "/")
	if len(segments) == 1 && usernamePart.MatchString(segments[0]) {
		return usernameRouteClass, true
	}
	if len(segments) == 2 && usernamePart.MatchString(segments[0]) && messageIDPart.MatchString(segments[1]) {
		return messageRouteClass, true
	}
	if len(segments) == 3 && segments[0] == "c" &&
		messageIDPart.MatchString(segments[1]) && messageIDPart.MatchString(segments[2]) {
		return messageRouteClass, true
	}
	return "", false
}

func canonicalLandingPath(class string) string {
	switch class {
	case inviteRouteClass:
		return "/+redacted"
	case usernameRouteClass:
		return "/redacted"
	case messageRouteClass:
		return "/redacted/1"
	default:
		return "/"
	}
}

func isAdminPath(path string) bool {
	return path == "/admin" || strings.HasPrefix(path, "/admin/")
}

func isWebPath(path string) bool {
	if path == "/" || path == "/.well-known/telegram-web/version.txt" {
		return true
	}
	if strings.ContainsAny(path, "%\\") || strings.HasSuffix(path, "/") || len(path) < 2 {
		return false
	}
	segments := strings.Split(path[1:], "/")
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return false
		}
	}
	if len(segments) == 1 {
		return rootFilePattern.MatchString(segments[0])
	}
	if !strings.HasPrefix(path, "/assets/") && !strings.HasPrefix(path, "/changelogs/") {
		return false
	}
	for _, segment := range segments[1:] {
		if !assetPart.MatchString(segment) {
			return false
		}
	}
	return len(segments) > 1
}

func removeHopByHopHeaders(header http.Header) {
	for _, value := range header.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			header.Del(strings.TrimSpace(token))
		}
	}
	for _, name := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		header.Del(name)
	}
}

func copyResponseHeaders(destination, source http.Header) {
	hopByHop := make(map[string]struct{})
	for _, value := range source.Values("Connection") {
		for token := range strings.SplitSeq(value, ",") {
			hopByHop[http.CanonicalHeaderKey(strings.TrimSpace(token))] = struct{}{}
		}
	}
	for _, name := range []string{
		"Connection", "Proxy-Connection", "Keep-Alive", "Proxy-Authenticate",
		"Proxy-Authorization", "Te", "Trailer", "Transfer-Encoding", "Upgrade",
	} {
		hopByHop[name] = struct{}{}
	}
	for name, values := range source {
		if _, ok := hopByHop[http.CanonicalHeaderKey(name)]; ok {
			continue
		}
		for _, value := range values {
			destination.Add(name, value)
		}
	}
}

type statusWriter struct {
	http.ResponseWriter

	status   int
	writeErr bool
}

func (w *statusWriter) Header() http.Header {
	return w.ResponseWriter.Header()
}

func (w *statusWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}

func (w *statusWriter) Write(body []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(body)
	if err != nil {
		w.writeErr = true
	}
	return n, err
}

func (w *statusWriter) Unwrap() http.ResponseWriter {
	return w.ResponseWriter
}
