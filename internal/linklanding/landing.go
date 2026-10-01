package linklanding

import (
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
)

const (
	landingPage   = `<!doctype html><html lang="en"><body><main>Open this in Telegramd</main></body></html>`
	notFoundPage  = `<!doctype html><html lang="en"><body><main>Not found</main></body></html>`
	methodPage    = `<!doctype html><html lang="en"><body><main>Method not allowed</main></body></html>`
	contentPolicy = "default-src 'none'; style-src 'unsafe-inline'; base-uri 'none'; form-action 'none'; frame-ancestors 'none'"
)

// NewHandler returns the isolated landing page handler. It does not inspect
// request data beyond the path shape used for privacy-safe route logging.
func NewHandler(logger *slog.Logger) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		class := routeClass(r.URL.Path)
		status, body := responseFor(r.Method, class)

		setSecurityHeaders(w.Header(), body)
		if status == http.StatusMethodNotAllowed {
			w.Header().Set("Allow", "GET, HEAD")
		}
		w.WriteHeader(status)

		if r.Method != http.MethodHead {
			if _, err := io.WriteString(w, body); err != nil {
				logger.Info("landing response", "route_class", class, "status", status)
				return
			}
		}
		logger.Info("landing response", "route_class", class, "status", status)
	})
}

func responseFor(method, class string) (int, string) {
	if method != http.MethodGet && method != http.MethodHead {
		return http.StatusMethodNotAllowed, methodPage
	}
	if class == "admin" {
		return http.StatusNotFound, notFoundPage
	}
	return http.StatusOK, landingPage
}

func routeClass(path string) string {
	if path == "/admin" || strings.HasPrefix(path, "/admin/") {
		return "admin"
	}
	if strings.HasPrefix(path, "/+") && !strings.Contains(path[1:], "/") {
		return "invite"
	}
	if path == "/" || !strings.HasPrefix(path, "/") {
		return "other"
	}

	segments := strings.Split(path[1:], "/")
	switch {
	case len(segments) == 1 && segments[0] != "":
		return "username"
	case len(segments) == 2 && segments[0] != "" && segments[1] != "":
		return "message"
	case len(segments) == 3 && segments[0] == "c" && segments[1] != "" && segments[2] != "":
		return "message"
	default:
		return "other"
	}
}

func setSecurityHeaders(header http.Header, body string) {
	header.Set("Cache-Control", "no-store")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Content-Type", "text/html; charset=utf-8")
	header.Set("Content-Security-Policy", contentPolicy)
	header.Set("Content-Length", strconv.Itoa(len(body)))
}
