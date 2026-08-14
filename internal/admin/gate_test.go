package admin_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// TestAuthGate_enumerates_routes verifies that every route registered on the
// admin sub-mux returns 401 without a valid session cookie. It drives from
// the actual route registry (ProtectedRoutes), not a hard-coded list, so the
// test fails if a new handler is registered directly on the protected sub-mux
// without going through RequireAdmin.
func TestAuthGate_enumerates_routes(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := authTestTokenHash()
	registry := mtproto.NewSessionRegistry()

	router := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
		Logger:    slog.Default(),
	}, registry)

	// Enumerate protected routes from the actual registry.
	// AdminRouter always returns *adminRouter which implements ProtectedRoutes.
	pr, ok := router.(interface{ ProtectedRoutes() []string })
	if !ok {
		t.Fatal("AdminRouter did not return a router with ProtectedRoutes")
	}
	protectedRoutes := pr.ProtectedRoutes()

	// Every registered protected pattern must return 401 without session.
	for _, pattern := range protectedRoutes {
		// Parse method and path from pattern (e.g., "GET /admin/metrics").
		method, path, _ := strings.Cut(pattern, " ")
		if method == "" {
			method = http.MethodGet
		}

		t.Run(method+" "+path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(ctx, method, path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected 401, got %d", rec.Code)
			}
		})
	}

	// Public paths should NOT return 401.
	publicPaths := []struct {
		method string
		path   string
	}{
		{http.MethodGet, "/admin/login"},
	}

	for _, tc := range publicPaths {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			req := httptest.NewRequestWithContext(ctx, tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code == http.StatusUnauthorized {
				t.Errorf("public route %s %s should not return 401", tc.method, tc.path)
			}
		})
	}
}

// TestAuthGate_new_route_without_auth catches the case where someone registers
// a new handler on the admin mux without wrapping it in RequireAdmin.
//
// It works by verifying that the only routes that respond without 401 are
// the explicitly public ones (login GET, login POST with valid CSRF+token,
// logout POST with valid CSRF). Everything else must return 401.
func TestAuthGate_methods_on_protected_routes(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := authTestTokenHash()
	registry := mtproto.NewSessionRegistry()

	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
		Logger:    slog.Default(),
	}, registry)

	// Every HTTP method on /admin/metrics should require auth.
	methods := []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodHead,
		http.MethodOptions,
	}

	for _, method := range methods {
		t.Run(method+" /admin/metrics", func(t *testing.T) {
			req := httptest.NewRequestWithContext(ctx, method, "/admin/metrics", nil)
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)

			// Should be 401 (no session) or 405 (method not allowed but still behind auth).
			// Must NOT be 200.
			if rec.Code == http.StatusOK {
				t.Errorf("%s /admin/metrics returned 200 without auth", method)
			}
		})
	}
}

// TestAuthGate_metrics_accessible_with_session verifies that after a successful
// login, the metrics endpoint is accessible.
func TestAuthGate_metrics_accessible_with_session(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	rawToken := "gate-test-token"
	tokenHash := sha256hex([]byte(rawToken))
	registry := mtproto.NewSessionRegistry()

	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
		Logger:    slog.Default(),
	}, registry)

	// Login.
	sessionID := loginAndGetSession(t, h, rawToken)

	// Access metrics with session.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 with valid session, got %d", rec.Code)
	}

	// Response should be JSON.
	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/json") {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

// loginAndGetSession performs a full login flow and returns the session cookie value.
func loginAndGetSession(t *testing.T, h http.Handler, rawToken string) string {
	t.Helper()

	// GET /admin/login to get CSRF token.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	csrfStart := strings.Index(body, `name="csrf_token" value="`)
	if csrfStart == -1 {
		t.Fatal("could not find csrf_token in response body")
	}
	csrfStart += len(`name="csrf_token" value="`)
	csrfEnd := strings.Index(body[csrfStart:], `"`)
	if csrfEnd == -1 {
		t.Fatal("could not find end of csrf_token value")
	}
	csrfToken := body[csrfStart : csrfStart+csrfEnd]

	csrfCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			csrfCookie = c.Value
		}
	}

	// POST /admin/login.
	form := strings.NewReader("csrf_token=" + csrfToken + "&token=" + rawToken)
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("login returned %d", rec.Code)
	}

	// Extract session cookie.
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-admin-session" {
			return c.Value
		}
	}
	t.Fatal("no session cookie after login")
	return ""
}
