package admin_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/admin"
)

// tokenHash returns a valid 64-char hex SHA-256 digest for testing.
func tokenHash(t *testing.T) string {
	t.Helper()
	raw := make([]byte, 32)
	for i := range raw {
		raw[i] = byte(i)
	}
	return hex.EncodeToString(raw)
}

// csrfTokenForCookie returns the raw CSRF token that matches the given cookie value.
// The cookie stores SHA-256(rawToken), so we need to reverse this for testing.
// Since we can't reverse SHA-256, we construct a known token and its hash.
func makeCSRFToken(t *testing.T) (rawToken, cookieValue string) {
	t.Helper()
	raw := make([]byte, admin.CsrfTokenLen())
	for i := range raw {
		raw[i] = byte(i + 100)
	}
	rawHex := hex.EncodeToString(raw)
	hash := sha256.Sum256(raw)
	hashHex := hex.EncodeToString(hash[:])
	return rawHex, hashHex
}

func TestLoginGET_serves_form_with_csrf(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	h := admin.LoginGET(cfg)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	body := rec.Body.String()

	// Should contain a form with method="post".
	if !strings.Contains(body, `method="post"`) {
		t.Error("form missing method=\"post\"")
	}

	// Should contain a hidden csrf_token field with a non-empty value.
	if !strings.Contains(body, `name="csrf_token"`) {
		t.Error("form missing csrf_token field")
	}

	// Should set a CSRF cookie.
	cookies := rec.Result().Cookies()
	var csrfCookieFound bool
	for _, c := range cookies {
		if c.Name == admin.CsrfCookieName() {
			csrfCookieFound = true
			if c.Value == "" {
				t.Error("CSRF cookie value is empty")
			}
			if !c.HttpOnly {
				t.Error("CSRF cookie not HttpOnly")
			}
			if c.Secure == false {
				t.Error("CSRF cookie not Secure")
			}
		}
	}
	if !csrfCookieFound {
		t.Error("no CSRF cookie set")
	}

	// Content-Type should be text/html.
	ct := rec.Header().Get("Content-Type")
	if ct != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/html; charset=utf-8", ct)
	}
}

func TestLoginPOST_no_csrf_token_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	h := admin.LoginPOST(cfg)
	body := strings.NewReader("token=wrong")
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLoginPOST_invalid_csrf_token_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	rawToken, _ := makeCSRFToken(t)
	wrongHash := "00" + hex.EncodeToString(make([]byte, 31))

	h := admin.LoginPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken + "&token=wrong")
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: wrongHash,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLoginPOST_wrong_token_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	rawToken, cookieValue := makeCSRFToken(t)

	h := admin.LoginPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken + "&token=wrong-password")
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: cookieValue,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLoginPOST_success_sets_session_cookie(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	tokenHash := tokenHash(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash,
	}

	// We need to know the pre-image of the token hash to send the correct token.
	// Since tokenHash generates a deterministic hash, we can't reverse it.
	// Instead, we create a known token and use its hash as TokenHash.
	knownToken := "test-operator-token"
	th := sha256.Sum256([]byte(knownToken))
	knownTokenHash := hex.EncodeToString(th[:])

	cfg.TokenHash = knownTokenHash

	rawToken, cookieValue := makeCSRFToken(t)

	h := admin.LoginPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken + "&token=" + knownToken)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: cookieValue,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected 303 redirect, got %d, body: %s", rec.Code, rec.Body.String())
	}

	// Should redirect to /admin/metrics.
	location := rec.Header().Get("Location")
	if location != "/admin/metrics" {
		t.Errorf("Location = %q, want /admin/metrics", location)
	}

	// Should set a session cookie with __Host- prefix.
	cookies := rec.Result().Cookies()
	var sessionCookieFound bool
	for _, c := range cookies {
		if c.Name == admin.SessionCookieName() {
			sessionCookieFound = true
			if c.Value == "" {
				t.Error("session cookie value is empty")
			}
			if !c.HttpOnly {
				t.Error("session cookie not HttpOnly")
			}
			if !c.Secure {
				t.Error("session cookie not Secure")
			}
			if c.SameSite != http.SameSiteStrictMode {
				t.Errorf("session cookie SameSite = %v, want Strict", c.SameSite)
			}
		}
	}
	if !sessionCookieFound {
		t.Error("no session cookie set after successful login")
	}
}

func TestLogoutPOST_no_csrf_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	h := admin.LogoutPOST(cfg)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/logout", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLogoutPOST_invalid_csrf_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	rawToken, _ := makeCSRFToken(t)
	wrongHash := "00" + hex.EncodeToString(make([]byte, 31))

	h := admin.LogoutPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/logout", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: wrongHash,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLogoutPOST_success_clears_cookies(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	rawToken, cookieValue := makeCSRFToken(t)

	h := admin.LogoutPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/logout", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: cookieValue,
	})
	// Add a session cookie to clear.
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.SessionCookieName(),
		Value: "some-session-id",
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected 303 redirect, got %d", rec.Code)
	}

	// Should redirect to /admin/login.
	location := rec.Header().Get("Location")
	if location != "/admin/login" {
		t.Errorf("Location = %q, want /admin/login", location)
	}

	// Should clear both cookies.
	cookies := rec.Result().Cookies()
	var sessionCleared, csrfCleared bool
	for _, c := range cookies {
		if c.Name == admin.SessionCookieName() && c.Value == "" && c.Expires.Unix() == 0 {
			sessionCleared = true
		}
		if c.Name == admin.CsrfCookieName() && c.Value == "" && c.Expires.Unix() == 0 {
			csrfCleared = true
		}
	}
	if !sessionCleared {
		t.Error("session cookie not cleared")
	}
	if !csrfCleared {
		t.Error("CSRF cookie not cleared")
	}
}

func TestSecurityHeaders_sets_headers(t *testing.T) {
	t.Parallel()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := admin.SecurityHeaders(next)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", rec.Header().Get("X-Content-Type-Options"))
	}
	if rec.Header().Get("Content-Security-Policy") != "frame-ancestors 'none'" {
		t.Errorf("Content-Security-Policy = %q, want frame-ancestors 'none'", rec.Header().Get("Content-Security-Policy"))
	}

	// No CORS headers.
	if v := rec.Header().Get("Access-Control-Allow-Origin"); v != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty", v)
	}
}

func TestSecurityHeaders_removes_cors_headers(t *testing.T) {
	t.Parallel()

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
		w.WriteHeader(http.StatusOK)
	})

	h := admin.SecurityHeaders(next)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if v := rec.Header().Get("Access-Control-Allow-Origin"); v != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty (should be removed)", v)
	}
	if v := rec.Header().Get("Access-Control-Allow-Methods"); v != "" {
		t.Errorf("Access-Control-Allow-Methods = %q, want empty (should be removed)", v)
	}
}

func TestLoginPOST_origin_mismatch_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:       st,
		TokenHash:   tokenHash(t),
		AdminOrigin: "https://admin.example.com",
	}

	h := admin.LoginPOST(cfg)
	body := strings.NewReader("csrf_token=abc&token=wrong")
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Origin", "https://evil.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLoginPOST_same_origin_fetch_site_passes(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	knownToken := "test-operator-token"
	th := sha256.Sum256([]byte(knownToken))
	knownTokenHash := hex.EncodeToString(th[:])
	cfg := admin.LoginConfig{
		Store:       st,
		TokenHash:   knownTokenHash,
		AdminOrigin: "https://admin.example.com",
	}

	rawToken, cookieValue := makeCSRFToken(t)

	h := admin.LoginPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken + "&token=" + knownToken)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: cookieValue,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Should succeed (303 redirect) because Sec-Fetch-Site is same-origin.
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("expected 303 redirect, got %d, body: %s", rec.Code, rec.Body.String())
	}
}

func TestLoginPOST_empty_token_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	rawToken, cookieValue := makeCSRFToken(t)

	h := admin.LoginPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken + "&token=")
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/login", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: cookieValue,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLogoutPOST_no_session_cookie_401(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)
	cfg := admin.LoginConfig{
		Store:     st,
		TokenHash: tokenHash(t),
	}

	rawToken, cookieValue := makeCSRFToken(t)

	h := admin.LogoutPOST(cfg)
	body := strings.NewReader("csrf_token=" + rawToken)
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/logout", body)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie
		Name:  admin.CsrfCookieName(),
		Value: cookieValue,
	})
	// No session cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}
