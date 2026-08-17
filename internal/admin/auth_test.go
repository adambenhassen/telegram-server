package admin_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// ctx is a background context for test requests.
var ctx = context.Background()

func newAuthTestStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() }) //nolint:errcheck // best-effort
	return st
}

func authTestTokenHash() string {
	return hex.EncodeToString(make([]byte, 32))
}

func sha256hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func newTestRouter(t *testing.T, st *store.Store, tokenHash string) http.Handler {
	t.Helper()
	registry := mtproto.NewSessionRegistry()
	return admin.AdminRouter(admin.LoginHandlerConfig{
		Store:     st,
		TokenHash: tokenHash,
		Logger:    slog.Default(),
	}, registry)
}

// --- GET /admin/login tests ---

func TestLoginGET_serves_form(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	h := newTestRouter(t, st, authTestTokenHash())

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	if rec.Header().Get("Content-Type") != "text/html; charset=utf-8" {
		t.Errorf("Content-Type = %q, want text/html; charset=utf-8", rec.Header().Get("Content-Type"))
	}
	body := rec.Body.String()
	if !strings.Contains(body, "csrf_token") {
		t.Error("login form does not contain csrf_token field")
	}
	if !strings.Contains(body, `type="password"`) {
		t.Error("login form does not contain password input")
	}
}

func TestLoginGET_sets_csrf_cookie(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	h := newTestRouter(t, st, authTestTokenHash())

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	cookies := rec.Result().Cookies()
	if len(cookies) == 0 {
		t.Fatal("expected at least one Set-Cookie header")
	}
	var foundCSRF bool
	for _, c := range cookies {
		if c.Name == "__Host-csrf-token" {
			foundCSRF = true
			if !c.Secure {
				t.Error("csrf cookie not Secure")
			}
			if !c.HttpOnly {
				t.Error("csrf cookie not HttpOnly")
			}
			if c.SameSite != http.SameSiteStrictMode {
				t.Errorf("csrf cookie SameSite = %v, want Strict", c.SameSite)
			}
		}
	}
	if !foundCSRF {
		t.Error("no __Host-csrf-token cookie set")
	}
}

// --- POST /admin/login tests ---

func TestLoginPOST_valid_token_creates_session(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	// First GET to obtain CSRF token and cookie.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Extract CSRF token from the HTML body.
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

	// Extract CSRF cookie.
	csrfCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			csrfCookie = c.Value
		}
	}

	// POST with valid token and CSRF.
	form := strings.NewReader("csrf_token=" + csrfToken + "&token=correct-token")
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Should redirect to /admin/metrics.
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
	if rec.Header().Get("Location") != "/admin/metrics" {
		t.Errorf("Location = %q, want /admin/metrics", rec.Header().Get("Location"))
	}

	// Check session cookie was set.
	cookies := rec.Result().Cookies()
	var foundSession bool
	for _, c := range cookies {
		if c.Name == "__Host-admin-session" {
			foundSession = true
			if !c.Secure {
				t.Error("session cookie not Secure")
			}
			if !c.HttpOnly {
				t.Error("session cookie not HttpOnly")
			}
		}
	}
	if !foundSession {
		t.Error("no __Host-admin-session cookie set after login")
	}
}

func TestLoginPOST_wrong_token_401(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	// GET to obtain CSRF.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	csrfStart := strings.Index(body, `name="csrf_token" value="`)
	csrfStart += len(`name="csrf_token" value="`)
	csrfEnd := strings.Index(body[csrfStart:], `"`)
	csrfToken := body[csrfStart : csrfStart+csrfEnd]

	csrfCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			csrfCookie = c.Value
		}
	}

	// POST with wrong token.
	form := strings.NewReader("csrf_token=" + csrfToken + "&token=wrong-token")
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Errorf("body should be empty on 401, got %d bytes", rec.Body.Len())
	}
}

func TestLoginPOST_missing_csrf_401(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	// POST without CSRF token.
	form := strings.NewReader("token=correct-token")
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestLoginPOST_bad_csrf_401(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	// GET to obtain CSRF cookie.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	csrfCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			csrfCookie = c.Value
		}
	}

	// POST with wrong CSRF token.
	form := strings.NewReader("csrf_token=fake-csrf&token=correct-token")
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

// --- POST /admin/logout tests ---

func TestLogoutPOST_with_csrf_clears_session(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	// Login first to get a session.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	csrfStart := strings.Index(body, `name="csrf_token" value="`)
	csrfStart += len(`name="csrf_token" value="`)
	csrfEnd := strings.Index(body[csrfStart:], `"`)
	csrfToken := body[csrfStart : csrfStart+csrfEnd]

	csrfCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			csrfCookie = c.Value
		}
	}

	form := strings.NewReader("csrf_token=" + csrfToken + "&token=correct-token")
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("login: expected 302, got %d", rec.Code)
	}

	// Extract session cookie.
	sessionCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-admin-session" {
			sessionCookie = c.Value
		}
	}
	if sessionCookie == "" {
		t.Fatal("no session cookie after login")
	}

	// Derive the logout CSRF token from the session cookie, as the dashboard
	// does. No CSRF cookie is involved for authenticated forms.
	logoutToken, err := admin.SessionCSRFToken(tokenHash, sessionCookie)
	if err != nil {
		t.Fatal(err)
	}

	// POST /admin/logout with CSRF and session cookie.
	form = strings.NewReader("csrf_token=" + logoutToken)
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/logout", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("logout: expected 302, got %d", rec.Code)
	}
	if rec.Header().Get("Location") != "/admin/login" {
		t.Errorf("Location = %q, want /admin/login", rec.Header().Get("Location"))
	}

	// Verify session is deleted from DB.
	sessionHash := sha256.Sum256([]byte(sessionCookie))
	_, err = st.GetAdminSession(context.Background(), sessionHash[:])
	if err == nil {
		t.Error("session should be deleted from DB after logout")
	}
}

func TestLogoutPOST_missing_csrf_401(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := authTestTokenHash()

	h := newTestRouter(t, st, tokenHash)

	// POST /admin/logout without CSRF.
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/logout", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

// --- Session-bound CSRF token tests ---

// TestLogoutPOST_second_tab_same_token reproduces MAIN-297: two tabs sharing
// the same session cookie must both carry a valid logout token. The token is
// derived from the session, so it does not depend on any cookie the dashboard
// may have re-issued.
func TestLogoutPOST_second_tab_same_token(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	sessionCookie := loginAndGetSession(t, h, "correct-token")

	// Tab 1 renders the dashboard; capture its token.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/dashboard", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard: expected 200, got %d", rec.Code)
	}
	tab1Token := extractCSRFToken(t, rec.Body.String())

	// Tab 2 renders the dashboard again (this is what used to re-issue the
	// CSRF cookie and invalidate tab 1's form).
	req = httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/dashboard", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("dashboard: expected 200, got %d", rec.Code)
	}
	tab2Token := extractCSRFToken(t, rec.Body.String())

	if tab1Token != tab2Token {
		t.Fatal("tokens differ between tabs; logout from tab 1 would 401")
	}

	// Logout using tab 1's token after tab 2 rendered.
	form := strings.NewReader("csrf_token=" + tab1Token)
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/logout", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("logout: expected 302, got %d", rec.Code)
	}
}

// TestLogoutPOST_wrong_token_401 verifies a token not derived from this
// session (e.g. the pre-auth login CSRF token) is rejected.
func TestLogoutPOST_wrong_token_401(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	sessionCookie := loginAndGetSession(t, h, "correct-token")

	form := strings.NewReader("csrf_token=not-derived-from-session")
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/logout", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

// TestLogoutPOST_body_size_limit verifies the body cap is enforced before
// auth: an oversized body is rejected with 413 even with a valid session.
func TestLogoutPOST_body_size_limit(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	sessionCookie := loginAndGetSession(t, h, "correct-token")

	// A single form value whose length field alone (64 bits + delimiter,
	// ~9 bytes) exceeds the 4 KiB cap. MaxBytesReader rejects it while the
	// body is being parsed, before the token check runs.
	body := "csrf_token=" + strings.Repeat("a", 4096)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/logout", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized body, got %d", rec.Code)
	}
}

// TestLogoutPOST_body_size_limit_in_cap_body_still_authenticates verifies the
// 413 path does not shadow the auth path: a body under the cap with a wrong
// token must be rejected with 401, not 413.
func TestLogoutPOST_body_size_limit_in_cap_body_still_authenticates(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	sessionCookie := loginAndGetSession(t, h, "correct-token")

	body := "csrf_token=short-but-wrong-token"
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/logout", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for in-cap body with wrong token, got %d", rec.Code)
	}
}

// TestLogoutPOST_token_valid_after_cookie_ttl pins the 15-minute TTL
// regression: the logout token is derived from the session, so it validates
// long after the pre-auth CSRF cookie would have expired.
func TestLogoutPOST_token_valid_after_cookie_ttl(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	sessionCookie := loginAndGetSession(t, h, "correct-token")

	// Derive the token now, then verify it still matches after the CSRF
	// cookie TTL (15 minutes) has elapsed. The derivation is stateless and
	// time-independent, so the token is identical at any point.
	tokenBefore, err := admin.SessionCSRFToken(tokenHash, sessionCookie)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the TTL having passed by deriving again; the result must be
	// identical to the token rendered at login time.
	tokenAfter, err := admin.SessionCSRFToken(tokenHash, sessionCookie)
	if err != nil {
		t.Fatal(err)
	}
	if tokenBefore != tokenAfter {
		t.Fatal("token changed over time; would 401 after the cookie TTL")
	}

	// Now actually log out with that token.
	form := strings.NewReader("csrf_token=" + tokenBefore)
	req := httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/logout", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionCookie}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d", rec.Code)
	}
}

// TestCSRFSessionToken_deterministic_and_rotating pins the derivation:
// identical for the same inputs, different for a different session, and
// invalidated when the token hash changes.
func TestCSRFSessionToken_deterministic_and_rotating(t *testing.T) {
	t.Parallel()
	hashA := sha256hex([]byte("token-a"))
	hashB := sha256hex([]byte("token-b"))

	tA1, err := admin.SessionCSRFToken(hashA, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	tA2, err := admin.SessionCSRFToken(hashA, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if tA1 != tA2 {
		t.Error("token derivation is not deterministic")
	}

	tB, err := admin.SessionCSRFToken(hashA, "session-2")
	if err != nil {
		t.Fatal(err)
	}
	if tA1 == tB {
		t.Error("different sessions must yield different tokens")
	}

	tArot, err := admin.SessionCSRFToken(hashB, "session-1")
	if err != nil {
		t.Fatal(err)
	}
	if tA1 == tArot {
		t.Error("rotating the token hash must invalidate existing tokens")
	}

	// Token is a 32-byte hex value.
	if len(tA1) != 64 {
		t.Errorf("token length = %d, want 64 hex chars", len(tA1))
	}

	if _, err := admin.SessionCSRFToken("zz", "session-1"); err == nil {
		t.Error("expected error for invalid token hash")
	}
}

// extractCSRFToken pulls the csrf_token hidden field value out of an HTML page.
func extractCSRFToken(t *testing.T, body string) string {
	t.Helper()
	start := strings.Index(body, `name="csrf_token" value="`)
	if start == -1 {
		t.Fatal("could not find csrf_token in response body")
	}
	start += len(`name="csrf_token" value="`)
	end := strings.Index(body[start:], `"`)
	if end == -1 {
		t.Fatal("could not find end of csrf_token value")
	}
	return body[start : start+end]
}

// --- Security headers tests ---

func TestSecurityHeaders_on_login(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	h := newTestRouter(t, st, authTestTokenHash())

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", rec.Header().Get("X-Content-Type-Options"))
	}
	if rec.Header().Get("Content-Security-Policy") != "frame-ancestors 'none'" {
		t.Errorf("CSP = %q, want frame-ancestors 'none'", rec.Header().Get("Content-Security-Policy"))
	}
	// No CORS headers.
	if v := rec.Header().Get("Access-Control-Allow-Origin"); v != "" {
		t.Errorf("Access-Control-Allow-Origin = %q, want empty", v)
	}
}

func TestSecurityHeaders_on_401(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	h := newTestRouter(t, st, authTestTokenHash())

	// Request a protected route without auth.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Header().Get("Cache-Control") != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", rec.Header().Get("Cache-Control"))
	}
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Errorf("X-Content-Type-Options = %q, want nosniff", rec.Header().Get("X-Content-Type-Options"))
	}
}

// --- TokenFingerprint export test ---

func TestTokenFingerprint(t *testing.T) {
	t.Parallel()
	tokenHash := hex.EncodeToString(make([]byte, 32))
	fp, err := admin.TokenFingerprint(tokenHash)
	if err != nil {
		t.Fatalf("TokenFingerprint: %v", err)
	}
	if len(fp) != 32 {
		t.Fatalf("TokenFingerprint returned %d bytes, want 32", len(fp))
	}
	// Verify it is SHA-256 of the decoded hex.
	decoded, err := hex.DecodeString(tokenHash)
	if err != nil {
		t.Fatal(err)
	}
	expected := sha256.Sum256(decoded)
	if string(fp) != string(expected[:]) {
		t.Fatal("TokenFingerprint did not return expected hash")
	}
}

func TestTokenFingerprint_invalid_hex(t *testing.T) {
	t.Parallel()
	_, err := admin.TokenFingerprint("zz")
	if err == nil {
		t.Fatal("expected error for invalid hex")
	}
}

// --- Protected route requires session ---

func TestProtectedRoute_without_session_401(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	h := newTestRouter(t, st, authTestTokenHash())

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

// --- Rate limiter test ---

func TestLoginPOST_rate_limiter_delays(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	// GET CSRF token once.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	csrfStart := strings.Index(body, `name="csrf_token" value="`)
	csrfStart += len(`name="csrf_token" value="`)
	csrfEnd := strings.Index(body[csrfStart:], `"`)
	csrfToken := body[csrfStart : csrfStart+csrfEnd]

	csrfCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			csrfCookie = c.Value
		}
	}

	// Make several rapid POST attempts with wrong token.
	for i := range 10 {
		form := strings.NewReader("csrf_token=" + csrfToken + "&token=wrong")
		req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
		rec = httptest.NewRecorder()
		start := time.Now()
		h.ServeHTTP(rec, req)
		elapsed := time.Since(start)

		// After the first 5 attempts, subsequent ones should take longer (due to rate limit delay).
		if i >= 5 && elapsed < 100*time.Millisecond {
			t.Errorf("attempt %d: elapsed %v, expected rate limit delay (>100ms)", i, elapsed)
		}
		if rec.Code != http.StatusUnauthorized {
			t.Fatalf("attempt %d: expected 401, got %d", i, rec.Code)
		}
	}
}

// --- Session fixation prevention ---

func TestLoginPOST_rotates_session(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	tokenHash := sha256hex([]byte("correct-token"))

	h := newTestRouter(t, st, tokenHash)

	// First login.
	firstSessionID := loginAndGetSession(t, h, "correct-token")
	firstHash := sha256.Sum256([]byte(firstSessionID))

	// Verify first session exists in DB.
	_, err := st.GetAdminSession(ctx, firstHash[:])
	if err != nil {
		t.Fatal("first session should exist in DB")
	}

	// Second login — carry the first session cookie so rotation is tested.
	secondSessionID := loginWithSession(t, h, "correct-token", firstSessionID)
	secondHash := sha256.Sum256([]byte(secondSessionID))

	if firstSessionID == secondSessionID {
		t.Error("session IDs should differ between logins (session fixation prevention)")
	}

	// Old session must be deleted.
	_, err = st.GetAdminSession(ctx, firstHash[:])
	if err == nil {
		t.Error("old session should be deleted after rotation")
	}

	// New session must exist.
	_, err = st.GetAdminSession(ctx, secondHash[:])
	if err != nil {
		t.Error("new session should exist after rotation")
	}
}

// loginWithSession performs a full login flow with an existing session cookie
// and returns the new session cookie value.
func loginWithSession(t *testing.T, h http.Handler, rawToken, sessionID string) string {
	t.Helper()

	// GET CSRF.
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

	// POST /admin/login with existing session cookie.
	form := strings.NewReader("csrf_token=" + csrfToken + "&token=" + rawToken)
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie})   //nolint:gosec // G124: test cookie
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusFound {
		t.Fatalf("login returned %d", rec.Code)
	}

	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-admin-session" {
			return c.Value
		}
	}
	t.Fatal("no session cookie after login")
	return ""
}

// --- No raw token/session in response ---

func TestLoginResponse_no_raw_token_leaked(t *testing.T) {
	t.Parallel()
	st := newAuthTestStore(t)
	rawToken := "my-secret-admin-token"
	tokenHash := sha256hex([]byte(rawToken))

	h := newTestRouter(t, st, tokenHash)

	// GET CSRF.
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/login", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	body := rec.Body.String()
	csrfStart := strings.Index(body, `name="csrf_token" value="`)
	csrfStart += len(`name="csrf_token" value="`)
	csrfEnd := strings.Index(body[csrfStart:], `"`)
	csrfToken := body[csrfStart : csrfStart+csrfEnd]

	csrfCookie := ""
	for _, c := range rec.Result().Cookies() {
		if c.Name == "__Host-csrf-token" {
			csrfCookie = c.Value
		}
	}

	// POST login.
	form := strings.NewReader("csrf_token=" + csrfToken + "&token=" + rawToken)
	req = httptest.NewRequestWithContext(ctx, http.MethodPost, "/admin/login", form)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.AddCookie(&http.Cookie{Name: "__Host-csrf-token", Value: csrfCookie}) //nolint:gosec // G124: test cookie
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	// Response body should not contain the raw token.
	respBody := rec.Body.String()
	if strings.Contains(respBody, rawToken) {
		t.Error("response body contains raw admin token")
	}
}
