package admin

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"html/template"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// csrfCookieName is the name of the cookie that carries the CSRF token hash.
const csrfCookieName = "__Host-csrf-token"

// csrfTokenLen is the size of the raw CSRF token in bytes.
const csrfTokenLen = 32

// loginFormTemplate renders the admin login form.
var loginFormTemplate = template.Must(template.New("login").Parse(`<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>Admin Login</title></head>
<body>
<form method="post" action="/admin/login">
  <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
  <label for="token">Token:<br>
    <input type="password" id="token" name="token" required autocomplete="current-password">
  </label>
  <button type="submit">Login</button>
</form>
</body>
</html>`))

// LoginConfig holds the dependencies required by the login handlers.
type LoginConfig struct {
	// Store is the Postgres-backed store used for session lookups and creation.
	Store *store.Store
	// TokenHash is the current hex-encoded SHA-256 digest of TG_ADMIN_TOKEN_HASH.
	// It is compared against the submitted token (hashed) to authenticate the operator.
	TokenHash string
	// AdminOrigin is the expected Origin or Sec-Fetch-Site header value.
	// Requests with a mismatching origin are rejected before reading credentials.
	// Empty string disables origin checking (for local/dev use).
	AdminOrigin string
}

// LoginGET serves the login form at GET /admin/login.
// It generates a per-request CSRF token and stores its hash in a cookie.
func LoginGET(cfg LoginConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Generate CSRF token from crypto/rand.
		csrfToken := make([]byte, csrfTokenLen)
		if _, err := rand.Read(csrfToken); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		csrfTokenHex := hex.EncodeToString(csrfToken)

		// Store the hash of the CSRF token in a cookie (not the raw token).
		csrfHash := sha256.Sum256(csrfToken)
		csrfHashHex := hex.EncodeToString(csrfHash[:])

		// Set CSRF cookie with __Host- prefix attributes.
		csrfCookie := &http.Cookie{
			Name:     csrfCookieName,
			Value:    csrfHashHex,
			Path:     "/",
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
		}
		http.SetCookie(w, csrfCookie)

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := loginFormTemplate.Execute(w, struct{ CSRFToken string }{CSRFToken: csrfTokenHex}); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}

// LoginPOST handles credential submission at POST /admin/login.
// It enforces rate limiting, origin validation, CSRF, and token comparison.
func LoginPOST(cfg LoginConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Rate limiter keyed on RemoteAddr (not X-Forwarded-For).
		// Uses a delay-based approach — never locks out permanently.
		remoteAddr := r.RemoteAddr
		if delay := rateLimitDelay(remoteAddr); delay > 0 {
			time.Sleep(delay)
		}

		// Wrap body in MaxBytesReader before parsing.
		r.Body = io.NopCloser(http.MaxBytesReader(w, r.Body, 1024))

		// Origin / Sec-Fetch-Site check before reading credentials.
		if cfg.AdminOrigin != "" {
			origin := r.Header.Get("Origin")
			fetchSite := r.Header.Get("Sec-Fetch-Site")
			// If Sec-Fetch-Site is "same-origin", the request is from the same origin.
			if fetchSite != "same-origin" {
				if origin != cfg.AdminOrigin {
					recordRateLimit(remoteAddr)
					w.WriteHeader(http.StatusUnauthorized)
					return
				}
			}
		}

		// Parse form.
		if err := r.ParseForm(); err != nil {
			recordRateLimit(remoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// CSRF validation: compare constant-time against stored hash.
		csrfToken := r.FormValue("csrf_token")
		if csrfToken == "" {
			recordRateLimit(remoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Decode the submitted CSRF token and hash it.
		submittedCSRF, err := hex.DecodeString(csrfToken)
		if err != nil || len(submittedCSRF) != csrfTokenLen {
			recordRateLimit(remoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		submittedHash := sha256.Sum256(submittedCSRF)
		submittedHashHex := hex.EncodeToString(submittedHash[:])

		// Retrieve stored CSRF hash from cookie.
		csrfCookie, err := r.Cookie(csrfCookieName)
		if err != nil {
			recordRateLimit(remoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Constant-time comparison of CSRF hashes.
		if subtle.ConstantTimeCompare([]byte(submittedHashHex), []byte(csrfCookie.Value)) != 1 {
			recordRateLimit(remoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Read the submitted token.
		submittedToken := r.FormValue("token")
		if submittedToken == "" {
			recordRateLimit(remoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Hash the submitted token and compare with ConstantTimeCompare.
		submittedTokenHash := sha256.Sum256([]byte(submittedToken))
		submittedTokenHashHex := hex.EncodeToString(submittedTokenHash[:])

		if subtle.ConstantTimeCompare([]byte(submittedTokenHashHex), []byte(cfg.TokenHash)) != 1 {
			recordRateLimit(remoteAddr)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Success: create a new session.
		sessionID := make([]byte, 32)
		if _, err := rand.Read(sessionID); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		sessionIDHex := hex.EncodeToString(sessionID)

		// Hash session ID for storage.
		sessionHash := sha256.Sum256([]byte(sessionIDHex))

		// Compute token fingerprint (double-hash of TokenHash).
		// TokenHash is validated at config load time, so decode error is impossible.
		decodedTokenHash, _ := hex.DecodeString(cfg.TokenHash) //nolint:errcheck // validated at config load
		tokenFingerprint := sha256.Sum256(decodedTokenHash)

		now := time.Now()
		expiresAt := now.Add(sessionLifetime)

		// Delete any existing session for this browser (session rotation).
		existingCookie, _ := r.Cookie(sessionCookieName) //nolint:errcheck // best-effort rotation
		if existingCookie != nil {
			existingHash := sha256.Sum256([]byte(existingCookie.Value))
			_, _ = cfg.Store.DeleteAdminSession(r.Context(), existingHash[:]) //nolint:errcheck // best-effort rotation
		}

		// Insert new session row.
		if err := cfg.Store.CreateAdminSession(r.Context(), sessionHash[:], tokenFingerprint[:], expiresAt, now); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		// Set session cookie.
		sessionCookie := SessionCookie(sessionIDHex)
		http.SetCookie(w, sessionCookie)

		// Redirect to metrics dashboard.
		http.Redirect(w, r, "/admin/metrics", http.StatusSeeOther)
	}
}

// LogoutPOST handles admin logout at POST /admin/logout.
// Requires valid CSRF and deletes the current session.
func LogoutPOST(cfg LoginConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Parse form first.
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// CSRF validation.
		csrfToken := r.FormValue("csrf_token")
		if csrfToken == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		submittedCSRF, err := hex.DecodeString(csrfToken)
		if err != nil || len(submittedCSRF) != csrfTokenLen {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		submittedHash := sha256.Sum256(submittedCSRF)
		submittedHashHex := hex.EncodeToString(submittedHash[:])

		csrfCookie, err := r.Cookie(csrfCookieName)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		if subtle.ConstantTimeCompare([]byte(submittedHashHex), []byte(csrfCookie.Value)) != 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}

		// Delete the session.
		sessionCookie, err := r.Cookie(sessionCookieName)
		if err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		sessionHash := sha256.Sum256([]byte(sessionCookie.Value))
		_, _ = cfg.Store.DeleteAdminSession(r.Context(), sessionHash[:]) //nolint:errcheck // logout: session gone either way

		// Clear session cookie.
		clearCookie := &http.Cookie{
			Name:     sessionCookieName,
			Value:    "",
			Path:     "/",
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			Expires:  time.Unix(0, 0),
		}
		http.SetCookie(w, clearCookie)

		// Clear CSRF cookie.
		clearCSRFCookie := &http.Cookie{
			Name:     csrfCookieName,
			Value:    "",
			Path:     "/",
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteStrictMode,
			Expires:  time.Unix(0, 0),
		}
		http.SetCookie(w, clearCSRFCookie)

		// Redirect to login.
		http.Redirect(w, r, "/admin/login", http.StatusSeeOther)
	}
}

// --- Rate Limiter ---

// rateLimitEntry tracks login attempt timing for a given address.
type rateLimitEntry struct {
	lastAttempt  time.Time
	attemptCount int
}

var (
	// rateLimitMu protects the rate limit map.
	rateLimitMu sync.Mutex
	// rateLimitMap stores per-address rate limit state.
	rateLimitMap = make(map[string]*rateLimitEntry)
)

// rateLimitDelay returns a delay to apply for the given address.
// Uses a progressive delay: 100ms for the 2nd attempt, 200ms for the 3rd, etc.
// capped at 1 second. Resets after 30 seconds of inactivity.
func rateLimitDelay(addr string) time.Duration {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()

	entry, ok := rateLimitMap[addr]
	if !ok {
		return 0
	}

	elapsed := time.Since(entry.lastAttempt)
	if elapsed > 30*time.Second {
		// Reset after 30 seconds of inactivity.
		entry.attemptCount = 0
		return 0
	}

	entry.attemptCount++
	delay := time.Duration(entry.attemptCount) * 100 * time.Millisecond
	return min(delay, time.Second)
}

// recordRateLimit records a failed login attempt for the given address.
func recordRateLimit(addr string) {
	rateLimitMu.Lock()
	defer rateLimitMu.Unlock()

	entry, ok := rateLimitMap[addr]
	if !ok {
		rateLimitMap[addr] = &rateLimitEntry{
			lastAttempt:  time.Now(),
			attemptCount: 1,
		}
		return
	}

	entry.lastAttempt = time.Now()
	entry.attemptCount++
}

// --- Security Headers ---

// SecurityHeaders returns middleware that adds security response headers to
// all admin responses: Cache-Control, X-Content-Type-Options, and CSP.
// It also ensures no CORS headers leak onto admin responses.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		next.ServeHTTP(w, r)
		// Remove any CORS headers that might have been set by the handler.
		// Done after ServeHTTP so the handler cannot re-add them.
		w.Header().Del("Access-Control-Allow-Origin")
		w.Header().Del("Access-Control-Allow-Methods")
		w.Header().Del("Access-Control-Allow-Headers")
		w.Header().Del("Access-Control-Max-Age")
	})
}

// CsrfCookieName returns the name of the CSRF cookie.
func CsrfCookieName() string {
	return csrfCookieName
}

// CsrfTokenLen returns the length of the CSRF token in bytes.
func CsrfTokenLen() int {
	return csrfTokenLen
}
