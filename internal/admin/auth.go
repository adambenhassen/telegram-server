package admin

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin/assets"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// csrfCookieName is the name of the cookie that carries the CSRF token hash.
const csrfCookieName = "__Host-csrf-token"

// csrfTokenLen is the size of the raw CSRF token in bytes before encoding.
const csrfTokenLen = 32

// loginMaxBodyBytes caps the request body of POST /admin/login.
const loginMaxBodyBytes = 4 * 1024 // 4 KiB

// csrfCookieTTL is how long the pre-auth CSRF cookie persists. It is
// short-lived: the token is re-issued on every GET /admin/login and cleared
// on successful login.
const csrfCookieTTL = 15 * time.Minute

// rateLimitWindow is the sliding window for the login rate limiter.
const rateLimitWindow = 30 * time.Second

// rateLimitMaxAttempts is the maximum number of login attempts allowed
// within rateLimitWindow from a single RemoteAddr.
const rateLimitMaxAttempts = 5

// rateLimitDelay is the penalty delay applied when the rate limit is hit.
// The limiter never locks an address out permanently.
const rateLimitDelay = 2 * time.Second

// TokenFingerprint computes the SHA-256 of the decoded hex token hash.
// It is the double-hash stored in admin_sessions.token_fingerprint and
// compared on each authenticated request.
func TokenFingerprint(tokenHash string) ([]byte, error) {
	decoded, err := hex.DecodeString(tokenHash)
	if err != nil {
		return nil, fmt.Errorf("decode token hash: %w", err)
	}
	h := sha256.Sum256(decoded)
	return h[:], nil
}

// generateCSRFToken creates a cryptographically random CSRF token and returns
// it as a hex-encoded string.
func generateCSRFToken() (string, error) {
	buf := make([]byte, csrfTokenLen)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("generate csrf token: %w", err)
	}
	return hex.EncodeToString(buf), nil
}

// csrfCookieHash returns the SHA-256 of a CSRF token as a hex string. This
// is what gets stored in the cookie so the raw token is never exposed.
func csrfCookieHash(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

// SessionCSRFToken derives the CSRF token for an authenticated session.
// It is deterministic: HMAC-SHA256 of a key derived from the admin token
// hash, over the SHA-256 of the hex-encoded session id. Every request that
// carries the same session cookie yields the same token, so the token is
// identical across tabs, never expires on its own, and rotates when
// TG_ADMIN_TOKEN_HASH changes.
func SessionCSRFToken(tokenHash, sessionIDHex string) (string, error) {
	decoded, err := hex.DecodeString(tokenHash)
	if err != nil {
		return "", fmt.Errorf("decode token hash: %w", err)
	}
	key := sha256.Sum256(decoded)
	sessionHash := sha256.Sum256([]byte(sessionIDHex))
	mac := hmac.New(sha256.New, key[:])
	mac.Write(sessionHash[:])
	return hex.EncodeToString(mac.Sum(nil)), nil
}

// csrfCookie returns a preconfigured http.Cookie for a CSRF token.
func csrfCookie(hash string) *http.Cookie {
	return &http.Cookie{
		Name:     csrfCookieName,
		Value:    hash,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   int(csrfCookieTTL.Seconds()),
	}
}

// clearCSRFCookie returns a cookie that deletes the CSRF token.
func clearCSRFCookie() *http.Cookie {
	return &http.Cookie{
		Name:     csrfCookieName,
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	}
}

// rateLimiter tracks login attempts per RemoteAddr using a sliding window.
type rateLimiter struct {
	mu       sync.Mutex
	attempts map[string][]time.Time
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{
		attempts: make(map[string][]time.Time),
	}
}

// new is called when the rate limit is exceeded. It returns the delay to apply.
func (rl *rateLimiter) record(addr string) time.Duration {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := time.Now()
	windowStart := now.Add(-rateLimitWindow)

	// Prune old attempts.
	if times, ok := rl.attempts[addr]; ok {
		pruned := times[:0]
		for _, t := range times {
			if t.After(windowStart) {
				pruned = append(pruned, t)
			}
		}
		rl.attempts[addr] = pruned
	}

	// Record this attempt.
	rl.attempts[addr] = append(rl.attempts[addr], now)

	// Check if over limit.
	if len(rl.attempts[addr]) > rateLimitMaxAttempts {
		return rateLimitDelay
	}
	return 0
}

// loginFormHTML renders the login form with the CSRF token embedded.
func loginFormHTML(csrfToken string) []byte {
	return fmt.Appendf(nil, `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Admin Login</title>
<style>
body { font-family: system-ui, sans-serif; margin: 40px; background: #f5f5f5; }
form { max-width: 360px; margin: 0 auto; background: #fff; padding: 24px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.1); }
label { display: block; margin-bottom: 4px; font-weight: 500; }
input[type="password"] { width: 100%%; padding: 8px; margin-bottom: 16px; border: 1px solid #ccc; border-radius: 4px; box-sizing: border-box; }
button { width: 100%%; padding: 10px; background: #0066cc; color: #fff; border: none; border-radius: 4px; cursor: pointer; }
button:hover { background: #0055aa; }
</style>
</head>
<body>
<form method="post" action="/admin/login">
<label for="token">Admin Token</label>
<input type="hidden" name="csrf_token" value="%s">
<input type="password" id="token" name="token" required autocomplete="current-password">
<button type="submit">Sign in</button>
</form>
</body>
</html>`, csrfToken)
}

// LoginHandlerConfig holds dependencies for login/logout handlers.
type LoginHandlerConfig struct {
	// Store is the Postgres-backed store used for session operations.
	Store *store.Store
	// TokenHash is the hex-encoded SHA-256 digest of TG_ADMIN_TOKEN_HASH.
	TokenHash string
	// Logger is used for structured logging (never logs raw tokens or session ids).
	Logger *slog.Logger
	// AdminOrigin is the expected Origin header for login/logout POST requests.
	// Derived from the admin listen address at startup.
	AdminOrigin string
	// Events is the shared metrics broadcaster backing GET /admin/events.
	// A nil value registers the route but reports it unavailable, so the
	// dashboard falls back to its server-rendered first paint.
	Events *Broadcaster
}

// handleLoginGET serves the login form with a CSRF token.
func handleLoginGET(cfg LoginHandlerConfig, w http.ResponseWriter, r *http.Request) {
	csrfToken, err := generateCSRFToken()
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Store the CSRF token hash in a cookie so we can verify it on POST.
	http.SetCookie(w, csrfCookie(csrfCookieHash(csrfToken)))

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write(loginFormHTML(csrfToken)); err != nil {
		// Client disconnected — nothing actionable.
		return
	}
}

// handleLoginPOST processes the login form submission.
func handleLoginPOST(cfg LoginHandlerConfig, rl *rateLimiter, w http.ResponseWriter, r *http.Request) {
	// Rate limit before reading credentials.
	addr := r.RemoteAddr
	if host, _, err := net.SplitHostPort(addr); err == nil {
		addr = host
	}
	delay := rl.record(addr)
	if delay > 0 {
		time.Sleep(delay)
	}

	// Cap request body size. http.MaxBytesReader returns a 413 if the body
	// exceeds the limit and rejects the partial parse that a bare LimitReader
	// would allow.
	r.Body = http.MaxBytesReader(w, r.Body, loginMaxBodyBytes)

	// Check Origin / Sec-Fetch-Site to reject cross-origin POST.
	if cfg.AdminOrigin != "" {
		origin := r.Header.Get("Origin")
		if origin != "" && origin != cfg.AdminOrigin {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		site := r.Header.Get("Sec-Fetch-Site")
		if site != "" && site != "same-origin" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	// Parse form.
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Verify CSRF token.
	csrfToken := r.FormValue("csrf_token")
	if csrfToken == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Look up the stored CSRF hash from the cookie.
	csrfCookie, err := r.Cookie(csrfCookieName)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	expectedHash := csrfCookieHash(csrfToken)
	if subtle.ConstantTimeCompare([]byte(expectedHash), []byte(csrfCookie.Value)) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Verify admin token using constant-time comparison over SHA-256.
	submittedToken := r.FormValue("token")
	submittedHash := sha256.Sum256([]byte(submittedToken))
	submittedHex := hex.EncodeToString(submittedHash[:])

	if subtle.ConstantTimeCompare([]byte(submittedHex), []byte(cfg.TokenHash)) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Generate session id from crypto/rand.
	sessionID := make([]byte, 32)
	if _, err := rand.Read(sessionID); err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}
	sessionIDHex := hex.EncodeToString(sessionID)

	// Hash the hex-encoded session id (same value the cookie carries) for storage.
	sessionHash := sha256.Sum256([]byte(sessionIDHex))

	// Compute token fingerprint for the session row.
	tokenFP, err := TokenFingerprint(cfg.TokenHash)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	now := time.Now()
	expiresAt := now.Add(sessionLifetime)

	// Delete any existing session from this user (prevent session fixation).
	// Read the old session cookie from the request, hash it, and delete.
	// If deletion fails, abort login: do not create the new session.
	if oldCookie, err := r.Cookie(sessionCookieName); err == nil {
		oldHash := sha256.Sum256([]byte(oldCookie.Value))
		_, delErr := cfg.Store.DeleteAdminSession(r.Context(), oldHash[:])
		if delErr != nil {
			cfg.Logger.Error("delete old session during login rotation", "err", delErr)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
	}

	// Create new session.
	if err := cfg.Store.CreateAdminSession(r.Context(), sessionHash[:], tokenFP, expiresAt, now); err != nil {
		cfg.Logger.Error("create admin session", "err", err)
		http.Error(w, "internal server error", http.StatusInternalServerError)
		return
	}

	// Clear CSRF cookie.
	http.SetCookie(w, clearCSRFCookie())

	// Set session cookie.
	cookie := SessionCookie(sessionIDHex)
	http.SetCookie(w, cookie)

	// Redirect to metrics dashboard.
	http.Redirect(w, r, "/admin/metrics", http.StatusFound)
}

// handleLogoutPOST processes logout requests.
func handleLogoutPOST(cfg LoginHandlerConfig, w http.ResponseWriter, r *http.Request) {
	// Cap request body size before any auth check (same cap as login).
	r.Body = http.MaxBytesReader(w, r.Body, loginMaxBodyBytes)

	// Parse the form up front so an oversized body is rejected (413) before
	// any auth check runs.
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusRequestEntityTooLarge)
		return
	}

	// Origin check (same as login).
	if cfg.AdminOrigin != "" {
		origin := r.Header.Get("Origin")
		if origin != "" && origin != cfg.AdminOrigin {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		site := r.Header.Get("Sec-Fetch-Site")
		if site != "" && site != "same-origin" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
	}

	// Look up session from cookie.
	sessionCookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// CSRF check for logout: recompute the expected token from the session
	// cookie and compare in constant time. The token is derived, not stored,
	// so it cannot be invalidated by a second tab or a cookie expiry.
	csrfToken := r.FormValue("csrf_token")
	expected, derr := SessionCSRFToken(cfg.TokenHash, sessionCookie.Value)
	if derr != nil {
		// Invalid TokenHash is a startup misconfiguration, not a client
		// error: same status as DashboardHandler for the same condition.
		cfg.Logger.Error("derive logout csrf token", "err", derr)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if csrfToken == "" || subtle.ConstantTimeCompare([]byte(csrfToken), []byte(expected)) != 1 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Hash and delete session. Validate the session exists first: if it is
	// already gone (expired, revoked, or never valid), reject with 401.
	sessionHash := sha256.Sum256([]byte(sessionCookie.Value))
	_, err = cfg.Store.GetAdminSession(r.Context(), sessionHash[:])
	if err != nil {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	n, err := cfg.Store.DeleteAdminSession(r.Context(), sessionHash[:])
	if err != nil || n == 0 {
		cfg.Logger.Error("delete admin session on logout", "err", err)
		w.WriteHeader(http.StatusUnauthorized)
		return
	}

	// Clear the session cookie. The pre-auth CSRF cookie is only set by the
	// login form and does not exist for authenticated sessions.
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1,
	})

	http.Redirect(w, r, "/admin/login", http.StatusFound)
}

// SecurityHeaders is middleware that adds security response headers to all
// admin responses. It sets Cache-Control: no-store, X-Content-Type-Options:
// nosniff, and Content-Security-Policy: frame-ancestors 'none'. It never
// sets Access-Control-Allow-Origin or any CORS header.
func SecurityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}

// AdminRouter builds the admin HTTP router with login, logout, and protected
// routes. It returns an http.Handler that serves on /admin/* with proper auth,
// CSRF protection, and security headers. The returned handler also implements
// ProtectedRoutes() so the gate test can enumerate protected paths without
// hard-coding them.
func AdminRouter(cfg LoginHandlerConfig, registry *mtproto.SessionRegistry) http.Handler {
	rl := newRateLimiter()

	// Protected routes (behind RequireAdmin). Track registered patterns so
	// the gate test can enumerate them without hard-coding.
	var protectedPatterns []string
	protectedMux := http.NewServeMux()
	registerProtected := func(pattern string, handler http.HandlerFunc) {
		protectedPatterns = append(protectedPatterns, pattern)
		protectedMux.HandleFunc(pattern, handler)
	}
	registerProtected("GET /admin/metrics", Handler(registry, cfg.Store))
	registerProtected("GET /admin/dashboard", DashboardHandler(registry, cfg.Store, cfg.TokenHash))
	registerProtected("GET /admin/events", EventsHandler(cfg.Events))

	// Top-level mux: specific public routes registered first (they take priority
	// over the prefix match), then the catch-all protected prefix.
	mux := http.NewServeMux()

	// Dashboard CSS — public so it loads before authentication redirects.
	mux.HandleFunc("GET /admin/assets/dashboard.css", func(w http.ResponseWriter, r *http.Request) {
		data, err := assets.FS.ReadFile("dashboard.css")
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/css; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		if _, err := w.Write(data); err != nil {
			return
		}
	})
	// Component JS bundle — public so the browser can fetch it before the session check.
	mux.HandleFunc("GET /admin/assets/shadcn-templ.js", func(w http.ResponseWriter, r *http.Request) {
		data, err := assets.FS.ReadFile("shadcn_templ.js")
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
		w.Header().Set("Cache-Control", "no-cache")
		if _, err := w.Write(data); err != nil {
			return
		}
	})
	// htmx and SSE extension — public; loaded before the session check.
	for name, file := range map[string]string{
		"GET /admin/assets/htmx.min.js":         "htmx.min.js",
		"GET /admin/assets/htmx-ext-sse.min.js": "htmx-ext-sse.min.js",
	} {
		mux.HandleFunc(name, func(w http.ResponseWriter, r *http.Request) {
			data, err := assets.FS.ReadFile(file)
			if err != nil {
				http.Error(w, "not found", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			if _, err := w.Write(data); err != nil {
				return
			}
		})
	}
	// Public login form (GET).
	mux.HandleFunc("GET /admin/login", func(w http.ResponseWriter, r *http.Request) {
		handleLoginGET(cfg, w, r)
	})
	// Public login submission (POST).
	mux.HandleFunc("POST /admin/login", func(w http.ResponseWriter, r *http.Request) {
		handleLoginPOST(cfg, rl, w, r)
	})
	// Public logout (POST).
	mux.HandleFunc("POST /admin/logout", func(w http.ResponseWriter, r *http.Request) {
		handleLogoutPOST(cfg, w, r)
	})
	// All other /admin/* routes require a valid session.
	mux.Handle("/admin/", RequireAdmin(AdminMiddlewareConfig{
		Store:     cfg.Store,
		TokenHash: cfg.TokenHash,
	})(protectedMux))

	h := SecurityHeaders(mux)
	// Wrap with protected routes metadata for the gate test.
	return &adminRouter{Handler: h, patterns: protectedPatterns}
}

// adminRouter wraps the admin http.Handler and exposes the list of protected
// route patterns for the gate test.
type adminRouter struct {
	http.Handler

	patterns []string
}

// ProtectedRoutes returns the list of protected route patterns registered on
// the admin sub-mux. Used by the gate test to enumerate routes without
// hard-coding them.
func (r *adminRouter) ProtectedRoutes() []string {
	return r.patterns
}
