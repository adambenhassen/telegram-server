package admin

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// sessionCookieName is the name of the cookie that carries an admin session id.
// The __Host- prefix enforces Secure, HttpOnly, Path=/, and no Domain attribute
// at the browser level.
const sessionCookieName = "__Host-admin-session"

// sessionLifetime is how long an admin session is valid from creation.
const sessionLifetime = 24 * time.Hour

// idleTimeout is how long an admin session can be inactive before it is
// considered expired. Every valid request updates the last-activity timestamp,
// so an idle session is rejected before this period passes.
const idleTimeout = 30 * time.Minute

// AdminMiddlewareConfig holds the dependencies required by RequireAdmin.
type AdminMiddlewareConfig struct {
	// Store is the Postgres-backed store used for session lookups.
	Store *store.Store
	// TokenHash is the current hex-encoded SHA-256 digest of TG_ADMIN_TOKEN_HASH.
	// It is compared against the token_fingerprint stored in each session row.
	// Changing the env var (and restarting) invalidates all existing sessions.
	TokenHash string
}

// RequireAdmin returns middleware that validates admin sessions backed by
// Postgres session storage. A valid session cookie grants access; any other
// request gets 401 with an empty body.
//
// On a valid session the last-activity timestamp is updated and the request
// passes through. Rotating TG_ADMIN_TOKEN_HASH (changing the env var and
// restarting) invalidates all existing sessions on the next request: the
// fingerprint check fails and each affected session returns 401.
//
// Nothing in this middleware logs the raw session id or the token hash.
func RequireAdmin(cfg AdminMiddlewareConfig) func(http.Handler) http.Handler {
	tokenFingerprint, err := hex.DecodeString(cfg.TokenHash)
	if err != nil {
		// Invalid config: log and reject all requests. This is a startup
		// misconfiguration (TG_ADMIN_TOKEN_HASH is not valid hex) that the
		// config validator should have caught.
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusUnauthorized)
			})
		}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cookie, err := r.Cookie(sessionCookieName)
			if err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Hash the session id before looking it up. The raw cookie value
			// never touches the database or the logs.
			sessionHash := sha256.Sum256([]byte(cookie.Value))

			row, err := cfg.Store.GetAdminSession(r.Context(), sessionHash[:])
			if err != nil {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			now := time.Now()

			// Check absolute expiry.
			if now.After(row.ExpiresAt) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Check idle timeout.
			if now.Sub(row.LastActivity) > idleTimeout {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Check token fingerprint — changing TG_ADMIN_TOKEN_HASH
			// invalidates all sessions on the next request.
			if !constantTimeEquals(row.TokenFingerprint, tokenFingerprint) {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Session valid — update last activity. A write failure here is
			// non-fatal: the request is already validated and the session will
			// be cleaned up by the sweep when it expires.
			_ = cfg.Store.UpdateAdminSessionActivity(r.Context(), sessionHash[:], now) //nolint:errcheck // best-effort activity update

			next.ServeHTTP(w, r)
		})
	}
}

// constantTimeEquals compares two byte slices in constant time to prevent
// timing attacks on the fingerprint comparison.
func constantTimeEquals(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	result := 0
	for i := range a {
		result |= int(a[i] ^ b[i])
	}
	return result == 0
}

// HashSessionID returns the hex-encoded SHA-256 of a session id. It is used
// by the login handler (stage 3c) to store the session.
func HashSessionID(sessionID string) string {
	h := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(h[:])
}

// SessionCookieName returns the cookie name used for admin sessions.
func SessionCookieName() string {
	return sessionCookieName
}

// SessionLifetime returns the absolute lifetime of an admin session.
func SessionLifetime() time.Duration {
	return sessionLifetime
}
