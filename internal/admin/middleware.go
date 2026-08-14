package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// sessionCookieName is the name of the cookie that carries an admin session id.
// The __Host- prefix enforces Secure, Path=/, and no Domain attribute at the
// browser level. HttpOnly and SameSite=Strict are not enforced by the prefix
// and must be set explicitly in the Set-Cookie header (see SessionCookie).
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
	decoded, err := hex.DecodeString(cfg.TokenHash)
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
	// Double-hash: SHA-256 of the decoded hash bytes. This is the fingerprint
	// stored in admin_sessions.token_fingerprint.
	tokenFingerprint := sha256.Sum256(decoded)

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
			if subtle.ConstantTimeCompare(row.TokenFingerprint, tokenFingerprint[:]) != 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			// Session valid — update last activity. A write failure or zero rows
			// updated means the session disappeared between lookup and update
			// (sweep raced ahead), so reject.
			n, err := cfg.Store.UpdateAdminSessionActivity(r.Context(), sessionHash[:], now)
			if err != nil || n != 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// HashSessionID returns the SHA-256 hash of a session id as raw bytes. It is
// used by the login handler (stage 3c) to store the session.
func HashSessionID(sessionID string) []byte {
	h := sha256.Sum256([]byte(sessionID))
	return h[:]
}

// SessionCookie returns a preconfigured http.Cookie for a new admin session.
// The cookie name carries the __Host- prefix, which enforces Secure, Path=/,
// and no Domain at the browser level. The returned cookie also sets HttpOnly
// and SameSite=Strict, which are not enforced by the prefix.
func SessionCookie(sessionID string) *http.Cookie {
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    sessionID,
		Path:     "/",
		Secure:   true,
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	}
}

// SessionCookieName returns the cookie name used for admin sessions.
func SessionCookieName() string {
	return sessionCookieName
}

// SessionLifetime returns the absolute lifetime of an admin session.
func SessionLifetime() time.Duration {
	return sessionLifetime
}
