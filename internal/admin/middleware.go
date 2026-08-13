package admin

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
)

// RequireAdmin wraps an http.Handler with token-based authentication.
//
// It compares the hex-encoded SHA-256 digest of the Authorization header value
// against the stored token hash. On mismatch (or missing header) it returns 401
// with an empty body and stops processing.
func RequireAdmin(tokenHash string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token := r.Header.Get("Authorization")
			if token == "" {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			digest := sha256.Sum256([]byte(token))
			if subtle.ConstantTimeCompare([]byte(hex.EncodeToString(digest[:])), []byte(tokenHash)) != 1 {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
