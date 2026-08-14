package admin

import "net/http"

// RequireAdmin is a stub that returns 401 with an empty body for every request.
//
// It is not yet a real auth middleware — token validation is wired in a later
// stage after the admin server skeleton is complete. At this stage the
// acceptance criterion is that no path on the admin listener is reachable
// without passing through a 401 response.
func RequireAdmin(_ string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusUnauthorized)
		})
	}
}
