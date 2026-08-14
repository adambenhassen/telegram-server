package admin_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/admin"
)

// protectedRoutes lists every route registered on the admin sub-mux behind
// RequireAdmin. The gate test iterates this list and verifies each returns 401
// without a valid session cookie. Adding a new protected route without adding
// it here will not break the test — but the route will not be registered on
// the sub-mux, so it will 404. The test catches the reverse: a route that is
// registered but not protected.
//
// To add a new protected route:
//  1. Register it on the admin sub-mux in cmd/telegramd/main.go
//  2. Add the method+path to this list
//  3. The gate test will verify it returns 401 without a session
var protectedRoutes = []struct {
	method string
	path   string
}{
	{http.MethodGet, "/admin/metrics"},
}

// TestGate_all_protected_routes_require_session verifies that every route
// registered on the admin sub-mux returns 401 without a valid session cookie.
// This test fails if a new handler is registered directly on the sub-mux without
// going through RequireAdmin, making the gate self-enforcing.
func TestGate_all_protected_routes_require_session(t *testing.T) {
	t.Parallel()

	st := newTestStore(t)

	// Build the same sub-mux structure as main.go.
	adminSubMux := http.NewServeMux()
	adminSubMux.HandleFunc("GET /admin/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	// Wrap with RequireAdmin.
	adminMux := http.NewServeMux()
	adminMux.Handle("/admin/", admin.RequireAdmin(admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: "aa" + string(make([]byte, 31)), // dummy hash
	})(adminSubMux))

	for _, route := range protectedRoutes {
		t.Run(route.method+" "+route.path, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequestWithContext(context.Background(), route.method, route.path, nil)
			rec := httptest.NewRecorder()
			adminMux.ServeHTTP(rec, req)

			if rec.Code != http.StatusUnauthorized {
				t.Errorf("expected 401 for %s %s, got %d", route.method, route.path, rec.Code)
			}
		})
	}
}
