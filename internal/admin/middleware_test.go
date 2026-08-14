package admin_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/admin"
)

func TestRequireAdmin_stub_always_401(t *testing.T) {
	t.Parallel()
	h := admin.RequireAdmin("any-hash")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot) // must never be reached
	}))

	// No token — 401.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("without token: expected 401, got %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body should be empty, got %d bytes", rec.Body.Len())
	}
}

func TestRequireAdmin_stub_valid_token_401(t *testing.T) {
	t.Parallel()
	h := admin.RequireAdmin("any-hash")(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	// Valid-looking token still returns 401 (stub, not real auth).
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	req.Header.Set("Authorization", "secret")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("with token: expected 401, got %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body should be empty, got %d bytes", rec.Body.Len())
	}
}
