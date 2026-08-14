package admin_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// newTestStore opens a store and returns it along with a cleanup function.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() }) //nolint:errcheck // best-effort
	return st
}

// seedSession inserts a valid admin session into the store. Returns the raw
// session id (the value the cookie carries).
func seedSession(t *testing.T, ctx context.Context, st *store.Store, tokenHash string, expiresAt, lastActivity time.Time) string {
	t.Helper()
	// Generate a deterministic session id for the test.
	sessionID := "test-session-id-12345"
	sessionHash := sha256.Sum256([]byte(sessionID))
	tokenFingerprint, err := hex.DecodeString(tokenHash)
	if err != nil {
		t.Fatalf("invalid token hash: %v", err)
	}
	if err := st.CreateAdminSession(ctx, sessionHash[:], tokenFingerprint, expiresAt, lastActivity); err != nil {
		t.Fatalf("create admin session: %v", err)
	}
	return sessionID
}

func TestRequireAdmin_valid_session_passes(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	ctx := context.Background()
	tokenHash := hex.EncodeToString(make([]byte, 32)) // 64 hex chars

	sessionID := seedSession(t, ctx, st, tokenHash,
		time.Now().Add(admin.SessionLifetime()),
		time.Now(),
	)

	cfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: tokenHash,
	}
	h := admin.RequireAdmin(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie — simulates what the browser sends under the __Host- prefix
		Name:  admin.SessionCookieName(),
		Value: sessionID,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Fatalf("expected 418 (handler reached), got %d", rec.Code)
	}
}

func TestRequireAdmin_no_cookie_401(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	cfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: hex.EncodeToString(make([]byte, 32)),
	}
	h := admin.RequireAdmin(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be reached")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("body should be empty, got %d bytes", rec.Body.Len())
	}
}

func TestRequireAdmin_session_absent_401(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)

	cfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: hex.EncodeToString(make([]byte, 32)),
	}
	h := admin.RequireAdmin(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be reached")
	}))

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie — simulates what the browser sends
		Name:  admin.SessionCookieName(),
		Value: "nonexistent-session",
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireAdmin_expired_session_401(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	ctx := context.Background()
	tokenHash := hex.EncodeToString(make([]byte, 32))

	sessionID := seedSession(t, ctx, st, tokenHash,
		time.Now().Add(-time.Hour), // expired
		time.Now(),
	)

	cfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: tokenHash,
	}
	h := admin.RequireAdmin(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be reached")
	}))

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie — simulates what the browser sends
		Name:  admin.SessionCookieName(),
		Value: sessionID,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireAdmin_idle_timeout_401(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	ctx := context.Background()
	tokenHash := hex.EncodeToString(make([]byte, 32))

	sessionID := seedSession(t, ctx, st, tokenHash,
		time.Now().Add(admin.SessionLifetime()), // not expired
		time.Now().Add(-31*time.Minute),         // idle > 30 min
	)

	cfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: tokenHash,
	}
	h := admin.RequireAdmin(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be reached")
	}))

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie — simulates what the browser sends
		Name:  admin.SessionCookieName(),
		Value: sessionID,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireAdmin_fingerprint_mismatch_401(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	ctx := context.Background()
	tokenHash := "aa" + hex.EncodeToString(make([]byte, 31))
	differentHash := "bb" + hex.EncodeToString(make([]byte, 31))

	sessionID := seedSession(t, ctx, st, tokenHash,
		time.Now().Add(admin.SessionLifetime()),
		time.Now(),
	)

	// Middleware uses a different token hash (simulates env var rotation).
	cfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: differentHash,
	}
	h := admin.RequireAdmin(cfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be reached")
	}))

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie — simulates what the browser sends
		Name:  admin.SessionCookieName(),
		Value: sessionID,
	})
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d", rec.Code)
	}
}

func TestRequireAdmin_token_rotation_invalidates_sessions(t *testing.T) {
	t.Parallel()
	st := newTestStore(t)
	ctx := context.Background()
	oldTokenHash := hex.EncodeToString(make([]byte, 32))
	oldTokenHash = "aa" + oldTokenHash[2:]
	newTokenHash := hex.EncodeToString(make([]byte, 32))
	newTokenHash = "bb" + newTokenHash[2:]

	sessionID := seedSession(t, ctx, st, oldTokenHash,
		time.Now().Add(admin.SessionLifetime()),
		time.Now(),
	)

	// Old token works.
	oldCfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: oldTokenHash,
	}
	oldH := admin.RequireAdmin(oldCfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{ //nolint:gosec // G124: test cookie — simulates what the browser sends
		Name:  admin.SessionCookieName(),
		Value: sessionID,
	})
	rec := httptest.NewRecorder()
	oldH.ServeHTTP(rec, req)
	if rec.Code != http.StatusTeapot {
		t.Fatalf("with old token: expected 418, got %d", rec.Code)
	}

	// New token rejects the old session.
	newCfg := admin.AdminMiddlewareConfig{
		Store:     st,
		TokenHash: newTokenHash,
	}
	newH := admin.RequireAdmin(newCfg)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("handler should not be reached")
	}))

	rec = httptest.NewRecorder()
	newH.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("with new token: expected 401, got %d", rec.Code)
	}
}

func TestSessionCookieName_prefix(t *testing.T) {
	t.Parallel()
	name := admin.SessionCookieName()
	if !strings.HasPrefix(name, "__Host-") {
		t.Errorf("cookie name %q does not have __Host- prefix", name)
	}
}
