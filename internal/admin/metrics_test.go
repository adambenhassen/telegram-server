package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestMetricsHandler(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	registry := mtproto.NewSessionRegistry()
	h := admin.Handler(registry, st)

	// GET /admin/metrics returns 200 with valid JSON.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp admin.MetricsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	// Snapshot should have a recent timestamp.
	if time.Since(resp.Timestamp) > time.Minute {
		t.Fatalf("timestamp too old: %v", resp.Timestamp)
	}

	// Empty database should return zeros.
	if resp.Connections != 0 {
		t.Errorf("expected 0 connections, got %d", resp.Connections)
	}
	if resp.Sessions != 0 {
		t.Errorf("expected 0 sessions, got %d", resp.Sessions)
	}
	if resp.TotalUsers != 0 {
		t.Errorf("expected 0 total users, got %d", resp.TotalUsers)
	}
	if resp.TotalChannels != 0 {
		t.Errorf("expected 0 total channels, got %d", resp.TotalChannels)
	}
	if resp.TotalChats != 0 {
		t.Errorf("expected 0 total chats, got %d", resp.TotalChats)
	}
	if resp.MaxPtsGap != 0 {
		t.Errorf("expected 0 max pts gap, got %d", resp.MaxPtsGap)
	}

	// Push latency fields should be zero (placeholder).
	if resp.PushLatencyP50 != 0 {
		t.Errorf("expected push_latency_p50_ms = 0, got %f", resp.PushLatencyP50)
	}
	if resp.PushLatencyP95 != 0 {
		t.Errorf("expected push_latency_p95_ms = 0, got %f", resp.PushLatencyP95)
	}

	// Uninstrumented fields must be listed so the dashboard can distinguish
	// a genuine zero from an absent instrument.
	want := []string{"notify_count", "push_latency_p50_ms", "push_latency_p95_ms"}
	if len(resp.Uninstrumented) != len(want) {
		t.Fatalf("expected %d uninstrumented fields, got %d: %v", len(want), len(resp.Uninstrumented), resp.Uninstrumented)
	}
	for i, name := range want {
		if resp.Uninstrumented[i] != name {
			t.Errorf("uninstrumented[%d] = %q, want %q", i, resp.Uninstrumented[i], name)
		}
	}
}

func TestMetricsHandler_POST(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	registry := mtproto.NewSessionRegistry()
	h := admin.Handler(registry, st)

	// POST should be rejected.
	req := httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestMetricsWithUserData(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	ctx := context.Background()

	// Create a user.
	u, err := st.CreateUser(ctx, "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Create a chat with the user as creator.
	if _, err := st.CreateChat(ctx, u.ID, "Test Chat", []int64{u.ID}); err != nil {
		t.Fatal(err)
	}

	registry := mtproto.NewSessionRegistry()
	h := admin.Handler(registry, st)

	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}

	var resp admin.MetricsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if resp.TotalUsers != 1 {
		t.Errorf("expected 1 total user, got %d", resp.TotalUsers)
	}
	if resp.TotalChats != 1 {
		t.Errorf("expected 1 total chat, got %d", resp.TotalChats)
	}
	// Storage rows should reflect the data.
	if resp.StorageRows.Users != 1 {
		t.Errorf("expected 1 user row, got %d", resp.StorageRows.Users)
	}
	if resp.StorageRows.Chats != 1 {
		t.Errorf("expected 1 chat row, got %d", resp.StorageRows.Chats)
	}
}
