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

func TestMetricsMaxPtsGapUsesRecentActivity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	activeA := createMetricsUser(t, st, "+15550000102")
	activeB := createMetricsUser(t, st, "+15550000103")
	sinkA := createMetricsUser(t, st, "+15550000104")
	sendMetricsMessages(t, ctx, st, activeA.ID, sinkA.ID, 2)
	if err := st.SetUserStatus(ctx, activeA.ID, true); err != nil {
		t.Fatalf("mark active user A online: %v", err)
	}
	if err := st.SetUserStatus(ctx, activeB.ID, true); err != nil {
		t.Fatalf("mark active user B online: %v", err)
	}

	registry := mtproto.NewSessionRegistry()

	resp := requestMetrics(t, ctx, admin.Handler(registry, st))
	if resp.MaxPtsGap != 2 {
		t.Errorf("expected recent activity pts spread 2, got %d", resp.MaxPtsGap)
	}
}

func TestMetricsMaxPtsGapNoRecentActivity(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	first := createMetricsUser(t, st, "+15550000111")
	second := createMetricsUser(t, st, "+15550000112")
	sink := createMetricsUser(t, st, "+15550000113")
	if _, _, _, _, err := st.SendMessage(ctx, first.ID, second.ID, "first", 1, 0, 0); err != nil {
		t.Fatalf("send first metrics message: %v", err)
	}
	if _, _, _, _, err := st.SendMessage(ctx, first.ID, sink.ID, "second", 2, 0, 0); err != nil {
		t.Fatalf("send second metrics message: %v", err)
	}
	registry := mtproto.NewSessionRegistry()
	if !registry.Add(first.ID, &mtproto.Conn{}) {
		t.Fatal("register first user")
	}
	if !registry.Add(sink.ID, &mtproto.Conn{}) {
		t.Fatal("register sink user")
	}

	resp := requestMetrics(t, ctx, admin.Handler(registry, st))
	if resp.MaxPtsGap != 0 {
		t.Errorf("expected no recent activity to report 0, got %d", resp.MaxPtsGap)
	}
}

func createMetricsUser(t *testing.T, st *store.Store, phone string) store.User {
	t.Helper()
	u, err := st.CreateUser(context.Background(), phone)
	if err != nil {
		t.Fatalf("create metrics user: %v", err)
	}
	return u
}

func sendMetricsMessages(t *testing.T, ctx context.Context, st *store.Store, fromID, toID int64, count int) {
	t.Helper()
	for i := range count {
		if _, _, _, _, err := st.SendMessage(ctx, fromID, toID, "metrics", int64(i+1), 0, 0); err != nil {
			t.Fatalf("send metrics message %d: %v", i+1, err)
		}
	}
}

func requestMetrics(t *testing.T, ctx context.Context, h http.Handler) admin.MetricsResponse {
	t.Helper()
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}
	var resp admin.MetricsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode metrics response: %v", err)
	}
	return resp
}
