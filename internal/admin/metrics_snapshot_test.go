package admin_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestProcessIdentityIsFreshPerProcessAndDoesNotInferReplica(t *testing.T) {
	first, err := admin.NewProcessIdentity("")
	if err != nil {
		t.Fatalf("first identity: %v", err)
	}
	second, err := admin.NewProcessIdentity("")
	if err != nil {
		t.Fatalf("second identity: %v", err)
	}
	if first.Generation == second.Generation {
		t.Fatalf("two process generations are equal: %q", first.Generation)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(first.Generation) {
		t.Fatalf("generation = %q, want 32 lowercase hex characters", first.Generation)
	}
	if first.StartedAt.Location() != time.UTC {
		t.Fatalf("started at location = %v, want UTC", first.StartedAt.Location())
	}
	if first.ReplicaID != nil {
		t.Fatalf("empty replica id = %v, want nil", first.ReplicaID)
	}
}

func TestMetricsJSONIncludesSnapshotIdentityMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newAuthTestStore(t)
	h := admin.Handler(mtproto.NewSessionRegistry(), st)

	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want 200", rec.Code)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(rec.Body.Bytes(), &raw); err != nil {
		t.Fatalf("decode metrics JSON: %v", err)
	}

	var timestamp string
	if err := json.Unmarshal(raw["timestamp"], &timestamp); err != nil {
		t.Fatalf("decode timestamp: %v", err)
	}
	parsedTimestamp, err := time.Parse(time.RFC3339Nano, timestamp)
	if err != nil {
		t.Fatalf("timestamp %q is not RFC3339: %v", timestamp, err)
	}
	if parsedTimestamp.Location() != time.UTC {
		t.Errorf("timestamp location = %v, want UTC", parsedTimestamp.Location())
	}

	var age float64
	if err := json.Unmarshal(raw["sample_age_seconds"], &age); err != nil {
		t.Fatalf("decode sample age: %v", err)
	}
	if age < 0 {
		t.Errorf("sample_age_seconds = %v, want nonnegative", age)
	}

	var state string
	if err := json.Unmarshal(raw["sample_state"], &state); err != nil {
		t.Fatalf("decode sample state: %v", err)
	}
	if state != "available" {
		t.Errorf("sample_state = %q, want available", state)
	}

	var startedAt string
	if err := json.Unmarshal(raw["process_started_at"], &startedAt); err != nil {
		t.Fatalf("decode process start: %v", err)
	}
	parsedStartedAt, err := time.Parse(time.RFC3339Nano, startedAt)
	if err != nil {
		t.Fatalf("process_started_at %q is not RFC3339: %v", startedAt, err)
	}
	if parsedStartedAt.Location() != time.UTC {
		t.Errorf("process_started_at location = %v, want UTC", parsedStartedAt.Location())
	}

	var generation string
	if err := json.Unmarshal(raw["process_generation"], &generation); err != nil {
		t.Fatalf("decode process generation: %v", err)
	}
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(generation) {
		t.Errorf("process_generation = %q, want 32 lowercase hex characters", generation)
	}

	if string(raw["replica_id"]) != "null" {
		t.Errorf("replica_id = %s, want null when no identity is configured", raw["replica_id"])
	}
}

func TestMetricsSnapshotCacheRetainsOneCompleteSampleOnRequiredFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() }) //nolint:errcheck // best-effort close
	registry := mtproto.NewSessionRegistry()
	identity, err := admin.NewProcessIdentity("edge-2")
	if err != nil {
		t.Fatalf("new process identity: %v", err)
	}
	notify := newTestNotificationMetrics()
	cache := admin.NewMetricsSnapshotCache(registry, st, identity, nil, notify)

	first, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("first sample: %v", err)
	}
	if first.SampleState != "available" {
		t.Fatalf("first sample state = %q, want available", first.SampleState)
	}
	if first.ReplicaID == nil || *first.ReplicaID != "edge-2" {
		t.Fatalf("first replica id = %v, want edge-2", first.ReplicaID)
	}
	if cached, err := cache.Snapshot(ctx); err != nil {
		t.Fatalf("cached sample: %v", err)
	} else if cached.NotifyCount != first.NotifyCount {
		t.Fatalf("cached notify count = %d, want retained %d", cached.NotifyCount, first.NotifyCount)
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to break required query: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) }) //nolint:errcheck // test cleanup
	if _, err := conn.Exec(ctx, `ALTER TABLE update_state RENAME TO update_state_hidden`); err != nil {
		t.Fatalf("hide update_state: %v", err)
	}
	if _, err := st.MaxPtsGap(ctx); err == nil {
		t.Fatal("MaxPtsGap succeeded after update_state was renamed")
	}
	restored := false
	restore := func() {
		if restored {
			return
		}
		_, _ = conn.Exec(ctx, `ALTER TABLE update_state_hidden RENAME TO update_state`) //nolint:errcheck // best-effort test cleanup
		restored = true
	}
	t.Cleanup(restore)

	if err := notify.RecordValidNotification("tg_updates"); err != nil {
		t.Fatalf("record notification: %v", err)
	}
	if _, err := admin.CollectMetricsStrict(ctx, registry, st); err == nil {
		t.Fatal("strict collection succeeded after update_state was renamed")
	}
	admin.ExpireMetricsSnapshotForTest(cache)
	stale, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("stale sample: %v", err)
	}
	if stale.SampleState != "stale" {
		t.Fatalf("stale sample state = %q, want stale", stale.SampleState)
	}
	if !stale.Timestamp.Equal(first.Timestamp) {
		t.Fatalf("stale timestamp = %s, want retained %s", stale.Timestamp, first.Timestamp)
	}
	if stale.NotifyCount != first.NotifyCount {
		t.Fatalf("stale notify count = %d, want retained %d", stale.NotifyCount, first.NotifyCount)
	}
	if stale.MaxPtsGap != first.MaxPtsGap {
		t.Fatalf("stale max pts gap = %d, want retained %d", stale.MaxPtsGap, first.MaxPtsGap)
	}
	if err := func() error {
		_, err := conn.Exec(ctx, `ALTER TABLE update_state_hidden RENAME TO update_state`)
		return err
	}(); err != nil {
		t.Fatalf("restore update_state: %v", err)
	}
	restored = true
	admin.ExpireMetricsSnapshotForTest(cache)
	recovered, err := cache.Snapshot(ctx)
	if err != nil {
		t.Fatalf("recovered sample: %v", err)
	}
	if recovered.SampleState != "available" {
		t.Fatalf("recovered sample state = %q, want available", recovered.SampleState)
	}
	if recovered.Timestamp.Equal(first.Timestamp) {
		t.Fatalf("recovered timestamp = %s, want a new sample", recovered.Timestamp)
	}
	if recovered.NotifyCount <= first.NotifyCount {
		t.Fatalf("recovered notify count = %d, want newer than %d", recovered.NotifyCount, first.NotifyCount)
	}
}

func TestMetricsUnavailableBeforeFirstCompleteSampleHasFixedJSONBody(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() }) //nolint:errcheck // best-effort close
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to break required query: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) }) //nolint:errcheck // test cleanup
	if _, err := conn.Exec(ctx, `ALTER TABLE update_state RENAME TO update_state_hidden`); err != nil {
		t.Fatalf("hide update_state: %v", err)
	}
	t.Cleanup(func() {
		_, _ = conn.Exec(ctx, `ALTER TABLE update_state_hidden RENAME TO update_state`) //nolint:errcheck // best-effort test cleanup
	})

	cache := admin.NewMetricsSnapshotCache(mtproto.NewSessionRegistry(), st, admin.ProcessIdentity{}, nil)
	req := httptest.NewRequestWithContext(ctx, http.MethodGet, "/admin/metrics", nil)
	rec := httptest.NewRecorder()
	admin.HandlerWithSnapshotCache(cache).ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("metrics status = %d, want 503", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); !strings.HasPrefix(got, "application/json") {
		t.Fatalf("error content type = %q, want application/json", got)
	}
	if got := rec.Body.String(); got != `{"error":"metrics_unavailable"}` {
		t.Fatalf("error body = %q, want fixed metrics_unavailable body", got)
	}

	dashboardRec := httptest.NewRecorder()
	admin.DashboardHandlerWithSnapshotCache(cache, authTestTokenHash()).ServeHTTP(dashboardRec, req)
	if dashboardRec.Code != http.StatusServiceUnavailable {
		t.Fatalf("dashboard status = %d, want 503", dashboardRec.Code)
	}
	dashboardBody := dashboardRec.Body.String()
	for _, want := range []string{"Reload", "Log out"} {
		if !strings.Contains(dashboardBody, want) {
			t.Fatalf("unavailable dashboard missing %q: %s", want, dashboardBody)
		}
	}
	if strings.Contains(dashboardBody, "SQLSTATE") || strings.Contains(dashboardBody, "relation") || strings.Contains(dashboardBody, "data-metric") {
		t.Fatalf("unavailable dashboard exposed raw error or metric values: %s", dashboardBody)
	}
}

func TestDashboardAndSSECarryTheSameSampleMetadata(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2026, 9, 14, 12, 0, 0, 123000000, time.UTC)
	started := time.Date(2026, 9, 14, 11, 0, 0, 0, time.UTC)
	replica := "edge-2"
	m := admin.MetricsResponse{
		Timestamp:         timestamp,
		SampleAgeSeconds:  12.5,
		SampleState:       "stale",
		ProcessStartedAt:  started,
		ProcessGeneration: "0123456789abcdef0123456789abcdef",
		ReplicaID:         &replica,
	}

	data := admin.BuildDashboardData(m, "csrf")
	var initial strings.Builder
	if err := admin.RenderDashboard(&initial, data); err != nil {
		t.Fatalf("render initial dashboard: %v", err)
	}
	fragments, err := admin.DashboardFragmentRenderer(m)
	if err != nil {
		t.Fatalf("render SSE fragment: %v", err)
	}
	if len(fragments) != 1 {
		t.Fatalf("SSE fragments = %d, want 1", len(fragments))
	}

	for _, body := range []string{initial.String(), fragments[0].HTML} {
		for _, want := range []string{
			`data-sample-timestamp="2026-09-14T12:00:00.123Z"`,
			`data-sample-age-seconds="12.5"`,
			`data-sample-state="stale"`,
			`data-process-started-at="2026-09-14T11:00:00Z"`,
			`data-process-generation="0123456789abcdef0123456789abcdef"`,
			`data-replica-id="edge-2"`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("body missing %s:\n%s", want, body)
			}
		}
	}
	if !strings.Contains(initial.String(), "performance.now()") {
		t.Fatal("dashboard freshness script does not use the monotonic clock")
	}
	if strings.Contains(initial.String(), "Date.now()") {
		t.Fatal("dashboard freshness script uses the wall clock")
	}
}

func TestUnknownUninstrumentedNamesNeverReachDashboardMarkup(t *testing.T) {
	t.Parallel()

	data := admin.BuildDashboardData(admin.MetricsResponse{
		Uninstrumented: []string{"notify_count", "<script>alert(1)</script>", "unknown_metric"},
	}, "csrf")
	if len(data.UninstrumentedNames) != 1 || data.UninstrumentedNames[0].Field != "notify_count" {
		t.Fatalf("filtered uninstrumented names = %+v, want only known notify_count", data.UninstrumentedNames)
	}
	var body strings.Builder
	if err := admin.RenderDashboard(&body, data); err != nil {
		t.Fatalf("render dashboard: %v", err)
	}
	if strings.Contains(body.String(), "unknown_metric") || strings.Contains(body.String(), "<script>alert(1)</script>") {
		t.Fatalf("unknown uninstrumented name reached markup: %s", body.String())
	}
}

func newTestNotificationMetrics() *store.NotificationMetrics {
	return store.NewNotificationMetricsWithClock(time.Now)
}
