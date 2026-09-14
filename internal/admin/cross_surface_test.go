package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type pushSurfacePayload struct {
	PushLatencyP50                 float64            `json:"push_latency_p50_ms"`
	PushLatencyP50Overflow         bool               `json:"push_latency_p50_overflow"`
	PushLatencyP95                 float64            `json:"push_latency_p95_ms"`
	PushLatencyP95Overflow         bool               `json:"push_latency_p95_overflow"`
	PushLatencySampleCount         int64              `json:"push_latency_sample_count"`
	PushWindowSeconds              float64            `json:"push_window_seconds"`
	PushOutcomes                   admin.PushOutcomes `json:"push_outcomes"`
	PushLatencyBucketUpperBoundsMS [15]float64        `json:"push_latency_bucket_upper_bounds_ms"`
	PushLatencyBucketCounts        [16]int64          `json:"push_latency_bucket_counts"`
}

var pushSurfaceKeys = []string{
	"push_latency_p50_ms",
	"push_latency_p50_overflow",
	"push_latency_p95_ms",
	"push_latency_p95_overflow",
	"push_latency_sample_count",
	"push_window_seconds",
	"push_outcomes",
	"push_latency_bucket_upper_bounds_ms",
	"push_latency_bucket_counts",
}

func TestAuthenticatedJSONAndSSESharePushSnapshot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newAuthTestStore(t)
	registry := mtproto.NewSessionRegistry()
	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })
	now = start.Add(24 * time.Millisecond)
	metrics.RecordPushOutcome(store.PushOutcomeSuccess, start)
	now = start.Add(42 * time.Second)
	metrics.RecordPushOutcome(store.PushOutcomeOwnerMismatch, now)
	recorded := metrics.Snapshot().Push

	want := pushSurfacePayload{
		PushLatencyP50:         50,
		PushLatencyP50Overflow: false,
		PushLatencyP95:         50,
		PushLatencyP95Overflow: false,
		PushLatencySampleCount: 1,
		PushWindowSeconds:      42,
		PushOutcomes: admin.PushOutcomes{
			Success:       1,
			OwnerMismatch: 1,
		},
		PushLatencyBucketUpperBoundsMS: [15]float64{1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000},
		PushLatencyBucketCounts:        [16]int64{0, 0, 0, 0, 0, 1},
	}
	if recorded.P50Milliseconds != want.PushLatencyP50 || recorded.P95Milliseconds != want.PushLatencyP95 ||
		recorded.SampleCount != want.PushLatencySampleCount || recorded.WindowSeconds != want.PushWindowSeconds ||
		recorded.Outcomes != (store.PushOutcomeCounts{Success: 1, OwnerMismatch: 1}) ||
		recorded.LatencyBucketUpperBoundsMilliseconds != want.PushLatencyBucketUpperBoundsMS ||
		recorded.LatencyBucketCounts != want.PushLatencyBucketCounts {
		t.Fatalf("recorder snapshot = %+v, want %+v", recorded, want)
	}

	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample:            admin.NewMetricsSampler(registry, st, metrics),
		Render:            admin.DefaultFragmentRenderer,
		Interval:          time.Hour,
		Heartbeat:         time.Hour,
		MaxStreamDuration: 100 * time.Millisecond,
	})
	rawToken := "cross-surface-token"
	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:         st,
		TokenHash:     sha256hex([]byte(rawToken)),
		Logger:        slog.New(slog.DiscardHandler),
		Events:        b,
		NotifyMetrics: metrics,
	}, registry)

	baseline := requestMetrics(t, ctx, admin.Handler(registry, st))
	wantUninstrumented := []string{"push_latency_p50_ms", "push_latency_p95_ms"}
	if !slices.Equal(baseline.Uninstrumented, wantUninstrumented) {
		t.Fatalf("uninstrumented baseline = %v, want %v", baseline.Uninstrumented, wantUninstrumented)
	}

	sessionID := loginAndGetSession(t, h, rawToken)
	jsonBody, jsonResponse := authenticatedMetrics(t, h, sessionID)
	if len(jsonResponse.Uninstrumented) != 0 {
		t.Fatalf("instrumented uninstrumented = %v, want only the two push percentile names removed", jsonResponse.Uninstrumented)
	}

	gotJSON := decodePushSurface(t, "JSON", pushFields(t, "JSON", []byte(jsonBody), true))
	if gotJSON != want {
		t.Fatalf("JSON push payload = %+v, want %+v", gotJSON, want)
	}

	sseServer := httptest.NewServer(h)
	t.Cleanup(sseServer.Close)
	sseReq, err := http.NewRequestWithContext(ctx, http.MethodGet, sseServer.URL+"/admin/events", nil)
	if err != nil {
		t.Fatalf("new authenticated SSE request: %v", err)
	}
	sseReq.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	sseResponse, err := http.DefaultClient.Do(sseReq)
	if err != nil {
		t.Fatalf("authenticated SSE request: %v", err)
	}
	defer func() { _ = sseResponse.Body.Close() }() //nolint:errcheck // best-effort close
	if sseResponse.StatusCode != http.StatusOK {
		t.Fatalf("authenticated SSE status = %d, want 200", sseResponse.StatusCode)
	}
	sseBody := readSSEUntil(t, sseResponse.Body, `<script id="push-telemetry" type="application/json">`, 5*time.Second)
	sseJSON := extractPushTelemetryJSON(t, sseBody)
	gotSSE := decodePushSurface(t, "SSE", pushFields(t, "SSE", sseJSON, false))
	if gotSSE != gotJSON {
		t.Fatalf("SSE push payload = %+v, JSON = %+v", gotSSE, gotJSON)
	}
	if gotSSE != want {
		t.Fatalf("SSE push payload = %+v, want %+v", gotSSE, want)
	}
}

func authenticatedMetrics(t *testing.T, h http.Handler, sessionID string) (string, admin.MetricsResponse) {
	t.Helper()
	req := httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/admin/metrics", nil)
	req.AddCookie(&http.Cookie{Name: "__Host-admin-session", Value: sessionID}) //nolint:gosec // G124: test cookie
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("authenticated metrics status = %d, want 200", rec.Code)
	}
	var response admin.MetricsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode authenticated metrics: %v", err)
	}
	return rec.Body.String(), response
}

func pushFields(t *testing.T, label string, body []byte, endpoint bool) map[string]json.RawMessage {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(body, &root); err != nil {
		t.Fatalf("decode %s JSON: %v", label, err)
	}
	fields := make(map[string]json.RawMessage, len(pushSurfaceKeys))
	for _, key := range pushSurfaceKeys {
		raw, ok := root[key]
		if !ok {
			t.Fatalf("%s payload missing %q", label, key)
		}
		fields[key] = raw
	}
	pushKeyCount := 0
	for key := range root {
		if !strings.HasPrefix(key, "push_") {
			continue
		}
		pushKeyCount++
		if !slices.Contains(pushSurfaceKeys, key) {
			t.Fatalf("%s payload has unexpected push key %q", label, key)
		}
	}
	if pushKeyCount != len(pushSurfaceKeys) {
		t.Fatalf("%s payload has %d push keys, want %d", label, pushKeyCount, len(pushSurfaceKeys))
	}
	if !endpoint && len(root) != len(pushSurfaceKeys) {
		t.Fatalf("%s push payload keys = %v, want exactly %v", label, sortedKeys(root), pushSurfaceKeys)
	}
	return fields
}

func decodePushSurface(t *testing.T, label string, fields map[string]json.RawMessage) pushSurfacePayload {
	t.Helper()
	var outcomes map[string]json.RawMessage
	if err := json.Unmarshal(fields["push_outcomes"], &outcomes); err != nil {
		t.Fatalf("decode %s push_outcomes: %v", label, err)
	}
	wantOutcomeKeys := []string{"encode_failure", "owner_mismatch", "success", "write_failure"}
	if !slices.Equal(sortedKeys(outcomes), wantOutcomeKeys) {
		t.Fatalf("%s push_outcomes keys = %v, want %v", label, sortedKeys(outcomes), wantOutcomeKeys)
	}
	for _, key := range wantOutcomeKeys {
		var value int64
		if err := json.Unmarshal(outcomes[key], &value); err != nil {
			t.Fatalf("decode %s push_outcomes.%s as integer: %v", label, key, err)
		}
	}
	var bounds []json.RawMessage
	if err := json.Unmarshal(fields["push_latency_bucket_upper_bounds_ms"], &bounds); err != nil {
		t.Fatalf("decode %s bucket bounds: %v", label, err)
	}
	if len(bounds) != 15 {
		t.Fatalf("%s bucket bounds length = %d, want 15", label, len(bounds))
	}
	for i, raw := range bounds {
		var value float64
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatalf("decode %s bucket bound %d as number: %v", label, i, err)
		}
	}
	var counts []json.RawMessage
	if err := json.Unmarshal(fields["push_latency_bucket_counts"], &counts); err != nil {
		t.Fatalf("decode %s bucket counts: %v", label, err)
	}
	if len(counts) != 16 {
		t.Fatalf("%s bucket counts length = %d, want 16", label, len(counts))
	}
	for i, raw := range counts {
		var value int64
		if err := json.Unmarshal(raw, &value); err != nil {
			t.Fatalf("decode %s bucket count %d as integer: %v", label, i, err)
		}
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatalf("encode %s push fields: %v", label, err)
	}
	var payload pushSurfacePayload
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&payload); err != nil {
		t.Fatalf("decode %s push fields with exact types: %v", label, err)
	}
	return payload
}

func sortedKeys(fields map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
