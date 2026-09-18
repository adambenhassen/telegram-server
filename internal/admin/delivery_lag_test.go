package admin_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestDeliveryLagSamplerStateTransitions(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 123).UTC()
	sampler := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return start })
	full := admin.PublishDeliveryLagForTest(sampler, 1, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  2,
		WorstPts:            3,
		Complete:            true,
	}, start)
	assertDeliveryLag(t, full, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 2, 2, 3, &start)

	partial := admin.PublishDeliveryLagForTest(sampler, 2, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  1,
		WorstPts:            99,
		Complete:            true,
	}, start.Add(time.Second))
	assertDeliveryLag(t, partial, admin.DeliveryLagStale, admin.DeliveryLagCoveragePartial, 2, 1, 3, &start)

	none := admin.PublishDeliveryLagForTest(sampler, 3, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  0,
		WorstPts:            99,
		Complete:            true,
	}, start.Add(2*time.Second))
	assertDeliveryLag(t, none, admin.DeliveryLagStale, admin.DeliveryLagCoverageNone, 2, 0, 3, &start)

	recoveredAt := start.Add(3 * time.Second)
	recovered := admin.PublishDeliveryLagForTest(sampler, 4, mtproto.DeliveryLagSample{
		EligibleConnections: 1,
		SampledConnections:  1,
		WorstPts:            0,
		Complete:            true,
	}, recoveredAt)
	assertDeliveryLag(t, recovered, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 1, 1, 0, &recoveredAt)

	firstFailure := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return start })
	unavailable := admin.PublishDeliveryLagForTest(firstFailure, 1, mtproto.DeliveryLagSample{
		EligibleConnections: 2,
		SampledConnections:  0,
		Complete:            true,
	}, time.Time{})
	if unavailable.State != admin.DeliveryLagUnavailable || unavailable.Coverage != admin.DeliveryLagCoverageNone ||
		unavailable.WorstPts != nil || unavailable.SampledAt != nil {
		t.Fatalf("first failed sample = %+v, want unavailable with null value and time", unavailable)
	}

	empty := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return start })
	zero := admin.PublishDeliveryLagForTest(empty, 1, mtproto.DeliveryLagSample{Complete: true}, start)
	assertDeliveryLag(t, zero, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 0, 0, 0, &start)
}

func TestDeliveryLagSamplerCallerCancellationDoesNotDowngradeState(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0).UTC()
	sampler := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return start })
	registry := mtproto.NewSessionRegistry()
	if !registry.Add(41, &mtproto.Conn{}) {
		t.Fatal("register connection")
	}
	readHead := func(context.Context, int64) (int64, error) { return 9, nil }
	first := admin.SampleDeliveryLagForTest(sampler, context.Background(), registry, readHead)
	assertDeliveryLag(t, first, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 1, 1, 9, &start)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	second := admin.SampleDeliveryLagForTest(sampler, canceled, registry, func(ctx context.Context, userID int64) (int64, error) {
		if err := ctx.Err(); err != nil {
			t.Fatalf("sampler propagated caller cancellation: %v", err)
		}
		return readHead(ctx, userID)
	})
	assertDeliveryLag(t, second, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 1, 1, 9, &start)
}

func TestDeliveryLagSamplerOverCapReportsPartialCoverage(t *testing.T) {
	t.Parallel()

	const eligible = 1025
	registry := mtproto.NewSessionRegistry()
	for userID := int64(1); userID <= eligible; userID++ {
		if !registry.Add(userID, &mtproto.Conn{}) {
			t.Fatalf("register connection for user %d", userID)
		}
	}

	var calls atomic.Int64
	sampler := admin.NewDeliveryLagSampler()
	got := admin.SampleDeliveryLagForTest(sampler, context.Background(), registry, func(context.Context, int64) (int64, error) {
		calls.Add(1)
		return 1, nil
	})
	if got.State != admin.DeliveryLagUnavailable || got.Coverage != admin.DeliveryLagCoveragePartial {
		t.Fatalf("over-cap state = %+v, want unavailable/partial", got)
	}
	if got.EligibleConnections != eligible || got.SampledConnections != 1024 {
		t.Fatalf("over-cap counts = %+v, want eligible=%d sampled=1024", got, eligible)
	}
	if calls.Load() != 1024 {
		t.Fatalf("over-cap account-head calls = %d, want 1024", calls.Load())
	}
	if got.WorstPts != nil || got.SampledAt != nil {
		t.Fatalf("over-cap first attempt exposed partial values: %+v", got)
	}
}

func TestDeliveryLagJSONAndSSEHaveFixedObject(t *testing.T) {
	t.Parallel()

	sampledAt := time.Unix(1_700_000_000, 0).UTC()
	worstPts := int64(3)
	m := admin.MetricsResponse{DeliveryLag: admin.DeliveryLag{
		WorstPts:            &worstPts,
		State:               admin.DeliveryLagAvailable,
		Coverage:            admin.DeliveryLagCoverageFull,
		EligibleConnections: 2,
		SampledConnections:  2,
		SampledAt:           &sampledAt,
	}}

	encoded, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal metrics: %v", err)
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &root); err != nil {
		t.Fatalf("decode metrics: %v", err)
	}
	jsonObject := decodeDeliveryLagObject(t, root["delivery_lag"])

	fragments, err := admin.DefaultFragmentRenderer(m)
	if err != nil {
		t.Fatalf("render SSE fragment: %v", err)
	}
	const open = `<script id="delivery-lag-telemetry" type="application/json">`
	start := strings.Index(fragments[0].HTML, open)
	if start < 0 {
		t.Fatal("SSE delivery-lag telemetry script missing")
	}
	start += len(open)
	end := strings.Index(fragments[0].HTML[start:], `</script>`)
	if end < 0 {
		t.Fatal("SSE delivery-lag telemetry script is not closed")
	}
	sseObject := decodeDeliveryLagObject(t, json.RawMessage(fragments[0].HTML[start:start+end]))
	if string(jsonObject["sampled_at"]) != string(sseObject["sampled_at"]) ||
		string(jsonObject["worst_pts"]) != string(sseObject["worst_pts"]) {
		t.Fatalf("SSE delivery lag = %v, JSON = %v", sseObject, jsonObject)
	}
}

func TestAuthenticatedJSONAndSSEShareDeliveryLag(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	st := newAuthTestStore(t)
	const phoneMarker = "+1555000742"
	user, err := st.CreateUser(ctx, phoneMarker)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := st.EnsureUpdateState(ctx, user.ID); err != nil {
		t.Fatalf("ensure update state: %v", err)
	}

	registry := mtproto.NewSessionRegistry()
	if !registry.Add(user.ID, &mtproto.Conn{}) {
		t.Fatal("register live connection")
	}
	sampledAt := time.Unix(1_700_000_000, 0).UTC()
	sampler := admin.NewDeliveryLagSamplerWithClock(func() time.Time { return sampledAt })
	b := sseTestBroadcaster(t, admin.BroadcasterConfig{
		Sample:    admin.NewMetricsSamplerWithDeliveryLag(registry, st, sampler),
		Render:    admin.DefaultFragmentRenderer,
		Interval:  time.Hour,
		Heartbeat: time.Hour,
	})
	rawToken := "delivery-lag-cross-surface-token" //nolint:gosec // G101: test credential
	h := admin.AdminRouter(admin.LoginHandlerConfig{
		Store:       st,
		TokenHash:   sha256hex([]byte(rawToken)),
		Logger:      slog.New(slog.DiscardHandler),
		Events:      b,
		DeliveryLag: sampler,
	}, registry)

	sessionID := loginAndGetSession(t, h, rawToken)
	jsonBody, jsonResponse := authenticatedMetrics(t, h, sessionID)
	jsonObject := decodeDeliveryLagObject(t, jsonResponseBytes(t, jsonBody))
	assertDeliveryLag(t, jsonResponse.DeliveryLag, admin.DeliveryLagAvailable, admin.DeliveryLagCoverageFull, 1, 1, 0, &sampledAt)

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
	sseBody := readSSEUntil(t, sseResponse.Body, `<script id="delivery-lag-telemetry" type="application/json">`, 5*time.Second)
	sseObject := decodeDeliveryLagObject(t, extractDeliveryLagTelemetryJSON(t, sseBody))
	for _, key := range []string{"worst_pts", "state", "coverage", "eligible_connections", "sampled_connections", "sampled_at"} {
		if string(jsonObject[key]) != string(sseObject[key]) {
			t.Fatalf("SSE delivery_lag.%s = %s, JSON = %s", key, sseObject[key], jsonObject[key])
		}
	}
	for _, body := range []string{jsonBody, sseBody} {
		if strings.Contains(body, phoneMarker) {
			t.Fatalf("delivery lag output exposed seeded identifier %q", phoneMarker)
		}
	}
}

func assertDeliveryLag(t *testing.T, got admin.DeliveryLag, state admin.DeliveryLagState, coverage admin.DeliveryLagCoverage, eligible, sampled int, worst int64, sampledAt *time.Time) {
	t.Helper()
	if got.State != state || got.Coverage != coverage || got.EligibleConnections != eligible || got.SampledConnections != sampled {
		t.Fatalf("delivery lag metadata = %+v, want state=%q coverage=%q eligible=%d sampled=%d", got, state, coverage, eligible, sampled)
	}
	if got.WorstPts == nil || *got.WorstPts != worst {
		t.Fatalf("delivery lag worst = %v, want %d", got.WorstPts, worst)
	}
	if got.SampledAt == nil || !got.SampledAt.Equal(*sampledAt) {
		t.Fatalf("delivery lag sampled_at = %v, want %v", got.SampledAt, sampledAt)
	}
}

func decodeDeliveryLagObject(t *testing.T, raw json.RawMessage) map[string]json.RawMessage {
	t.Helper()
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		t.Fatalf("decode delivery lag object: %v", err)
	}
	want := []string{"coverage", "eligible_connections", "sampled_at", "sampled_connections", "state", "worst_pts"}
	if got := sortedDeliveryLagKeys(object); !slices.Equal(got, want) {
		t.Fatalf("delivery lag keys = %v, want %v", got, want)
	}
	return object
}

func jsonResponseBytes(t *testing.T, body string) json.RawMessage {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal([]byte(body), &root); err != nil {
		t.Fatalf("decode metrics JSON: %v", err)
	}
	return root["delivery_lag"]
}

func extractDeliveryLagTelemetryJSON(t *testing.T, html string) []byte {
	t.Helper()
	const open = `<script id="delivery-lag-telemetry" type="application/json">`
	start := strings.Index(html, open)
	if start < 0 {
		t.Fatalf("delivery-lag telemetry script missing")
	}
	start += len(open)
	end := strings.Index(html[start:], `</script>`)
	if end < 0 {
		t.Fatalf("delivery-lag telemetry script is not closed")
	}
	return []byte(html[start : start+end])
}

func sortedDeliveryLagKeys(object map[string]json.RawMessage) []string {
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}
