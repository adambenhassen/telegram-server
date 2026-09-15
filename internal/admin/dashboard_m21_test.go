package admin_test

import (
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/admin"
)

func TestDashboardM21RendersFixedOperationalFamilies(t *testing.T) {
	worstPts := int64(0)
	sampledAt := time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC)
	m := admin.MetricsResponse{
		Timestamp:        sampledAt,
		SampleState:      admin.SampleStateAvailable,
		SampleAgeSeconds: 2,
		Connections:      2,
		Sessions:         2,
		TotalUsers:       1,
		ActiveUsers1H:    1,
		ActiveUsers24H:   1,
		DeliveryLag: admin.DeliveryLag{
			WorstPts:            &worstPts,
			State:               admin.DeliveryLagAvailable,
			Coverage:            admin.DeliveryLagCoverageFull,
			EligibleConnections: 2,
			SampledConnections:  2,
			SampledAt:           &sampledAt,
		},
		MaxPtsGap:              0,
		NotifyCount:            0,
		NotifyWindowSeconds:    3600,
		NotifyRatePerSecond:    0,
		PushLatencyP50:         50,
		PushLatencyP95:         50,
		PushLatencySampleCount: 1,
		PushWindowSeconds:      3600,
		PushOutcomes: admin.PushOutcomes{
			Success: 1,
		},
		RateLimitDenialsWindowSeconds: 3600,
		RateLimitDenialsBySurface: admin.RateLimitDenialsBySurface{
			MessageSend: 1,
		},
		StorageRows: admin.StorageRows{Users: 1},
	}

	data := admin.BuildDashboardData(m, "csrf")
	var body strings.Builder
	if err := admin.RenderDashboard(&body, data); err != nil {
		t.Fatal(err)
	}
	markup := body.String()

	for _, want := range []string{
		"This replica · process-local",
		"Counters reset at process restart",
		"Worst live-connection lag",
		"0 PTS",
		"All sampled connections are at the current head.",
		"Account-head spread",
		"0 PTS",
		"Persisted account updates (",
		"tg_updates",
		"≤ 50 ms",
		"3,600 s (1 h)",
		"Successful write",
		"Owner mismatch",
		"Encoding failed",
		"Write failed",
		"No valid notifications in this window.",
		"tg_updates",
		"tg_pinned",
		"Rate-limit denials",
		"message_send",
		"update_profile",
		"Telemetry observations dropped",
		"RPC timing is not available in this dashboard. Production trace export is unavailable.",
		"Across replicas, sum notification delivery work",
		`<dl class="dashboard-meta-grid"`,
	} {
		if !strings.Contains(markup, want) {
			t.Errorf("dashboard missing %q", want)
		}
	}
	if strings.Contains(markup, `<div class="dashboard-meta-grid"`) {
		t.Fatal("delivery sample metadata must use a description list")
	}
	if strings.Contains(markup, "No successful writes in this window.") {
		t.Fatal("a positive push sample count must not render the empty-window helper")
	}
	spread := strings.Index(markup, `data-metric="max_pts_gap"`)
	if spread < 0 || !strings.Contains(markup[spread:], ">0 PTS") {
		t.Fatalf("account-head spread should carry PTS units: %q", markup[max(0, spread-80):min(len(markup), spread+180)])
	}
}

func TestDashboardM21KeepsOutcomesWhenLatencyHasNoSamples(t *testing.T) {
	m := admin.MetricsResponse{
		PushLatencySampleCount: 0,
		PushWindowSeconds:      3600,
		PushOutcomes: admin.PushOutcomes{
			OwnerMismatch: 2,
			EncodeFailure: 1,
			WriteFailure:  3,
		},
	}

	data := admin.BuildDashboardData(m, "csrf")
	if data.Push.P50.Value != "No samples" || data.Push.P95.Value != "No samples" {
		t.Fatalf("empty latency readings = %q, %q, want no samples", data.Push.P50.Value, data.Push.P95.Value)
	}
	want := []string{"0", "2", "1", "3"}
	for i, row := range data.Push.Outcomes {
		if row.Value != want[i] {
			t.Errorf("outcome %q = %q, want %q", row.Label, row.Value, want[i])
		}
	}
}

func TestDashboardM21NoSampledConnectionsCopy(t *testing.T) {
	worstPts := int64(0)
	m := admin.MetricsResponse{
		DeliveryLag: admin.DeliveryLag{
			WorstPts:            &worstPts,
			State:               admin.DeliveryLagAvailable,
			Coverage:            admin.DeliveryLagCoverageFull,
			EligibleConnections: 0,
			SampledConnections:  0,
		},
	}

	data := admin.BuildDashboardData(m, "csrf")
	if data.Delivery.Coverage != "No sampled connections" {
		t.Fatalf("delivery coverage = %q, want no-sampled copy", data.Delivery.Coverage)
	}
	if data.Delivery.Worst.Helper != "No live authenticated connections." {
		t.Fatalf("delivery helper = %q, want no-live copy", data.Delivery.Worst.Helper)
	}
}

func TestDashboardM21RendersStaleDeliveryWithSuccessfulSampleTime(t *testing.T) {
	worstPts := int64(4)
	sampledAt := time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC)
	m := admin.MetricsResponse{
		DeliveryLag: admin.DeliveryLag{
			WorstPts:            &worstPts,
			State:               admin.DeliveryLagStale,
			Coverage:            admin.DeliveryLagCoveragePartial,
			EligibleConnections: 2,
			SampledConnections:  1,
			SampledAt:           &sampledAt,
		},
	}

	data := admin.BuildDashboardData(m, "csrf")
	wantState := "Stale · last successful sample 2026-09-15 02:00:00 UTC"
	if data.Delivery.Worst.State != wantState {
		t.Fatalf("stale delivery state = %q, want %q", data.Delivery.Worst.State, wantState)
	}
	if data.Delivery.Worst.Helper != "Showing the last successful sample. Maximum of database account head minus connection push watermark, floored at zero." {
		t.Fatalf("stale delivery helper = %q, want successful-sample copy", data.Delivery.Worst.Helper)
	}

	var body strings.Builder
	if err := admin.RenderDashboard(&body, data); err != nil {
		t.Fatal(err)
	}
	markup := body.String()
	if strings.Contains(markup, "last complete sample") {
		t.Fatal("stale delivery markup must not use the old complete-sample copy")
	}
}

func TestDashboardM21RendersPushP95Overflow(t *testing.T) {
	m := admin.MetricsResponse{
		PushLatencyP95:         60_000,
		PushLatencyP95Overflow: true,
		PushLatencySampleCount: 1,
		PushWindowSeconds:      3600,
	}

	data := admin.BuildDashboardData(m, "csrf")
	if data.Push.P95.Value != "> 60,000 ms" {
		t.Fatalf("p95 overflow = %q, want > 60,000 ms", data.Push.P95.Value)
	}
	if data.Push.P95.Helper != "" {
		t.Fatalf("p95 overflow helper = %q, want empty", data.Push.P95.Helper)
	}
}

func TestDashboardM21CapabilitiesSuppressEveryAllowlistedValue(t *testing.T) {
	worstPts := int64(9)
	sampledAt := time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC)
	m := admin.MetricsResponse{
		Timestamp:        sampledAt,
		SampleState:      admin.SampleStateAvailable,
		SampleAgeSeconds: 2,
		MaxPtsGap:        42,
		DeliveryLag: admin.DeliveryLag{
			WorstPts:            &worstPts,
			State:               admin.DeliveryLagAvailable,
			Coverage:            admin.DeliveryLagCoverageFull,
			EligibleConnections: 3,
			SampledConnections:  2,
			SampledAt:           &sampledAt,
		},
		NotifyCount:         17,
		NotifyRatePerSecond: 1.25,
		NotifyChannels: admin.NotifyChannels{
			Updates: 3,
		},
		NotifyInvalid:                 4,
		PushLatencyP50:                50,
		PushLatencyP95:                95,
		PushLatencySampleCount:        7,
		PushWindowSeconds:             3600,
		PushOutcomes:                  admin.PushOutcomes{Success: 5, WriteFailure: 2},
		RateLimitDenialsCount:         11,
		RateLimitDenialsWindowSeconds: 10,
		RateLimitDenialsRatePerSecond: 1.1,
		RateLimitDenialsBySurface:     admin.RateLimitDenialsBySurface{MessageSend: 8},
		RateLimitDenialsDropped:       3,
		Uninstrumented: []string{
			"delivery_lag",
			"max_pts_gap",
			"notify_count",
			"notify_rate_per_second",
			"notify_channels",
			"notify_invalid",
			"push_latency_p50_ms",
			"push_latency_p95_ms",
			"push_latency_sample_count",
			"push_outcomes",
			"rate_limit_denials_count",
			"rate_limit_denials_rate_per_second",
			"rate_limit_denials_by_surface",
			"rate_limit_denials_dropped",
		},
	}

	data := admin.BuildDashboardData(m, "csrf")
	if len(data.UninstrumentedFields) != len(m.Uninstrumented) {
		t.Fatalf("uninstrumented fields = %v, want %v", data.UninstrumentedFields, m.Uninstrumented)
	}
	if data.Delivery.Worst.Value != "Not yet instrumented" || data.Delivery.SampledConnections != "Unavailable" || data.Delivery.EligibleConnections != "Unavailable" {
		t.Fatalf("delivery capability state = %+v, want all readings suppressed", data.Delivery)
	}
	if data.Delivery.AccountHeadSpread.Value != "Not yet instrumented" {
		t.Fatalf("account-head spread = %+v, want capability state", data.Delivery.AccountHeadSpread)
	}
	if data.Push.P50.Value != "Not yet instrumented" || data.Push.P95.Value != "Not yet instrumented" || data.Push.SampleCount != "Not yet instrumented" {
		t.Fatalf("push capability state = %+v, want percentile and count readings suppressed", data.Push)
	}
	for _, row := range data.Push.Outcomes {
		if row.Value != "Not yet instrumented" {
			t.Fatalf("push outcome %q = %q, want capability state", row.Label, row.Value)
		}
	}
	if data.Notifications.Count.Value != "Not yet instrumented" || data.Notifications.Rate.Value != "Not yet instrumented" || data.Notifications.Invalid.Value != "Not yet instrumented" {
		t.Fatalf("notification capability state = %+v, want all readings suppressed", data.Notifications)
	}
	for _, row := range data.Notifications.Rows {
		if row.Value != "Not yet instrumented" {
			t.Fatalf("notification channel %q = %q, want capability state", row.Label, row.Value)
		}
	}
	if data.Denials.Count.Value != "Not yet instrumented" || data.Denials.Rate.Value != "Not yet instrumented" || data.Denials.Dropped.Value != "Not yet instrumented" {
		t.Fatalf("denial capability state = %+v, want all readings suppressed", data.Denials)
	}
	for _, row := range data.Denials.Rows {
		if row.Value != "Not yet instrumented" {
			t.Fatalf("denial surface %q = %q, want capability state", row.Label, row.Value)
		}
	}
}

func TestDashboardM21SuppressesPercentilesUntilPushWindowUsable(t *testing.T) {
	m := admin.MetricsResponse{
		PushLatencyP50:                50,
		PushLatencyP95:                95,
		PushLatencySampleCount:        2,
		PushWindowSeconds:             0,
		NotifyWindowSeconds:           0,
		NotifyRatePerSecond:           0,
		RateLimitDenialsWindowSeconds: 0,
		RateLimitDenialsRatePerSecond: 0,
	}

	data := admin.BuildDashboardData(m, "csrf")
	if data.Push.SampleCount != "2" {
		t.Fatalf("sample count = %q, want supplied count retained", data.Push.SampleCount)
	}
	if data.Push.Window != "<0.1 s" || data.Push.WindowStatus != "Window just started" {
		t.Fatalf("push window = %q (%q), want zero-duration state", data.Push.Window, data.Push.WindowStatus)
	}
	if data.Push.P50.Value != "No samples" || data.Push.P95.Value != "No samples" {
		t.Fatalf("percentiles = %q, %q, want suppressed until the window is usable", data.Push.P50.Value, data.Push.P95.Value)
	}
	if data.Notifications.WindowStatus != "Window just started" || data.Denials.WindowStatus != "Window just started" {
		t.Fatalf("empty window states = %q, %q, want zero-duration state", data.Notifications.WindowStatus, data.Denials.WindowStatus)
	}
	if data.Notifications.Rate.Value != "0.00/s" || data.Denials.Rate.Value != "0.00/s" {
		t.Fatalf("zero-duration rates = %q, %q, want defined zero rates", data.Notifications.Rate.Value, data.Denials.Rate.Value)
	}
}

func TestDashboardM21ExplicitCapabilityBeatsInvalidPushSentinel(t *testing.T) {
	m := admin.MetricsResponse{
		PushLatencyP50:         50,
		PushLatencyP95:         95,
		PushLatencySampleCount: -1,
		PushWindowSeconds:      3600,
		Uninstrumented:         []string{"push_latency_p50_ms", "push_latency_p95_ms"},
	}

	data := admin.BuildDashboardData(m, "csrf")
	if data.Push.P50.Value != "Not yet instrumented" || data.Push.P95.Value != "Not yet instrumented" {
		t.Fatalf("explicit capability state was overwritten: p50=%q p95=%q", data.Push.P50.Value, data.Push.P95.Value)
	}
	if data.Push.SampleCount != "Unavailable" {
		t.Fatalf("invalid sample count = %q, want unavailable", data.Push.SampleCount)
	}
}

func TestDashboardM21InitialStaleStateUsesOneBanner(t *testing.T) {
	m := admin.MetricsResponse{
		Timestamp:        time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC),
		SampleState:      admin.SampleStateStale,
		SampleAgeSeconds: 45,
	}
	data := admin.BuildDashboardData(m, "csrf")
	var body strings.Builder
	if err := admin.RenderDashboard(&body, data); err != nil {
		t.Fatal(err)
	}
	markup := body.String()
	if !strings.Contains(markup, `id="banner-stale"`) || !strings.Contains(markup, `data-server-stale="Stale`) {
		t.Fatal("initial stale sample should mark the shared freshness banner")
	}
	if strings.Contains(markup, `id="banner-snapshot"`) {
		t.Fatal("initial stale sample should not render a duplicate freshness banner")
	}
}
