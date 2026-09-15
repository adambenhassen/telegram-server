package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"html"
	"log/slog"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// DashboardFragmentRenderer renders the metrics sections fragment using templ
// components. Pass it as BroadcasterConfig.Render to replace DefaultFragmentRenderer.
// The CSRF token is deliberately absent — fragments are rendered once per tick
// and fanned out to every connected client.
func DashboardFragmentRenderer(m MetricsResponse) ([]Fragment, error) {
	d := BuildDashboardData(m, "")
	var buf bytes.Buffer
	// The wrapper is the patch target itself, reproducing the element the
	// first paint put around the same component: a fragment must be a single
	// element whose id is the selector, or the merge keeps only its last
	// top-level node.
	buf.WriteString(metricsStreamOpenHTML(d))
	if err := metricsFragment(d).Render(context.Background(), &buf); err != nil {
		return nil, fmt.Errorf("render metrics fragment: %w", err)
	}
	telemetry, err := pushTelemetryHTML(m)
	if err != nil {
		return nil, err
	}
	rateLimitDenialTelemetry, err := rateLimitDenialTelemetryHTML(m)
	if err != nil {
		return nil, err
	}
	deliveryLagTelemetry, err := deliveryLagTelemetryHTML(m)
	if err != nil {
		return nil, err
	}
	buf.WriteString(telemetry)
	buf.WriteString(rateLimitDenialTelemetry)
	buf.WriteString(deliveryLagTelemetry)
	buf.WriteString(`</div>`)
	return []Fragment{{Event: sseDefaultEvent, HTML: buf.String()}}, nil
}

func metricsStreamOpenHTML(d DashboardData) string {
	return `<div id="` + sseTargetID +
		`" data-sample-timestamp="` + html.EscapeString(d.SampleTimestamp) +
		`" data-sample-age-seconds="` + html.EscapeString(d.SampleAgeSeconds) +
		`" data-sample-state="` + html.EscapeString(d.SampleState) +
		`" data-process-started-at="` + html.EscapeString(d.ProcessStartedAt) +
		`" data-process-generation="` + html.EscapeString(d.ProcessGeneration) +
		`" data-replica-id="` + html.EscapeString(d.ReplicaID) + `">`
}

// DashboardData is the template data for GET /admin/dashboard. Exported for
// testing.
type DashboardData struct {
	// Legacy fields are retained for the JSON/SSE compatibility renderer and
	// existing callers. The page uses the grouped M21 data below.
	Connections string
	Sessions    string
	Messages1H  string
	MaxPtsGap   string

	// Accounts
	TotalUsers      string
	ActiveUsers1H   string
	ActiveUsers24H  string
	ActiveUsersMeta string // "12,480 of 41,203 (30%)" or "" when total is 0
	ActiveUsersPct  float64

	// Content
	TotalChannels string
	TotalChats    string
	Messages24H   string

	// Throttling
	RateLimitActive string

	// Uninstrumented card — names from the metrics payload, plus their labels
	UninstrumentedNames []UninstrLabel

	// UninstrumentedFields is the raw field-name slice for the JS initializer.
	UninstrumentedFields []string

	// UninstrumentedJSON is the JSON-encoded JS array literal for the script block.
	UninstrumentedJSON string

	// Storage
	StorageRows []DashStorageRow

	// M21 dashboard families.
	ReplicaScope     string
	ReplicaResetCopy string
	Delivery         DashDeliveryData
	Push             DashPushData
	Notifications    DashNotificationsData
	Denials          DashDenialsData
	SharedDatabase   DashSharedDatabaseData
	RPCNote          string
	FleetAggregation string
	SnapshotStatus   string

	// Banner state
	ShowEmptyAlert bool

	// Logout form
	CSRFToken string

	// Snapshot metadata is copied to both the initial HTML and every SSE
	// fragment. The browser uses the sample timestamp and age as its freshness
	// clock; these strings are intentionally server-formatted.
	SampleTimestamp   string
	SampleAgeSeconds  string
	SampleState       string
	ProcessStartedAt  string
	ProcessGeneration string
	ReplicaID         string

	// Server timestamp string (for chip tooltip)
	ServerTimestamp string
}

type UninstrLabel struct {
	Field string
	Label string
}

type DashStorageRow struct {
	Table   string
	Display string // "41,203", "~1,204,880", or "—"
	Source  string // "Exact", "Estimated", or "Unknown"
}

// DashMetric is a fixed, aggregate-only reading rendered by the dashboard.
// Labels and IDs are compiled into the server; no payload key becomes visible
// text or an attribute.
type DashMetric struct {
	ID     string
	Metric string
	Label  string
	Value  string
	State  string
	Helper string
}

type DashCountRow struct {
	ID    string
	Label string
	Value string
}

type DashDeliveryData struct {
	Worst               DashMetric
	AccountHeadSpread   DashMetric
	Sample              string
	Coverage            string
	SampledConnections  string
	EligibleConnections string
}

type DashPushData struct {
	P50          DashMetric
	P95          DashMetric
	SampleCount  string
	Window       string
	WindowStatus string
	Outcomes     []DashCountRow
}

type DashNotificationsData struct {
	Count        DashMetric
	Rate         DashMetric
	Window       string
	WindowStatus string
	WindowHelper string
	Rows         []DashCountRow
	Invalid      DashCountRow
}

type DashDenialsData struct {
	Count        DashMetric
	Rate         DashMetric
	Window       string
	WindowStatus string
	WindowHelper string
	Rows         []DashCountRow
	Dropped      DashCountRow
}

type DashSharedDatabaseData struct {
	TotalUsers      string
	ActiveUsers1H   string
	ActiveUsers24H  string
	ActiveUsersMeta string
	ActiveUsersPct  float64
	TotalChannels   string
	TotalChats      string
	Messages1H      string
	Messages24H     string
	RateLimitActive string
}

// uninstrumentedLabels maps raw field names to their display labels.
var uninstrumentedLabels = map[string]string{
	"delivery_lag":                       "Delivery lag",
	"max_pts_gap":                        "Account-head spread",
	"notify_count":                       "NOTIFY events per hour",
	"notify_rate_per_second":             "Notification rate",
	"notify_channels":                    "Notification channels",
	"notify_invalid":                     "Invalid notifications",
	"push_latency_p50_ms":                "Push latency p50",
	"push_latency_p95_ms":                "Push latency p95",
	"push_latency_sample_count":          "Successful push samples",
	"push_outcomes":                      "Push outcomes",
	"rate_limit_denials_count":           "Rate-limit denials",
	"rate_limit_denials_rate_per_second": "Denial rate",
	"rate_limit_denials_by_surface":      "Denials by surface",
	"rate_limit_denials_dropped":         "Dropped telemetry observations",
}

// storageTableOrder defines the display order and labels for storage_rows.
var storageTableOrder = []struct {
	Field string
	Label string
	Exact bool
}{
	{"users", "users", true},
	{"messages", "messages", false},
	{"events", "events", false},
	{"channels", "channels", true},
	{"channel_messages", "channel_messages", false},
	{"chats", "chats", true},
	{"files", "files", false},
	{"auth_keys", "auth_keys", false},
}

// FmtInt formats an integer with thousands separators. Negative values render
// as "—" (unknown/unanalyzed). Exported for testing.
func FmtInt(n int64) string {
	if n < 0 {
		return "—"
	}
	s := strconv.FormatInt(n, 10)
	// Insert commas every 3 digits from the right.
	var b strings.Builder
	start := len(s) % 3
	if start == 0 {
		start = 3
	}
	b.WriteString(s[:start])
	for i := start; i < len(s); i += 3 {
		b.WriteByte(',')
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

// storageDisplay returns (display, source) for a storage_rows value.
// Exact counts come from count(*); estimated from pg_class.reltuples (may be -1).
func storageDisplay(n int64, exact bool) (display, source string) {
	if n < 0 {
		return "—", "Unknown"
	}
	if exact {
		return FmtInt(n), "Exact"
	}
	return "~" + FmtInt(n), "Estimated"
}

// BuildDashboardData assembles the template data from a MetricsResponse and a
// fresh CSRF token. All display collections below are fixed server-side lists.
func BuildDashboardData(m MetricsResponse, csrfToken string) DashboardData {
	d := DashboardData{
		Connections:       safeCount(int64(m.Connections)),
		Sessions:          safeCount(int64(m.Sessions)),
		Messages1H:        safeCount(m.Messages1H),
		MaxPtsGap:         safeCount(m.MaxPtsGap),
		TotalUsers:        safeCount(m.TotalUsers),
		ActiveUsers1H:     safeCount(m.ActiveUsers1H),
		ActiveUsers24H:    safeCount(m.ActiveUsers24H),
		TotalChannels:     safeCount(m.TotalChannels),
		TotalChats:        safeCount(m.TotalChats),
		Messages24H:       safeCount(m.Messages24H),
		RateLimitActive:   safeCount(m.RateLimitActive),
		CSRFToken:         csrfToken,
		SampleTimestamp:   formatTimestamp(m.Timestamp, time.RFC3339Nano),
		SampleAgeSeconds:  strconv.FormatFloat(maxFloat(m.SampleAgeSeconds), 'f', -1, 64),
		SampleState:       string(m.SampleState),
		ProcessStartedAt:  formatTimestamp(m.ProcessStartedAt, time.RFC3339Nano),
		ProcessGeneration: m.ProcessGeneration,
		ServerTimestamp:   formatTimestamp(m.Timestamp, "2006-01-02 15:04:05 UTC"),
		ShowEmptyAlert:    m.TotalUsers == 0 && m.Connections == 0 && m.Messages1H == 0 && m.Messages24H == 0,
		ReplicaScope:      "This replica · process-local",
		ReplicaResetCopy:  "Counters reset at process restart.",
		RPCNote:           "RPC timing is not available in this dashboard. Production trace export is unavailable.",
		FleetAggregation:  fleetAggregationCopy,
		SnapshotStatus:    snapshotStatus(m),
	}
	if m.ReplicaID != nil {
		d.ReplicaID = *m.ReplicaID
	}

	// Active users meter: only when total and activity values are valid.
	if m.TotalUsers > 0 && m.ActiveUsers24H >= 0 {
		p := float64(m.ActiveUsers24H) / float64(m.TotalUsers) * 100
		d.ActiveUsersPct = p
		d.ActiveUsersMeta = fmt.Sprintf("%s of %s (%.0f%%)",
			FmtInt(m.ActiveUsers24H), FmtInt(m.TotalUsers), p)
	}

	// Uninstrumented card. Unknown names are intentionally discarded before
	// either visible markup or the browser initializer is built.
	for _, name := range m.Uninstrumented {
		label, ok := uninstrumentedLabels[name]
		if !ok {
			continue
		}
		d.UninstrumentedFields = append(d.UninstrumentedFields, name)
		d.UninstrumentedNames = append(d.UninstrumentedNames, UninstrLabel{
			Field: name,
			Label: label,
		})
	}
	jsonBytes, err := json.Marshal(d.UninstrumentedFields)
	if err != nil || jsonBytes == nil {
		jsonBytes = []byte("[]")
	}
	d.UninstrumentedJSON = string(jsonBytes)

	d.Delivery = dashboardDelivery(m)
	d.Push = dashboardPush(m)
	d.Notifications = dashboardNotifications(m)
	d.Denials = dashboardDenials(m)
	d.SharedDatabase = dashboardSharedDatabase(d)

	// Storage table.
	storageMap := map[string]int64{
		"users":            m.StorageRows.Users,
		"messages":         m.StorageRows.Messages,
		"events":           m.StorageRows.Events,
		"channels":         m.StorageRows.Channels,
		"channel_messages": m.StorageRows.ChannelMessages,
		"chats":            m.StorageRows.Chats,
		"files":            m.StorageRows.Files,
		"auth_keys":        m.StorageRows.AuthKeys,
	}
	for _, row := range storageTableOrder {
		val := storageMap[row.Field]
		display, source := storageDisplay(val, row.Exact)
		d.StorageRows = append(d.StorageRows, DashStorageRow{
			Table:   row.Label,
			Display: display,
			Source:  source,
		})
	}

	return d
}

func formatTimestamp(value time.Time, layout string) string {
	if value.IsZero() {
		return "Unavailable"
	}
	return value.UTC().Format(layout)
}

const fleetAggregationCopy = "Across replicas, sum notification delivery work and actual denial/outcome counts over compatible windows. Notification totals are not unique committed events. Merge matching latency histogram buckets before computing percentiles; never average replica p50 or p95. Worst connection lag is the maximum across replicas, never a sum. Shared database counts are read once, not added across replicas."

func safeCount(value int64) string {
	if value < 0 {
		return "Unavailable"
	}
	return FmtInt(value)
}

func snapshotStatus(m MetricsResponse) string {
	if m.SampleState == SampleStateStale || maxFloat(m.SampleAgeSeconds) >= 40 {
		return "Stale · last sample " + formatAge(m.SampleAgeSeconds)
	}
	return ""
}

func formatAge(seconds float64) string {
	seconds = maxFloat(seconds)
	if seconds < 60 {
		return fmt.Sprintf("%.0fs", seconds)
	}
	minutes := math.Round(seconds / 60)
	if minutes < 60 {
		return fmt.Sprintf("%.0fm", minutes)
	}
	return fmt.Sprintf("%.0fh", math.Round(minutes/60))
}

func formatWindow(seconds float64) string {
	if seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return "Unavailable"
	}
	if seconds >= 3600 {
		return "3,600 s (1 h)"
	}
	if seconds < 0.1 {
		return "<0.1 s"
	}
	return fmt.Sprintf("%.1f s", seconds)
}

func windowStatus(seconds float64) string {
	if seconds < 0 || math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return "Unavailable"
	}
	if seconds == 0 {
		return "Window just started"
	}
	if seconds < 3600 {
		return "Collecting window · " + formatWindow(seconds) + " of 3,600 s"
	}
	return ""
}

func formatRate(rate float64) string {
	if rate < 0 || math.IsNaN(rate) || math.IsInf(rate, 0) {
		return "Unavailable"
	}
	if rate > 0 && rate < 0.01 {
		return "<0.01/s"
	}
	return fmt.Sprintf("%.2f/s", rate)
}

func formatLatencyBound(value float64) string {
	if value <= 0 || math.IsNaN(value) || math.IsInf(value, 0) {
		return "Unavailable"
	}
	if value < 0.01 {
		return "<0.01"
	}
	return strings.TrimRight(strings.TrimRight(strconv.FormatFloat(value, 'f', 2, 64), "0"), ".")
}

func formatLatency(value float64, overflow bool) string {
	if overflow {
		return "> 60,000 ms"
	}
	bound := formatLatencyBound(value)
	if bound == "Unavailable" {
		return bound
	}
	return "≤ " + bound + " ms"
}

func windowHelper(seconds float64) string {
	if seconds > 0 && seconds < 3600 && !math.IsNaN(seconds) && !math.IsInf(seconds, 0) {
		return "Average uses the elapsed window."
	}
	return ""
}

func formatPTS(value int64) string {
	display := safeCount(value)
	if display == "Unavailable" {
		return display
	}
	return display + " PTS"
}

func isUninstrumented(fields []string, field string) bool {
	return slices.Contains(fields, field)
}

func dashboardMetric(id, metric, label, value, helper, state string) DashMetric {
	return DashMetric{ID: id, Metric: metric, Label: label, Value: value, Helper: helper, State: state}
}

func markUninstrumentedMetric(metric *DashMetric) {
	metric.Value = "Not yet instrumented"
	metric.State = "Not yet instrumented"
	metric.Helper = "This metric is not measured yet."
}

func markUninstrumentedRows(rows []DashCountRow) {
	for i := range rows {
		rows[i].Value = "Not yet instrumented"
	}
}

func dashboardDelivery(m MetricsResponse) DashDeliveryData {
	state := snapshotStatus(m)
	d := DashDeliveryData{
		Worst: dashboardMetric(
			"v-delivery-lag", "delivery_lag", "Worst live-connection lag", "Unavailable",
			"Maximum of database account head minus connection push watermark, floored at zero.", "Unavailable",
		),
		AccountHeadSpread: dashboardMetric(
			"v-max_pts_gap", "max_pts_gap", "Account-head spread", formatPTS(m.MaxPtsGap),
			"Difference between account heads for accounts connected here. This does not measure connection delivery lag.", state,
		),
		Sample:              "Sample unavailable",
		Coverage:            "Coverage unavailable",
		SampledConnections:  "Unavailable",
		EligibleConnections: "Unavailable",
	}
	if isUninstrumented(m.Uninstrumented, "max_pts_gap") {
		markUninstrumentedMetric(&d.AccountHeadSpread)
	}
	if isUninstrumented(m.Uninstrumented, "delivery_lag") {
		markUninstrumentedMetric(&d.Worst)
		d.Sample = "Sample unavailable"
		d.Coverage = "Coverage unavailable"
		return d
	}
	if m.DeliveryLag.State != "" || m.DeliveryLag.Coverage != "" {
		d.SampledConnections = safeCount(int64(m.DeliveryLag.SampledConnections))
		d.EligibleConnections = safeCount(int64(m.DeliveryLag.EligibleConnections))
	}
	if m.DeliveryLag.SampledAt != nil {
		d.Sample = "Sample " + m.DeliveryLag.SampledAt.UTC().Format("2006-01-02 15:04:05 UTC")
	}
	switch m.DeliveryLag.Coverage {
	case DeliveryLagCoverageFull:
		d.Coverage = "Full coverage"
	case DeliveryLagCoveragePartial:
		d.Coverage = "Partial coverage"
	case DeliveryLagCoverageNone:
		d.Coverage = "No sampled connections"
	}
	if m.DeliveryLag.WorstPts != nil && *m.DeliveryLag.WorstPts >= 0 &&
		m.DeliveryLag.State == DeliveryLagAvailable && m.DeliveryLag.Coverage == DeliveryLagCoverageFull {
		d.Worst.Value = safeCount(*m.DeliveryLag.WorstPts) + " PTS"
		d.Worst.State = state
		switch {
		case m.DeliveryLag.SampledConnections == 0:
			d.Worst.Helper = "No live authenticated connections."
		case *m.DeliveryLag.WorstPts == 0:
			d.Worst.Helper = "All sampled connections are at the current head."
		}
		return d
	}
	if m.DeliveryLag.WorstPts != nil && *m.DeliveryLag.WorstPts >= 0 && m.DeliveryLag.State == DeliveryLagStale {
		d.Worst.Value = safeCount(*m.DeliveryLag.WorstPts) + " PTS"
		d.Worst.State = "Stale · last complete sample"
		d.Worst.Helper = "Showing the last complete sample. " + d.Worst.Helper
		return d
	}
	if m.DeliveryLag.Coverage == DeliveryLagCoveragePartial {
		d.Worst.State = "Partial coverage"
	} else if m.DeliveryLag.State == DeliveryLagUnavailable {
		d.Worst.State = "Unavailable"
	}
	return d
}

func dashboardPush(m MetricsResponse) DashPushData {
	baseState := snapshotStatus(m)
	sampleCountUnavailable := isUninstrumented(m.Uninstrumented, "push_latency_sample_count")
	p50Unavailable := isUninstrumented(m.Uninstrumented, "push_latency_p50_ms")
	p95Unavailable := isUninstrumented(m.Uninstrumented, "push_latency_p95_ms")
	data := DashPushData{
		P50:          dashboardMetric("v-push-p50", "push_latency_p50_ms", "p50", "No samples", "No successful writes in this window.", baseState),
		P95:          dashboardMetric("v-push-p95", "push_latency_p95_ms", "p95", "No samples", "No successful writes in this window.", baseState),
		SampleCount:  safeCount(m.PushLatencySampleCount),
		Window:       formatWindow(m.PushWindowSeconds),
		WindowStatus: windowStatus(m.PushWindowSeconds),
		Outcomes: []DashCountRow{
			{ID: "push-outcome-success", Label: "Successful write", Value: safeCount(m.PushOutcomes.Success)},
			{ID: "push-outcome-owner-mismatch", Label: "Owner mismatch", Value: safeCount(m.PushOutcomes.OwnerMismatch)},
			{ID: "push-outcome-encode-failure", Label: "Encoding failed", Value: safeCount(m.PushOutcomes.EncodeFailure)},
			{ID: "push-outcome-write-failure", Label: "Write failed", Value: safeCount(m.PushOutcomes.WriteFailure)},
		},
	}
	if sampleCountUnavailable {
		data.SampleCount = "Not yet instrumented"
	}
	switch {
	case p50Unavailable:
		markUninstrumentedMetric(&data.P50)
	case m.PushLatencySampleCount > 0 && m.PushWindowSeconds > 0:
		data.P50.Value = formatLatency(m.PushLatencyP50, m.PushLatencyP50Overflow)
	case m.PushLatencySampleCount > 0:
		data.P50.Helper = "Percentile unavailable until the observation window has elapsed."
	}
	switch {
	case p95Unavailable:
		markUninstrumentedMetric(&data.P95)
	case m.PushLatencySampleCount > 0 && m.PushWindowSeconds > 0:
		data.P95.Value = formatLatency(m.PushLatencyP95, m.PushLatencyP95Overflow)
	case m.PushLatencySampleCount > 0:
		data.P95.Helper = "Percentile unavailable until the observation window has elapsed."
	}
	if isUninstrumented(m.Uninstrumented, "push_outcomes") {
		markUninstrumentedRows(data.Outcomes)
	}
	if m.PushLatencySampleCount < 0 && !sampleCountUnavailable {
		data.SampleCount = "Unavailable"
		if !p50Unavailable {
			data.P50.Value = "Unavailable"
			data.P50.Helper = "No complete latency sample is available."
		}
		if !p95Unavailable {
			data.P95.Value = "Unavailable"
			data.P95.Helper = "No complete latency sample is available."
		}
	}
	return data
}

func dashboardNotifications(m MetricsResponse) DashNotificationsData {
	countUnavailable := isUninstrumented(m.Uninstrumented, "notify_count")
	channelsUnavailable := isUninstrumented(m.Uninstrumented, "notify_channels")
	data := DashNotificationsData{
		Count:  dashboardMetric("v-notify-count", "notify_count", "Valid notifications", safeCount(m.NotifyCount), "Known-channel notifications accepted after successful parsing. Counts replica delivery work; one notification may fan out to multiple connection writes.", ""),
		Rate:   dashboardMetric("v-notify-rate", "notify_rate_per_second", "Average per second", formatRate(m.NotifyRatePerSecond), "Average over the elapsed rolling window.", ""),
		Window: formatWindow(m.NotifyWindowSeconds), WindowStatus: windowStatus(m.NotifyWindowSeconds), WindowHelper: windowHelper(m.NotifyWindowSeconds),
		Invalid: DashCountRow{ID: "notify-invalid", Label: "Invalid or unknown notifications", Value: safeCount(m.NotifyInvalid)},
	}
	if countUnavailable {
		markUninstrumentedMetric(&data.Count)
	}
	if isUninstrumented(m.Uninstrumented, "notify_rate_per_second") {
		markUninstrumentedMetric(&data.Rate)
	}
	if isUninstrumented(m.Uninstrumented, "notify_invalid") {
		data.Invalid.Value = "Not yet instrumented"
	}
	if m.NotifyCount == 0 && !countUnavailable {
		data.Count.Helper = "No valid notifications in this window."
	}
	data.Rows = []DashCountRow{
		{ID: "notify-tg_updates", Label: "tg_updates", Value: safeCount(m.NotifyChannels.Updates)},
		{ID: "notify-tg_typing", Label: "tg_typing", Value: safeCount(m.NotifyChannels.Typing)},
		{ID: "notify-tg_evict", Label: "tg_evict", Value: safeCount(m.NotifyChannels.Evict)},
		{ID: "notify-tg_channel_post", Label: "tg_channel_post", Value: safeCount(m.NotifyChannels.ChannelPost)},
		{ID: "notify-tg_encryption", Label: "tg_encryption", Value: safeCount(m.NotifyChannels.Encryption)},
		{ID: "notify-tg_status", Label: "tg_status", Value: safeCount(m.NotifyChannels.Status)},
		{ID: "notify-tg_encrypted_msg", Label: "tg_encrypted_msg", Value: safeCount(m.NotifyChannels.EncryptedMsg)},
		{ID: "notify-tg_reactions", Label: "tg_reactions", Value: safeCount(m.NotifyChannels.Reactions)},
		{ID: "notify-tg_pinned", Label: "tg_pinned", Value: safeCount(m.NotifyChannels.Pinned)},
	}
	if channelsUnavailable {
		markUninstrumentedRows(data.Rows)
	}
	return data
}

func dashboardDenials(m MetricsResponse) DashDenialsData {
	countUnavailable := isUninstrumented(m.Uninstrumented, "rate_limit_denials_count")
	data := DashDenialsData{
		Count:  dashboardMetric("v-denials-count", "rate_limit_denials_count", "Rate-limit denials", safeCount(m.RateLimitDenialsCount), "Requests returned as FLOOD_WAIT. Allowed requests, refunded reservations and storage failures do not count.", ""),
		Rate:   dashboardMetric("v-denials-rate", "rate_limit_denials_rate_per_second", "Average per second", formatRate(m.RateLimitDenialsRatePerSecond), "Average over the elapsed rolling window.", ""),
		Window: formatWindow(m.RateLimitDenialsWindowSeconds), WindowStatus: windowStatus(m.RateLimitDenialsWindowSeconds), WindowHelper: windowHelper(m.RateLimitDenialsWindowSeconds),
		Dropped: DashCountRow{ID: "denials-dropped", Label: "Telemetry observations dropped", Value: safeCount(m.RateLimitDenialsDropped)},
	}
	if countUnavailable {
		markUninstrumentedMetric(&data.Count)
	}
	if isUninstrumented(m.Uninstrumented, "rate_limit_denials_rate_per_second") {
		markUninstrumentedMetric(&data.Rate)
	}
	if isUninstrumented(m.Uninstrumented, "rate_limit_denials_dropped") {
		data.Dropped.Value = "Not yet instrumented"
	}
	if m.RateLimitDenialsCount == 0 && !countUnavailable {
		data.Count.Helper = "No rate-limit denials in this window."
	}
	data.Rows = []DashCountRow{
		{ID: "denial-message_send", Label: "message_send", Value: safeCount(m.RateLimitDenialsBySurface.MessageSend)},
		{ID: "denial-create_chat", Label: "create_chat", Value: safeCount(m.RateLimitDenialsBySurface.CreateChat)},
		{ID: "denial-add_chat_user", Label: "add_chat_user", Value: safeCount(m.RateLimitDenialsBySurface.AddChatUser)},
		{ID: "denial-create_channel", Label: "create_channel", Value: safeCount(m.RateLimitDenialsBySurface.CreateChannel)},
		{ID: "denial-messages_search", Label: "messages_search", Value: safeCount(m.RateLimitDenialsBySurface.MessagesSearch)},
		{ID: "denial-contacts_search", Label: "contacts_search", Value: safeCount(m.RateLimitDenialsBySurface.ContactsSearch)},
		{ID: "denial-messages_search_global", Label: "messages_search_global", Value: safeCount(m.RateLimitDenialsBySurface.MessagesSearchGlobal)},
		{ID: "denial-save_file_part", Label: "save_file_part", Value: safeCount(m.RateLimitDenialsBySurface.SaveFilePart)},
		{ID: "denial-upload_get_file", Label: "upload_get_file", Value: safeCount(m.RateLimitDenialsBySurface.UploadGetFile)},
		{ID: "denial-send_code_ip_calls", Label: "send_code_ip_calls", Value: safeCount(m.RateLimitDenialsBySurface.SendCodeIPCalls)},
		{ID: "denial-send_code_ip_distinct_numbers", Label: "send_code_ip_distinct_numbers", Value: safeCount(m.RateLimitDenialsBySurface.SendCodeIPDistinctNumbers)},
		{ID: "denial-sign_in_fail_ip", Label: "sign_in_fail_ip", Value: safeCount(m.RateLimitDenialsBySurface.SignInFailIP)},
		{ID: "denial-check_password", Label: "check_password", Value: safeCount(m.RateLimitDenialsBySurface.CheckPassword)},
		{ID: "denial-check_password_ip", Label: "check_password_ip", Value: safeCount(m.RateLimitDenialsBySurface.CheckPasswordIP)},
		{ID: "denial-get_password_ip", Label: "get_password_ip", Value: safeCount(m.RateLimitDenialsBySurface.GetPasswordIP)},
		{ID: "denial-sign_up_ip", Label: "sign_up_ip", Value: safeCount(m.RateLimitDenialsBySurface.SignUpIP)},
		{ID: "denial-password_proof", Label: "password_proof", Value: safeCount(m.RateLimitDenialsBySurface.PasswordProof)},
		{ID: "denial-get_password", Label: "get_password", Value: safeCount(m.RateLimitDenialsBySurface.GetPassword)},
		{ID: "denial-update_profile", Label: "update_profile", Value: safeCount(m.RateLimitDenialsBySurface.UpdateProfile)},
	}
	if isUninstrumented(m.Uninstrumented, "rate_limit_denials_by_surface") {
		markUninstrumentedRows(data.Rows)
	}
	return data
}

func dashboardSharedDatabase(d DashboardData) DashSharedDatabaseData {
	return DashSharedDatabaseData{
		TotalUsers: d.TotalUsers, ActiveUsers1H: d.ActiveUsers1H, ActiveUsers24H: d.ActiveUsers24H,
		ActiveUsersMeta: d.ActiveUsersMeta, ActiveUsersPct: d.ActiveUsersPct,
		TotalChannels: d.TotalChannels, TotalChats: d.TotalChats,
		Messages1H: d.Messages1H, Messages24H: d.Messages24H,
		RateLimitActive: d.RateLimitActive,
	}
}

func maxFloat(value float64) float64 {
	if value < 0 {
		return 0
	}
	return value
}

// DashboardHandler returns an http.HandlerFunc for GET /admin/dashboard.
// It server-renders the full operations dashboard using shadcn-templ components.
// tokenHash is the hex-encoded SHA-256 digest of TG_ADMIN_TOKEN_HASH, used to
// derive the session-bound CSRF token for the logout form.
func DashboardHandler(registry *mtproto.SessionRegistry, st *store.Store, tokenHash string, notifyMetrics ...*store.NotificationMetrics) http.HandlerFunc {
	return DashboardHandlerWithDeliveryLag(registry, st, tokenHash, NewDeliveryLagSampler(), notifyMetrics...)
}

// DashboardHandlerWithDeliveryLag returns a dashboard handler using a supplied
// lag sampler. Sharing it with the JSON and SSE handlers keeps the retained
// complete sample and its source timestamp consistent across surfaces.
func DashboardHandlerWithDeliveryLag(registry *mtproto.SessionRegistry, st *store.Store, tokenHash string, deliveryLag *DeliveryLagSampler, notifyMetrics ...*store.NotificationMetrics) http.HandlerFunc {
	cache := NewMetricsSnapshotCache(registry, st, defaultProcessIdentity, deliveryLag, notifyMetrics...)
	return DashboardHandlerWithSnapshotCache(cache, tokenHash)
}

// DashboardHandlerWithSnapshotCache returns a dashboard handler backed by the
// supplied shared sampler.
func DashboardHandlerWithSnapshotCache(cache *MetricsSnapshotCache, tokenHash string) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		m, err := cache.Snapshot(r.Context())
		if err != nil {
			writeDashboardUnavailable(w, r, tokenHash)
			return
		}

		// Derive the logout CSRF token from the session cookie. It is
		// deterministic, so no Set-Cookie is needed: every tab with the same
		// session renders the same token and it survives past any cookie TTL.
		sessionCookie, err := r.Cookie(sessionCookieName)
		if err != nil {
			// RequireAdmin already validated the session; this cannot fail.
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		csrfToken, err := SessionCSRFToken(tokenHash, sessionCookie.Value)
		if err != nil {
			// Invalid TokenHash is a startup misconfiguration; reject.
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		data := BuildDashboardData(m, csrfToken)

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := dashboardPage(data).Render(r.Context(), w); err != nil {
			slog.Error("render dashboard", "err", err)
			return
		}
	}
}

func writeDashboardUnavailable(w http.ResponseWriter, r *http.Request, tokenHash string) {
	var csrfToken string
	if sessionCookie, err := r.Cookie(sessionCookieName); err == nil {
		if token, err := SessionCSRFToken(tokenHash, sessionCookie.Value); err == nil {
			csrfToken = token
		}
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusServiceUnavailable)
	if err := dashboardUnavailablePage(DashboardData{CSRFToken: csrfToken}).Render(r.Context(), w); err != nil {
		slog.Error("render unavailable dashboard", "err", err)
	}
}
