package admin

import (
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// DashboardData is the template data for GET /admin/dashboard. Exported for
// testing.
type DashboardData struct {
	// Live band
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

	// Storage
	StorageRows []DashStorageRow

	// Banner state
	ShowEmptyAlert bool

	// Logout form
	CSRFToken string

	// Server timestamp string (for chip tooltip)
	ServerTimestamp string

	// JSON array of uninstrumented field names for the JS auto-refresh
	UninstrumentedJSON string
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

// uninstrumentedLabels maps raw field names to their display labels.
var uninstrumentedLabels = map[string]string{
	"notify_count":        "NOTIFY events per hour",
	"push_latency_p50_ms": "Push latency p50",
	"push_latency_p95_ms": "Push latency p95",
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

// storageDisplay returns (display, prefix, source) for a storage_rows value.
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
// fresh CSRF token.
func BuildDashboardData(m MetricsResponse, csrfToken string) DashboardData {
	d := DashboardData{
		Connections:     FmtInt(int64(m.Connections)),
		Sessions:        FmtInt(int64(m.Sessions)),
		Messages1H:      FmtInt(m.Messages1H),
		MaxPtsGap:       FmtInt(m.MaxPtsGap),
		TotalUsers:      FmtInt(m.TotalUsers),
		ActiveUsers1H:   FmtInt(m.ActiveUsers1H),
		ActiveUsers24H:  FmtInt(m.ActiveUsers24H),
		TotalChannels:   FmtInt(m.TotalChannels),
		TotalChats:      FmtInt(m.TotalChats),
		Messages24H:     FmtInt(m.Messages24H),
		RateLimitActive: FmtInt(m.RateLimitActive),
		CSRFToken:       csrfToken,
		ServerTimestamp: m.Timestamp.Format("2006-01-02 15:04:05 UTC"),
		ShowEmptyAlert:  m.TotalUsers == 0 && m.Connections == 0 && m.Messages24H == 0,
	}

	// Active users meter: only when total > 0.
	if m.TotalUsers > 0 {
		pct := float64(m.ActiveUsers24H) / float64(m.TotalUsers) * 100
		d.ActiveUsersPct = pct
		d.ActiveUsersMeta = fmt.Sprintf("%s of %s (%.0f%%)",
			FmtInt(m.ActiveUsers24H), FmtInt(m.TotalUsers), pct)
	}

	// Uninstrumented card.
	for _, name := range m.Uninstrumented {
		label, ok := uninstrumentedLabels[name]
		if !ok {
			label = name
		}
		d.UninstrumentedNames = append(d.UninstrumentedNames, UninstrLabel{
			Field: name,
			Label: label,
		})
	}

	// Build JSON array of uninstrumented field names for the JS refresh handler.
	quoted := make([]string, len(m.Uninstrumented))
	for i, n := range m.Uninstrumented {
		quoted[i] = `"` + n + `"`
	}
	d.UninstrumentedJSON = "[" + strings.Join(quoted, ",") + "]"

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

// DashboardHandler returns an http.HandlerFunc for GET /admin/dashboard.
// It server-renders the full operations dashboard with real metric values.
func DashboardHandler(registry *mtproto.SessionRegistry, st *store.Store) http.HandlerFunc {
	var cache metricsCache

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		cache.refresh(r.Context(), registry, st)
		m := cache.get()

		csrfToken, err := generateCSRFToken()
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}
		http.SetCookie(w, csrfCookie(csrfCookieHash(csrfToken)))

		data := BuildDashboardData(m, csrfToken)

		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		if err := dashboardTmpl.Execute(w, data); err != nil {
			// Headers already sent; nothing to do.
			return
		}
	}
}

var dashboardTmpl = template.Must(template.New("dashboard").Funcs(template.FuncMap{
	"safeJS": func(s string) template.JS { return template.JS(s) }, //nolint:gosec // controlled input
}).Parse(dashboardHTML))

const dashboardHTML = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Operations — telegram-server</title>
<style>
/* Design tokens */
:root {
  --bg: #ffffff;
  --card-bg: #f9f9f9;
  --border: #e5e5e5;
  --fg: #0a0a0a;
  --fg-muted: #737373;
  --ring: var(--fg);
  --status-good: #0ca30c;
  --status-warning: #fab219;
  --status-critical: #d03b3b;
  --status-good-text: #0b0b0b;
  --status-warning-text: #0b0b0b;
  --status-critical-text: #ffffff;
  --radius: 8px;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0a0a0a;
    --card-bg: #171717;
    --border: #262626;
    --fg: #fafafa;
    --fg-muted: #a1a1a1;
  }
}
.dark {
  --bg: #0a0a0a;
  --card-bg: #171717;
  --border: #262626;
  --fg: #fafafa;
  --fg-muted: #a1a1a1;
}

*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: system-ui, -apple-system, sans-serif; background: var(--bg); color: var(--fg); font-size: 14px; line-height: 1.5; }

/* Focus */
:focus-visible { outline: 2px solid var(--ring); outline-offset: 2px; }

/* Skip link */
.skip { position: absolute; top: -40px; left: 8px; background: var(--fg); color: var(--bg); padding: 4px 8px; border-radius: var(--radius); font-size: 12px; transition: top .1s; z-index: 100; }
.skip:focus { top: 8px; }

/* Layout */
.container { max-width: 1440px; margin: 0 auto; padding: 32px 24px; }
@media (max-width: 767px) { .container { padding: 24px 16px; } }

/* Header */
.header { display: flex; align-items: flex-start; justify-content: space-between; gap: 16px; margin-bottom: 24px; flex-wrap: wrap; }
.header-title { display: flex; flex-direction: column; }
.header-title h1 { font-size: 20px; font-weight: 600; line-height: 1.2; }
.header-title .subtitle { font-size: 12px; color: var(--fg-muted); }
.header-controls { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
@media (max-width: 767px) {
  .header { flex-direction: column; gap: 8px; }
  .header-controls { gap: 8px; }
}

/* Freshness chip */
.chip { display: inline-flex; align-items: center; gap: 6px; padding: 3px 10px; border-radius: 9999px; font-size: 12px; font-weight: 500; border: 1px solid transparent; cursor: default; }
.chip-good { background: var(--status-good); color: var(--status-good-text); }
.chip-warning { background: var(--status-warning); color: var(--status-warning-text); }
.chip-critical { background: var(--status-critical); color: var(--status-critical-text); }
.chip-paused { background: var(--card-bg); color: var(--fg-muted); border-color: var(--border); }
.chip-dot { width: 7px; height: 7px; border-radius: 50%; background: currentColor; flex-shrink: 0; }
@keyframes pulse { 0%,100%{opacity:1}50%{opacity:.4} }
.chip-good .chip-dot { animation: pulse 2s ease-in-out infinite; }
@media (prefers-reduced-motion: reduce) { .chip-dot { animation: none; } }

/* Switch */
.switch-wrap { display: flex; align-items: center; gap: 6px; font-size: 13px; color: var(--fg-muted); }
.switch { position: relative; display: inline-block; width: 36px; height: 20px; }
.switch input { opacity: 0; width: 0; height: 0; }
.switch-track { position: absolute; inset: 0; background: var(--border); border-radius: 9999px; transition: background .2s; cursor: pointer; }
.switch input:checked + .switch-track { background: var(--fg); }
.switch-thumb { position: absolute; top: 2px; left: 2px; width: 16px; height: 16px; background: var(--bg); border-radius: 50%; transition: transform .2s; pointer-events: none; }
.switch input:checked ~ .switch-thumb { transform: translateX(16px); }
.switch input:focus-visible + .switch-track { outline: 2px solid var(--ring); outline-offset: 2px; }

/* Buttons */
.btn { display: inline-flex; align-items: center; gap: 6px; padding: 6px 12px; border-radius: var(--radius); font-size: 13px; font-weight: 500; cursor: pointer; border: 1px solid var(--border); background: var(--card-bg); color: var(--fg); transition: opacity .1s; }
.btn:hover { opacity: .8; }
.btn:disabled { opacity: .4; cursor: not-allowed; }
.btn-ghost { background: transparent; border-color: transparent; }
.btn-destructive { background: var(--status-critical); color: var(--status-critical-text); border-color: transparent; }

/* Banners / alerts */
.banner { display: flex; align-items: flex-start; gap: 10px; padding: 12px 16px; border-radius: var(--radius); border: 1px solid; margin-bottom: 20px; font-size: 13px; }
.banner-info { background: oklch(0.97 0.01 240); border-color: oklch(0.85 0.05 240); color: oklch(0.3 0.03 240); }
.banner-warning { background: oklch(0.98 0.03 80); border-color: oklch(0.88 0.1 80); color: oklch(0.35 0.05 80); }
.banner-critical { background: oklch(0.97 0.02 20); border-color: oklch(0.85 0.08 20); color: oklch(0.35 0.05 20); }
@media (prefers-color-scheme: dark) {
  .banner-info { background: oklch(0.2 0.02 240); border-color: oklch(0.35 0.05 240); color: oklch(0.85 0.02 240); }
  .banner-warning { background: oklch(0.2 0.03 80); border-color: oklch(0.35 0.1 80); color: oklch(0.88 0.03 80); }
  .banner-critical { background: oklch(0.2 0.02 20); border-color: oklch(0.35 0.08 20); color: oklch(0.88 0.02 20); }
}
.banner-icon { flex-shrink: 0; margin-top: 1px; }
.hidden { display: none !important; }

/* Sections */
section { margin-bottom: 32px; }
.section-heading { font-size: 13px; font-weight: 500; color: var(--fg-muted); text-transform: uppercase; letter-spacing: .06em; margin-bottom: 12px; }

/* Cards */
.card { background: var(--card-bg); border: 1px solid var(--border); border-radius: var(--radius); padding: 20px; }

/* Stat grid */
.stat-grid { display: grid; gap: 16px; }
.stat-grid-live { grid-template-columns: 1fr; }
@media (min-width: 768px) { .stat-grid-live { grid-template-columns: 1fr 1fr; } }
.live-band { display: grid; grid-template-columns: 1fr; gap: 0; }
@media (min-width: 768px) {
  .live-band { grid-template-columns: auto 1px 1fr; align-items: stretch; }
}
.live-band-sep { background: var(--border); width: 1px; display: none; }
@media (min-width: 768px) { .live-band-sep { display: block; } }
.live-stats { display: flex; flex-direction: column; gap: 0; padding-left: 0; }
@media (min-width: 768px) { .live-stats { padding-left: 24px; } }

.stat-grid-3 { display: grid; grid-template-columns: 1fr; gap: 16px; }
@media (min-width: 640px) { .stat-grid-3 { grid-template-columns: repeat(2, 1fr); } }
@media (min-width: 1024px) { .stat-grid-3 { grid-template-columns: repeat(3, 1fr); } }

.throttle-grid { display: grid; grid-template-columns: 1fr; gap: 16px; }
@media (min-width: 768px) { .throttle-grid { grid-template-columns: auto 1fr; } }

/* Stat tile */
.stat-tile { display: flex; flex-direction: column; gap: 4px; }
.stat-label { font-size: 13px; color: var(--fg-muted); display: flex; align-items: center; gap: 4px; }
.stat-value { font-size: 30px; font-weight: 600; line-height: 1.1; }
.stat-value-hero { font-size: 48px; }
.stat-sub { font-size: 12px; color: var(--fg-muted); }

/* Fetching opacity */
.fetching .stat-value { opacity: .6; }

/* Progress / meter */
.meter-wrap { margin-top: 6px; }
.meter { width: 100%; height: 6px; background: var(--border); border-radius: 3px; overflow: hidden; }
.meter-fill { height: 100%; background: var(--fg); border-radius: 3px; transition: width .3s; }
.meter-text { font-size: 12px; color: var(--fg-muted); margin-top: 4px; }

/* Uninstrumented card */
.uninstr-card { background: var(--card-bg); border: 1px solid var(--border); border-radius: var(--radius); padding: 16px; }
.uninstr-title { font-size: 13px; font-weight: 500; color: var(--fg-muted); margin-bottom: 6px; }
.uninstr-list { list-style: none; font-size: 13px; color: var(--fg-muted); }
.uninstr-list li { padding: 2px 0; }
.uninstr-note { font-size: 12px; color: var(--fg-muted); margin-top: 8px; line-height: 1.4; }

/* Storage table */
.storage-table-wrap { overflow: hidden; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th { text-align: left; padding: 8px 12px; font-size: 12px; font-weight: 500; color: var(--fg-muted); border-bottom: 1px solid var(--border); }
td { padding: 8px 12px; border-bottom: 1px solid var(--border); }
tr:last-child td { border-bottom: none; }
.td-num { font-variant-numeric: tabular-nums; }
.badge { display: inline-flex; align-items: center; padding: 1px 8px; border-radius: 9999px; font-size: 11px; font-weight: 500; }
.badge-exact { background: oklch(0.95 0.02 140); color: oklch(0.3 0.06 140); }
.badge-est { background: oklch(0.95 0.03 80); color: oklch(0.35 0.06 80); }
.badge-unknown { background: var(--border); color: var(--fg-muted); }
@media (prefers-color-scheme: dark) {
  .badge-exact { background: oklch(0.25 0.04 140); color: oklch(0.85 0.04 140); }
  .badge-est { background: oklch(0.25 0.05 80); color: oklch(0.88 0.04 80); }
  .badge-unknown { background: oklch(0.2 0 0); color: var(--fg-muted); }
}

/* Mobile storage: stacked */
@media (max-width: 639px) {
  .storage-table-wrap table, .storage-table-wrap thead, .storage-table-wrap tbody,
  .storage-table-wrap th, .storage-table-wrap td, .storage-table-wrap tr { display: block; }
  .storage-table-wrap thead { display: none; }
  .storage-table-wrap tr { border-bottom: 1px solid var(--border); padding: 8px 0; }
  .storage-table-wrap tr:last-child { border-bottom: none; }
  .storage-table-wrap td { border-bottom: none; padding: 2px 12px; }
  .storage-table-wrap td::before { content: attr(data-label); font-weight: 500; color: var(--fg-muted); margin-right: 6px; font-size: 12px; }
}

/* Tooltip */
.tooltip-wrap { position: relative; display: inline-flex; }
.tooltip-btn { background: none; border: none; cursor: pointer; color: var(--fg-muted); padding: 0 2px; font-size: 13px; line-height: 1; border-radius: 3px; }
.tooltip-popup { display: none; position: absolute; bottom: calc(100% + 6px); left: 50%; transform: translateX(-50%); background: var(--fg); color: var(--bg); border-radius: 6px; padding: 6px 10px; font-size: 12px; white-space: nowrap; max-width: 280px; white-space: normal; z-index: 10; pointer-events: none; }
.tooltip-wrap:hover .tooltip-popup,
.tooltip-btn:focus + .tooltip-popup,
.tooltip-btn[aria-expanded="true"] + .tooltip-popup { display: block; }

/* Logout form inline */
.logout-form { display: inline; }

/* Separator */
.sep { border: none; border-top: 1px solid var(--border); margin: 12px 0; }
</style>
</head>
<body>
<a href="#main" class="skip">Skip to content</a>

<div class="container">
  <!-- Header -->
  <header class="header">
    <div class="header-title">
      <h1>Operations</h1>
      <span class="subtitle">telegram-server</span>
    </div>
    <div class="header-controls">
      <!-- Freshness chip: aria-live so state transitions are announced -->
      <span id="chip" class="chip chip-good" role="status" aria-live="polite" title="Server snapshot: {{.ServerTimestamp}}">
        <span class="chip-dot"></span>
        <span id="chip-text">Live &middot; updated now</span>
      </span>

      <!-- Auto-refresh switch -->
      <div class="switch-wrap">
        <label class="switch" for="ar-toggle">
          <input type="checkbox" id="ar-toggle" checked aria-label="Auto-refresh">
          <span class="switch-track"></span>
          <span class="switch-thumb"></span>
        </label>
        <span>Auto-refresh</span>
      </div>

      <!-- Manual refresh -->
      <button id="refresh-btn" class="btn btn-ghost" type="button" aria-label="Refresh now">&#8635; Refresh</button>

      <!-- Logout -->
      <form method="post" action="/admin/logout" class="logout-form">
        <input type="hidden" name="csrf_token" value="{{.CSRFToken}}">
        <button type="submit" class="btn btn-ghost">Log out</button>
      </form>
    </div>
  </header>

  <main id="main">
    <!-- Banners — managed by JS; hidden on server render -->
    <div id="banner-empty" class="banner banner-info{{if not .ShowEmptyAlert}} hidden{{end}}" role="alert" aria-atomic="true">
      <span class="banner-icon">&#8505;</span>
      <span>No activity recorded yet. Every value below is a real zero, not missing data.</span>
    </div>
    <div id="banner-stale" class="banner banner-warning hidden" role="alert" aria-atomic="true">
      <span class="banner-icon">&#9888;</span>
      <span id="banner-stale-text">Data may be stale.</span>
    </div>
    <div id="banner-disconnected" class="banner banner-critical hidden" role="alert" aria-atomic="true">
      <span class="banner-icon">&#9888;</span>
      <span id="banner-dc-text">Can&#39;t reach the server.</span>
      <button id="retry-btn" class="btn btn-ghost" type="button" style="margin-left:auto">Retry</button>
    </div>
    <div id="banner-signedout" class="banner banner-critical hidden" role="alert" aria-atomic="true">
      <span class="banner-icon">&#128274;</span>
      <span>Session expired.</span>
      <a href="/admin/login" class="btn btn-ghost" style="margin-left:auto">Log in</a>
    </div>

    <!-- LIVE section -->
    <section aria-labelledby="h-live">
      <h2 id="h-live" class="section-heading">Live</h2>
      <div class="card">
        <div class="live-band">
          <!-- Hero: connections -->
          <dl class="stat-tile" style="padding-right:24px;padding-bottom:16px">
            <dt class="stat-label">Live connections</dt>
            <dd class="stat-value stat-value-hero" id="v-connections" data-metric="connections">{{.Connections}}</dd>
          </dl>
          <div class="live-band-sep"></div>
          <!-- Three secondary stats -->
          <dl class="live-stats">
            <div class="stat-tile" style="padding:12px 0;border-bottom:1px solid var(--border)">
              <dt class="stat-label">
                Authenticated sessions
                <span class="tooltip-wrap">
                  <button class="tooltip-btn" type="button" aria-describedby="tip-sessions">&#9432;</button>
                  <span class="tooltip-popup" id="tip-sessions" role="tooltip">Distinct users with at least one live connection.</span>
                </span>
              </dt>
              <dd class="stat-value" style="font-size:24px" id="v-sessions" data-metric="sessions">{{.Sessions}}</dd>
            </div>
            <div class="stat-tile" style="padding:12px 0;border-bottom:1px solid var(--border)">
              <dt class="stat-label">Messages last hour</dt>
              <dd class="stat-value" style="font-size:24px" id="v-messages_1h" data-metric="messages_1h">{{.Messages1H}}</dd>
            </div>
            <div class="stat-tile" style="padding:12px 0">
              <dt class="stat-label">
                Max pts spread
                <span class="tooltip-wrap">
                  <button class="tooltip-btn" type="button" aria-describedby="tip-pts">&#9432;</button>
                  <span class="tooltip-popup" id="tip-pts" role="tooltip">Spread between the furthest-ahead and furthest-behind account across all connected sessions. Not a lag from the head of the stream.</span>
                </span>
              </dt>
              <dd class="stat-value" style="font-size:24px" id="v-max_pts_gap" data-metric="max_pts_gap">{{.MaxPtsGap}}</dd>
            </div>
          </dl>
        </div>
      </div>
    </section>

    <!-- ACCOUNTS section -->
    <section aria-labelledby="h-accounts">
      <h2 id="h-accounts" class="section-heading">Accounts</h2>
      <div class="stat-grid stat-grid-3">
        <div class="card">
          <dl class="stat-tile">
            <dt class="stat-label">Total accounts</dt>
            <dd class="stat-value" id="v-total_users" data-metric="total_users">{{.TotalUsers}}</dd>
          </dl>
        </div>
        <div class="card">
          <dl class="stat-tile">
            <dt class="stat-label">
              Active last hour
              <span class="tooltip-wrap">
                <button class="tooltip-btn" type="button" aria-describedby="tip-active1h">&#9432;</button>
                <span class="tooltip-popup" id="tip-active1h" role="tooltip">Accounts with last_seen_at in the last hour.</span>
              </span>
            </dt>
            <dd class="stat-value" id="v-active_users_1h" data-metric="active_users_1h">{{.ActiveUsers1H}}</dd>
          </dl>
        </div>
        <div class="card">
          <dl class="stat-tile">
            <dt class="stat-label">Active last 24 hours</dt>
            <dd class="stat-value" id="v-active_users_24h" data-metric="active_users_24h">{{.ActiveUsers24H}}</dd>
            <div class="meter-wrap{{if not .ActiveUsersMeta}} hidden{{end}}" id="meter-wrap">
              <div class="meter" role="presentation"><div class="meter-fill" id="meter-fill" style="width:{{printf "%.0f" .ActiveUsersPct}}%"></div></div>
              <div class="meter-text" id="meter-text">{{.ActiveUsersMeta}}</div>
            </div>
          </dl>
        </div>
      </div>
    </section>

    <!-- CONTENT section -->
    <section aria-labelledby="h-content">
      <h2 id="h-content" class="section-heading">Content</h2>
      <div class="stat-grid stat-grid-3">
        <div class="card">
          <dl class="stat-tile">
            <dt class="stat-label">Channels</dt>
            <dd class="stat-value" id="v-total_channels" data-metric="total_channels">{{.TotalChannels}}</dd>
          </dl>
        </div>
        <div class="card">
          <dl class="stat-tile">
            <dt class="stat-label">Group chats</dt>
            <dd class="stat-value" id="v-total_chats" data-metric="total_chats">{{.TotalChats}}</dd>
          </dl>
        </div>
        <div class="card">
          <dl class="stat-tile">
            <dt class="stat-label">Messages last 24 hours</dt>
            <dd class="stat-value" id="v-messages_24h" data-metric="messages_24h">{{.Messages24H}}</dd>
          </dl>
        </div>
      </div>
    </section>

    <!-- THROTTLING section -->
    <section aria-labelledby="h-throttle">
      <h2 id="h-throttle" class="section-heading">Throttling</h2>
      <div class="throttle-grid">
        <div class="card" style="min-width:160px">
          <dl class="stat-tile">
            <dt class="stat-label">
              Active rate limits
              <span class="tooltip-wrap">
                <button class="tooltip-btn" type="button" aria-describedby="tip-ratelimit">&#9432;</button>
                <span class="tooltip-popup" id="tip-ratelimit" role="tooltip">Rate-limit rows that have not yet expired — a live count, not a rate. Some throttling is normal.</span>
              </span>
            </dt>
            <dd class="stat-value" id="v-rate_limit_active" data-metric="rate_limit_active">{{.RateLimitActive}}</dd>
          </dl>
        </div>
        {{if .UninstrumentedNames}}
        <div class="uninstr-card">
          <div class="uninstr-title">Not yet instrumented</div>
          <ul class="uninstr-list">
            {{range .UninstrumentedNames}}<li>{{.Label}}</li>{{end}}
          </ul>
          <p class="uninstr-note">These are not measured yet — the fields exist so the schema is stable. Not a reading of zero.</p>
        </div>
        {{end}}
      </div>
    </section>

    <!-- STORAGE section -->
    <section aria-labelledby="h-storage">
      <h2 id="h-storage" class="section-heading">Storage</h2>
      <div class="card storage-table-wrap">
        <table>
          <thead>
            <tr>
              <th scope="col">Table</th>
              <th scope="col">Rows</th>
              <th scope="col">Source</th>
            </tr>
          </thead>
          <tbody id="storage-tbody">
            {{range .StorageRows}}
            <tr>
              <td data-label="Table"><code>{{.Table}}</code></td>
              <td data-label="Rows" class="td-num"
                {{if eq .Source "Unknown"}}title="Estimate not available yet — Postgres has not analyzed this table."{{end}}>{{.Display}}</td>
              <td data-label="Source">
                {{if eq .Source "Exact"}}<span class="badge badge-exact">Exact</span>
                {{else if eq .Source "Estimated"}}<span class="badge badge-est">Estimated</span>
                {{else}}<span class="badge badge-unknown">Unknown</span>{{end}}
              </td>
            </tr>
            {{end}}
          </tbody>
        </table>
      </div>
    </section>
  </main>
</div>

<script>
(function() {
  'use strict';

  var uninstrumented = {{safeJS .UninstrumentedJSON}};
  var uninstrSet = new Set(uninstrumented);

  // Field name → element id mapping for scalar metrics
  var metricEls = {
    connections:      document.getElementById('v-connections'),
    sessions:         document.getElementById('v-sessions'),
    messages_1h:      document.getElementById('v-messages_1h'),
    max_pts_gap:      document.getElementById('v-max_pts_gap'),
    total_users:      document.getElementById('v-total_users'),
    active_users_1h:  document.getElementById('v-active_users_1h'),
    active_users_24h: document.getElementById('v-active_users_24h'),
    total_channels:   document.getElementById('v-total_channels'),
    total_chats:      document.getElementById('v-total_chats'),
    messages_24h:     document.getElementById('v-messages_24h'),
    rate_limit_active:document.getElementById('v-rate_limit_active'),
  };

  var chipEl   = document.getElementById('chip');
  var chipText = document.getElementById('chip-text');
  var arToggle = document.getElementById('ar-toggle');
  var refreshBtn = document.getElementById('refresh-btn');
  var retryBtn = document.getElementById('retry-btn');

  var bannerEmpty  = document.getElementById('banner-empty');
  var bannerStale  = document.getElementById('banner-stale');
  var bannerDc     = document.getElementById('banner-disconnected');
  var bannerOut    = document.getElementById('banner-signedout');
  var bannerDcText = document.getElementById('banner-dc-text');

  var lastOk = Date.now();   // time of last successful 200
  var lastFailed = false;    // true when the last fetch errored (network, non-2xx, JSON)
  var paused = false;
  var signedOut = false;
  var backoff = 10000;
  var timer = null;
  var fetching = false;

  // Number formatting
  var fmt = new Intl.NumberFormat('en-US');
  var fmtCompact = new Intl.NumberFormat('en-US', {notation:'compact', maximumSignificantDigits:3});

  function formatN(n) {
    if (n == null) return '—';
    if (Math.abs(n) > 9999999) return fmtCompact.format(n);
    return fmt.format(n);
  }

  function ago(ms) {
    var s = Math.round(ms / 1000);
    if (s < 60) return s + 's ago';
    var m = Math.round(s / 60);
    if (m < 60) return m + 'm ago';
    return Math.round(m / 60) + 'h ago';
  }

  // Chip state transitions announce to screen reader only on change
  var lastChipClass = 'chip-good';
  function setChip(cls, text, announce) {
    if (chipEl.className !== 'chip ' + cls) {
      chipEl.className = 'chip ' + cls;
    }
    chipText.textContent = text;
    if (announce && cls !== lastChipClass) {
      // Force re-announce by toggling aria-live content
      chipEl.setAttribute('aria-live', 'off');
      chipEl.setAttribute('aria-live', 'polite');
    }
    lastChipClass = cls;
  }

  function updateChip() {
    if (signedOut) { setChip('chip-critical', 'Signed out', true); return; }
    if (paused) { setChip('chip-paused', '⏸ Paused · updated ' + ago(Date.now() - lastOk), false); return; }
    var age = Date.now() - lastOk;
    if (lastFailed || age > 120000) {
      setChip('chip-critical', '● Disconnected · last updated ' + ago(age), true);
    } else if (age <= 40000) {
      setChip('chip-good', '● Live · updated ' + ago(age), true);
    } else {
      setChip('chip-warning', '● Stale · updated ' + ago(age), true);
    }
  }

  // Show/hide banners. Hides all four banners, then unhides el (if non-null).
  // bannerEmpty is managed separately by patchEmptyBanner on successful fetch;
  // it must be hidden here too so a 401 or disconnect doesn't show both.
  function showBanner(el) {
    [bannerEmpty, bannerStale, bannerDc, bannerOut].forEach(function(b) {
      b.classList.add('hidden');
    });
    if (el) el.classList.remove('hidden');
  }

  function updateBanners(age) {
    if (signedOut) { showBanner(bannerOut); return; }
    if (lastFailed || age > 120000) {
      bannerDcText.textContent = "Can't reach the server. Last updated " + ago(Date.now() - lastOk) + '.';
      showBanner(bannerDc);
    } else {
      showBanner(null);
    }
  }

  // Patch scalar metrics
  function patchScalars(data) {
    var keys = {
      connections:      data.connections,
      sessions:         data.sessions,
      messages_1h:      data.messages_1h,
      max_pts_gap:      data.max_pts_gap,
      total_users:      data.total_users,
      active_users_1h:  data.active_users_1h,
      active_users_24h: data.active_users_24h,
      total_channels:   data.total_channels,
      total_chats:      data.total_chats,
      messages_24h:     data.messages_24h,
      rate_limit_active:data.rate_limit_active,
    };
    Object.keys(keys).forEach(function(k) {
      var el = metricEls[k];
      if (!el) return;
      if (uninstrSet.has(k)) return; // never render uninstrumented as number
      var v = keys[k];
      el.textContent = (v == null || (v < 0)) ? '—' : formatN(v);
    });
  }

  // Patch active users meter. The scaffold is always in the DOM (even when
  // initial total_users=0), so elements are always found.
  function patchMeter(data) {
    var wrap = document.getElementById('meter-wrap');
    var fill = document.getElementById('meter-fill');
    var txt  = document.getElementById('meter-text');
    if (!wrap || !fill || !txt) return;
    var total  = data.total_users || 0;
    var active = data.active_users_24h || 0;
    if (total <= 0) {
      wrap.classList.add('hidden');
      return;
    }
    var pct = Math.min(100, active / total * 100);
    fill.style.width = pct.toFixed(0) + '%';
    txt.textContent = formatN(active) + ' of ' + formatN(total) + ' (' + Math.round(pct) + '%)';
    wrap.classList.remove('hidden');
  }

  // Patch empty-deployment banner
  function patchEmptyBanner(data) {
    var none = (data.total_users === 0) && (data.connections === 0) && (data.messages_24h === 0);
    bannerEmpty.classList.toggle('hidden', !none);
  }

  // Uninstrumented field label map (mirrors Go-side uninstrumentedLabels).
  var uninstrLabels = {
    'notify_count':        'NOTIFY events per hour',
    'push_latency_p50_ms': 'Push latency p50',
    'push_latency_p95_ms': 'Push latency p95',
  };

  // Rebuild uninstrSet and the card DOM from each poll response.
  // This ensures a field newly listed in a response stops rendering as a
  // number and appears in the card immediately.
  function patchUninstrumented(data) {
    var arr = data.uninstrumented;
    if (!Array.isArray(arr)) return;
    uninstrSet = new Set(arr);
    var list = document.querySelector('.uninstr-list');
    if (!list) return;
    list.innerHTML = '';
    arr.forEach(function(field) {
      var li = document.createElement('li');
      li.textContent = uninstrLabels[field] || field;
      list.appendChild(li);
    });
  }

  // Patch storage table
  function patchStorage(data) {
    var rows = data.storage_rows;
    if (!rows) return;
    var tbody = document.getElementById('storage-tbody');
    if (!tbody) return;
    var exact = new Set(['users','channels','chats']);
    var order = ['users','messages','events','channels','channel_messages','chats','files','auth_keys'];
    var trs = tbody.querySelectorAll('tr');
    order.forEach(function(field, i) {
      var tr = trs[i];
      if (!tr) return;
      var val = rows[field];
      var tds = tr.querySelectorAll('td');
      if (tds.length < 3) return;
      var isExact = exact.has(field);
      var display, source;
      if (val == null || val < 0) {
        display = '—'; source = 'Unknown';
        tds[1].setAttribute('title', 'Estimate not available yet — Postgres has not analyzed this table.');
      } else if (isExact) {
        display = formatN(val); source = 'Exact';
        tds[1].removeAttribute('title');
      } else {
        display = '~' + formatN(val); source = 'Estimated';
        tds[1].removeAttribute('title');
      }
      tds[1].textContent = display;
      var badge = tds[2].querySelector('.badge');
      if (badge) {
        badge.textContent = source;
        badge.className = 'badge badge-' + (source === 'Exact' ? 'exact' : source === 'Estimated' ? 'est' : 'unknown');
      }
    });
  }

  // Main fetch
  function doFetch() {
    if (fetching) return;
    fetching = true;
    document.body.classList.add('fetching');

    fetch('/admin/metrics', {credentials: 'same-origin', cache: 'no-store'})
      .then(function(res) {
        if (res.status === 401) {
          signedOut = true;
          fetching = false;
          document.body.classList.remove('fetching');
          clearTimer();
          updateChip();
          showBanner(bannerOut);
          return null;
        }
        if (!res.ok) throw new Error('HTTP ' + res.status);
        return res.json();
      })
      .then(function(data) {
        if (!data) return;
        lastOk = Date.now();
        lastFailed = false;
        backoff = 10000;
        fetching = false;
        document.body.classList.remove('fetching');
        patchUninstrumented(data); // rebuild uninstrSet before patchScalars reads it
        patchScalars(data);
        patchMeter(data);
        patchStorage(data);
        updateChip();
        updateBanners(0);        // clear disconnect banners first
        patchEmptyBanner(data);  // then show empty-alert if all-zeros
        scheduleNext(10000);
      })
      .catch(function() {
        fetching = false;
        lastFailed = true;
        document.body.classList.remove('fetching');
        backoff = Math.min(backoff * 2, 60000);
        updateChip();
        updateBanners(0); // lastFailed=true forces Disconnected regardless of age
        scheduleNext(backoff);
      });
  }

  function clearTimer() { if (timer) { clearTimeout(timer); timer = null; } }

  function scheduleNext(ms) {
    clearTimer();
    if (paused || signedOut) return;
    timer = setTimeout(doFetch, ms);
  }

  // Chip text update loop (runs even when paused to keep age fresh)
  function chipLoop() {
    updateChip();
    if (!signedOut) {
      var age = Date.now() - lastOk;
      if (!paused) updateBanners(age);
    }
    setTimeout(chipLoop, 5000);
  }

  // Controls
  arToggle.addEventListener('change', function() {
    paused = !this.checked;
    if (paused) {
      clearTimer();
    } else {
      scheduleNext(0);
    }
    updateChip();
  });

  refreshBtn.addEventListener('click', function() {
    if (signedOut) return;
    clearTimer();
    doFetch();
  });

  if (retryBtn) {
    retryBtn.addEventListener('click', function() {
      backoff = 10000;
      clearTimer();
      doFetch();
    });
  }

  // Tooltip keyboard support
  document.querySelectorAll('.tooltip-btn').forEach(function(btn) {
    btn.addEventListener('click', function() {
      var expanded = this.getAttribute('aria-expanded') === 'true';
      this.setAttribute('aria-expanded', expanded ? 'false' : 'true');
    });
    btn.addEventListener('keydown', function(e) {
      if (e.key === 'Escape') {
        this.setAttribute('aria-expanded', 'false');
        this.blur();
      }
    });
  });

  // Start
  scheduleNext(10000);
  chipLoop();
})();
</script>
</body>
</html>`
