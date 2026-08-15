package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// Fragment and FragmentRenderer are defined here so DashboardFragmentRenderer
// compiles before internal/admin/sse.go (MAIN-303) lands. The definitions are
// byte-identical to the MAIN-303 originals and will be removed on rebase.

// Fragment is one server-rendered HTML update addressed to a named SSE event.
type Fragment struct {
	Event string
	HTML  string
}

// FragmentRenderer turns a metrics snapshot into SSE fragments.
type FragmentRenderer func(MetricsResponse) ([]Fragment, error)

// DashboardFragmentRenderer renders the metrics sections fragment using templ
// components. Pass it as BroadcasterConfig.Render to replace DefaultFragmentRenderer.
// The CSRF token is deliberately absent — fragments are rendered once per tick
// and fanned out to every connected client.
func DashboardFragmentRenderer(m MetricsResponse) ([]Fragment, error) {
	d := BuildDashboardData(m, "")
	var buf bytes.Buffer
	if err := metricsFragment(d).Render(context.Background(), &buf); err != nil {
		return nil, fmt.Errorf("render metrics fragment: %w", err)
	}
	return []Fragment{{Event: "metrics", HTML: buf.String()}}, nil
}

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

	// UninstrumentedFields is the raw field-name slice for the JS initializer.
	UninstrumentedFields []string

	// UninstrumentedJSON is the JSON-encoded JS array literal for the script block.
	UninstrumentedJSON string

	// Storage
	StorageRows []DashStorageRow

	// Banner state
	ShowEmptyAlert bool

	// Logout form
	CSRFToken string

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
		p := float64(m.ActiveUsers24H) / float64(m.TotalUsers) * 100
		d.ActiveUsersPct = p
		d.ActiveUsersMeta = fmt.Sprintf("%s of %s (%.0f%%)",
			FmtInt(m.ActiveUsers24H), FmtInt(m.TotalUsers), p)
	}

	// Uninstrumented card.
	d.UninstrumentedFields = m.Uninstrumented
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
	// JSON-encode the field list so the script block can consume it safely.
	// json.Marshal on []string cannot fail; the fallback guards a nil slice.
	jsonBytes, err := json.Marshal(m.Uninstrumented)
	if err != nil || jsonBytes == nil {
		jsonBytes = []byte("[]")
	}
	d.UninstrumentedJSON = string(jsonBytes)

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
// It server-renders the full operations dashboard using shadcn-templ components.
// tokenHash is the hex-encoded SHA-256 digest of TG_ADMIN_TOKEN_HASH, used to
// derive the session-bound CSRF token for the logout form.
func DashboardHandler(registry *mtproto.SessionRegistry, st *store.Store, tokenHash string) http.HandlerFunc {
	var cache metricsCache

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		cache.refresh(r.Context(), registry, st)
		if cache.failed() {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		m := cache.get()

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
			// Headers already sent; nothing to do.
			_ = err
			return
		}
	}
}
