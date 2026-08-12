// Package admin provides read-only operational metrics for the M16 dashboard.
//
// The handler is auth-agnostic: it returns a JSON payload and does not perform
// any access check itself. An admin-only HTTP server in main.go is where
// middleware attaches — the handler can sit behind it with zero structural change
// to its handler code.
package admin

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// MetricsResponse is the JSON payload returned by GET /admin/metrics.
//
// All values are point-in-time snapshots or rolling-window counters. No per-user
// data, no PII, no message content.
//
// PushLatencyP50 and PushLatencyP95 are placeholder zeroes: push delivery
// latency is not yet instrumented. They are included so the response schema is
// stable when instrumentation lands.
type MetricsResponse struct {
	// Timestamp is the server time at which this snapshot was assembled.
	Timestamp time.Time `json:"timestamp"`

	// Connections is the number of currently open MTProto connections.
	Connections int `json:"connections"`
	// Sessions is the number of authenticated sessions (distinct users with
	// at least one live connection).
	Sessions int `json:"sessions"`

	// TotalUsers is the number of registered accounts.
	TotalUsers int64 `json:"total_users"`
	// ActiveUsers1H is the number of accounts with activity in the last hour.
	ActiveUsers1H int64 `json:"active_users_1h"`
	// ActiveUsers24H is the number of accounts with activity in the last 24 hours.
	ActiveUsers24H int64 `json:"active_users_24h"`

	// Messages1H is the number of message events dispatched in the last hour.
	Messages1H int64 `json:"messages_1h"`
	// Messages24H is the number of message events dispatched in the last 24 hours.
	Messages24H int64 `json:"messages_24h"`

	// TotalChannels is the number of channels.
	TotalChannels int64 `json:"total_channels"`
	// TotalChats is the number of group chats.
	TotalChats int64 `json:"total_chats"`

	// MaxPtsGap is the maximum pts gap observed across active sessions — how
	// far behind the furthest authenticated client is from the head of the
	// update stream. A gap of 0 means all clients are caught up.
	MaxPtsGap int64 `json:"max_pts_gap"`

	// NotifyCount is the number of Postgres NOTIFY events dispatched in the
	// last hour. Zero if not yet instrumented.
	NotifyCount int64 `json:"notify_count"`

	// PushLatencyP50 is the p50 push delivery latency in milliseconds.
	// Placeholder zero — push delivery latency is not yet instrumented.
	PushLatencyP50 float64 `json:"push_latency_p50_ms"`
	// PushLatencyP95 is the p95 push delivery latency in milliseconds.
	// Placeholder zero — push delivery latency is not yet instrumented.
	PushLatencyP95 float64 `json:"push_latency_p95_ms"`

	// RateLimitActive is the number of currently active rate-limit rows
	// (rows that have not yet expired), approximating recent throttling activity.
	RateLimitActive int64 `json:"rate_limit_active"`

	// StorageRows holds approximate row counts for key database tables.
	StorageRows StorageRows `json:"storage_rows"`
}

// StorageRows is approximate row counts across key database tables,
// suitable for monitoring storage growth without a full-table scan.
type StorageRows struct {
	Users           int64 `json:"users"`
	Messages        int64 `json:"messages"`
	Events          int64 `json:"events"`
	Channels        int64 `json:"channels"`
	ChannelMessages int64 `json:"channel_messages"`
	Chats           int64 `json:"chats"`
	Files           int64 `json:"files"`
	AuthKeys        int64 `json:"auth_keys"`
}

// Handler returns an http.HandlerFunc that serves operational metrics as JSON.
// The handler reads from the provided registry and store on each request and
// does not persist any state or start background goroutines.
func Handler(registry *mtproto.SessionRegistry, st *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		snap, err := st.Metrics(r.Context())
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			return
		}

		resp := MetricsResponse{
			Timestamp:       time.Now(),
			Connections:     registry.TotalConns(),
			Sessions:        registry.TotalSessions(),
			TotalUsers:      snap.TotalUsers,
			ActiveUsers1H:   snap.ActiveUsers1H,
			ActiveUsers24H:  snap.ActiveUsers24H,
			Messages1H:      snap.Messages1H,
			Messages24H:     snap.Messages24H,
			TotalChannels:   snap.TotalChannels,
			TotalChats:      snap.TotalChats,
			NotifyCount:     0, // not yet instrumented
			PushLatencyP50:  0, // not yet instrumented
			PushLatencyP95:  0, // not yet instrumented
			RateLimitActive: snap.RateLimitHits1H,
			StorageRows: StorageRows{
				Users:           snap.StorageRows.Users,
				Messages:        snap.StorageRows.Messages,
				Events:          snap.StorageRows.Events,
				Channels:        snap.StorageRows.Channels,
				ChannelMessages: snap.StorageRows.ChannelMessages,
				Chats:           snap.StorageRows.Chats,
				Files:           snap.StorageRows.Files,
				AuthKeys:        snap.StorageRows.AuthKeys,
			},
		}

		// Best-effort pts gap: if the query fails, report 0 rather than error
		// the whole response.
		if gap, err := st.MaxPtsGap(r.Context()); err == nil {
			resp.MaxPtsGap = gap
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}
