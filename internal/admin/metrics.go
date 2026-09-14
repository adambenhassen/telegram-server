package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// MetricsResponse is the JSON payload returned by GET /admin/metrics.
//
// All values are point-in-time snapshots or rolling-window counters. No per-user
// data, no PII, no message content.
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

	// MaxPtsGap is the maximum pts spread across accounts with a live connection.
	// Historical accounts that are no longer connected are excluded; no live
	// connections report a spread of 0.
	MaxPtsGap int64 `json:"max_pts_gap"`

	// DeliveryLag is the aggregate lag from each live connection's push
	// watermark to its account's authoritative current head.
	DeliveryLag DeliveryLag `json:"delivery_lag"`

	// NotifyCount is the number of Postgres NOTIFY events dispatched in the
	// rolling observation window on this replica. It is delivery work: summing
	// this value across replicas does not count unique committed events.
	NotifyCount int64 `json:"notify_count"`
	// NotifyWindowSeconds is the elapsed observation window, capped at one hour
	// and reset when the process starts.
	NotifyWindowSeconds float64 `json:"notify_window_seconds"`
	// NotifyRatePerSecond is NotifyCount divided by NotifyWindowSeconds.
	NotifyRatePerSecond float64 `json:"notify_rate_per_second"`
	// NotifyChannels is the fixed per-channel distribution of valid notifications.
	NotifyChannels NotifyChannels `json:"notify_channels"`
	// NotifyInvalid is the aggregate count of malformed or unknown
	// notifications. It has no channel label.
	NotifyInvalid int64 `json:"notify_invalid"`

	// PushLatencyP50 is the p50 persisted-update push latency in milliseconds.
	PushLatencyP50 float64 `json:"push_latency_p50_ms"`
	// PushLatencyP50Overflow reports whether the p50 rank landed in the final
	// bucket above the 60,000 ms finite bound.
	PushLatencyP50Overflow bool `json:"push_latency_p50_overflow"`
	// PushLatencyP95 is the p95 persisted-update push latency in milliseconds.
	PushLatencyP95 float64 `json:"push_latency_p95_ms"`
	// PushLatencyP95Overflow reports whether the p95 rank landed in the final
	// bucket above the 60,000 ms finite bound.
	PushLatencyP95Overflow bool `json:"push_latency_p95_overflow"`
	// PushLatencySampleCount is the number of successful latency samples in the
	// rolling observation window.
	PushLatencySampleCount int64 `json:"push_latency_sample_count"`
	// PushWindowSeconds is the rolling observation window used by push telemetry,
	// capped at one hour and reset at process start.
	PushWindowSeconds float64 `json:"push_window_seconds"`
	// PushOutcomes holds one fixed count for every attempted persisted-update
	// push result.
	PushOutcomes PushOutcomes `json:"push_outcomes"`
	// PushLatencyBucketUpperBoundsMS contains the 15 finite histogram bounds.
	// The matching counts array has one additional final overflow bucket.
	PushLatencyBucketUpperBoundsMS [15]float64 `json:"push_latency_bucket_upper_bounds_ms"`
	// PushLatencyBucketCounts contains one disjoint count per finite bound and a
	// final count for observations above 60,000 ms.
	PushLatencyBucketCounts [16]int64 `json:"push_latency_bucket_counts"`

	// RateLimitActive is the number of currently active rate-limit rows
	// (rows that have not yet expired), approximating recent throttling activity.
	RateLimitActive int64 `json:"rate_limit_active"`
	// RateLimitDenialsCount is the rolling count of requests that actually
	// returned FLOOD_WAIT from one of the fixed rate-limit surfaces.
	RateLimitDenialsCount int64 `json:"rate_limit_denials_count"`
	// RateLimitDenialsWindowSeconds is the process-local rolling observation
	// window, capped at one hour and reset when the process starts.
	RateLimitDenialsWindowSeconds float64 `json:"rate_limit_denials_window_seconds"`
	// RateLimitDenialsRatePerSecond is the fixed-surface denial count divided by
	// RateLimitDenialsWindowSeconds.
	RateLimitDenialsRatePerSecond float64 `json:"rate_limit_denials_rate_per_second"`
	// RateLimitDenialsBySurface holds exactly the fixed client-visible surfaces.
	RateLimitDenialsBySurface RateLimitDenialsBySurface `json:"rate_limit_denials_by_surface"`
	// RateLimitDenialsDropped counts denials whose internal surface was not in
	// the fixed contract. It is not included in the aggregate or breakdown.
	RateLimitDenialsDropped int64 `json:"rate_limit_denials_dropped"`

	// Uninstrumented lists the field names in this payload that are hardcoded
	// zeroes because the underlying metric is not yet instrumented. A field is
	// removed from this list when its instrumentation lands, so clients can
	// distinguish a genuine zero from an absent instrument without hard-coding
	// field names.
	Uninstrumented []string `json:"uninstrumented"`

	// StorageRows holds approximate row counts for key database tables.
	StorageRows StorageRows `json:"storage_rows"`
}

// NotifyChannels holds one count for each compiled Postgres notification
// channel. The fixed field set prevents input from creating metric series.
type NotifyChannels struct {
	Updates      int64 `json:"tg_updates"`
	Typing       int64 `json:"tg_typing"`
	Evict        int64 `json:"tg_evict"`
	ChannelPost  int64 `json:"tg_channel_post"`
	Encryption   int64 `json:"tg_encryption"`
	Status       int64 `json:"tg_status"`
	EncryptedMsg int64 `json:"tg_encrypted_msg"`
	Reactions    int64 `json:"tg_reactions"`
	Pinned       int64 `json:"tg_pinned"`
}

// PushOutcomes holds one count for every fixed persisted-update push result.
// No account, connection, peer, payload, or error label is retained.
type PushOutcomes struct {
	Success       int64 `json:"success"`
	OwnerMismatch int64 `json:"owner_mismatch"`
	EncodeFailure int64 `json:"encode_failure"`
	WriteFailure  int64 `json:"write_failure"`
}

// RateLimitDenialsBySurface holds one count for every fixed rate-limit
// surface. No caller-supplied label becomes a JSON key.
type RateLimitDenialsBySurface struct {
	MessageSend               int64 `json:"message_send"`
	CreateChat                int64 `json:"create_chat"`
	AddChatUser               int64 `json:"add_chat_user"`
	CreateChannel             int64 `json:"create_channel"`
	MessagesSearch            int64 `json:"messages_search"`
	ContactsSearch            int64 `json:"contacts_search"`
	MessagesSearchGlobal      int64 `json:"messages_search_global"`
	SaveFilePart              int64 `json:"save_file_part"`
	UploadGetFile             int64 `json:"upload_get_file"`
	SendCodeIPCalls           int64 `json:"send_code_ip_calls"`
	SendCodeIPDistinctNumbers int64 `json:"send_code_ip_distinct_numbers"`
	SignInFailIP              int64 `json:"sign_in_fail_ip"`
	CheckPassword             int64 `json:"check_password"`
	CheckPasswordIP           int64 `json:"check_password_ip"`
	GetPasswordIP             int64 `json:"get_password_ip"`
	SignUpIP                  int64 `json:"sign_up_ip"`
	PasswordProof             int64 `json:"password_proof"`
	GetPassword               int64 `json:"get_password"`
	UpdateProfile             int64 `json:"update_profile"`
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

// cacheRefresh is the minimum interval between metric refreshes. It prevents
// N dashboard tabs from costing N full query sets against the DB.
const cacheRefresh = 10 * time.Second

// metricsCache holds a stale snapshot and its collection time.
type metricsCache struct {
	mu      sync.Mutex
	last    time.Time
	resp    MetricsResponse
	lastErr bool // true if the most recent refresh attempt failed
}

// refresh re-reads metrics from the store and registry if enough time has
// elapsed since the last refresh.
func (c *metricsCache) refresh(ctx context.Context, reg *mtproto.SessionRegistry, st *store.Store, deliveryLag *DeliveryLagSampler, notifyMetrics ...*store.NotificationMetrics) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if time.Since(c.last) < cacheRefresh {
		return
	}

	resp, err := collectMetricsWithDeliveryLag(ctx, reg, st, tolerateGapFailure, deliveryLag, notifyMetrics...)
	if err != nil {
		slog.Error("admin metrics refresh", "err", err)
		c.lastErr = true
		return
	}

	c.resp = resp
	c.last = time.Now()
	c.lastErr = false
}

// Gap-failure tolerance for collectMetrics, named at the call sites because a
// bare boolean there says nothing.
const (
	// tolerateGapFailure degrades a failed pts-gap query to zero and serves the
	// rest of the snapshot. It is the JSON endpoint's long-standing behaviour:
	// a polling client sees the same field every 10 s and a transient zero
	// corrects itself on the next poll.
	tolerateGapFailure = true

	// requireAllMetrics fails the whole snapshot instead. The SSE stream needs
	// this: it only emits on a successful sample, and a zeroed MaxPtsGap
	// carrying a fresh timestamp would push a false "all clients caught up"
	// that nothing later contradicts.
	requireAllMetrics = false
)

// collectMetrics assembles one metrics snapshot from the store and the session
// registry for callers that do not need to share lag state. Production JSON,
// dashboard, and SSE handlers use collectMetricsWithDeliveryLag with the one
// process-local sampler instead.
func collectMetrics(ctx context.Context, reg *mtproto.SessionRegistry, st *store.Store, tolerateGapErr bool, notifyMetrics ...*store.NotificationMetrics) (MetricsResponse, error) {
	return collectMetricsWithDeliveryLag(ctx, reg, st, tolerateGapErr, NewDeliveryLagSampler(), notifyMetrics...)
}

// collectMetricsWithDeliveryLag assembles one snapshot using the supplied
// process-local lag sampler. A shared sampler lets JSON and SSE retain and
// expose the same complete sample while their database snapshots remain
// independently cached.
func collectMetricsWithDeliveryLag(ctx context.Context, reg *mtproto.SessionRegistry, st *store.Store, tolerateGapErr bool, deliveryLag *DeliveryLagSampler, notifyMetrics ...*store.NotificationMetrics) (MetricsResponse, error) {
	snap, err := st.Metrics(ctx)
	if err != nil {
		return MetricsResponse{}, fmt.Errorf("collect metrics: %w", err)
	}

	maxGap, err := st.MaxPtsGap(ctx, reg.Users()...)
	if err != nil {
		if !tolerateGapErr {
			return MetricsResponse{}, fmt.Errorf("collect max pts gap: %w", err)
		}
		maxGap = 0
	}
	if deliveryLag == nil {
		deliveryLag = NewDeliveryLagSampler()
	}
	lag := deliveryLag.sample(ctx, reg, st)

	notify := notificationSnapshot(notifyMetrics...)
	pushUninstrumented := pushMetricsUninstrumented(notifyMetrics...)
	return MetricsResponse{
		Timestamp:                      time.Now(),
		Connections:                    reg.TotalConns(),
		Sessions:                       reg.TotalSessions(),
		TotalUsers:                     snap.TotalUsers,
		ActiveUsers1H:                  snap.ActiveUsers1H,
		ActiveUsers24H:                 snap.ActiveUsers24H,
		Messages1H:                     snap.Messages1H,
		Messages24H:                    snap.Messages24H,
		TotalChannels:                  snap.TotalChannels,
		TotalChats:                     snap.TotalChats,
		MaxPtsGap:                      maxGap,
		DeliveryLag:                    lag,
		NotifyCount:                    notify.NotifyCount,
		NotifyWindowSeconds:            notify.WindowSeconds,
		NotifyRatePerSecond:            notify.RatePerSecond,
		NotifyChannels:                 notificationChannels(notify.Channels),
		NotifyInvalid:                  notify.Invalid,
		PushLatencyP50:                 notify.Push.P50Milliseconds,
		PushLatencyP50Overflow:         notify.Push.P50Overflow,
		PushLatencyP95:                 notify.Push.P95Milliseconds,
		PushLatencyP95Overflow:         notify.Push.P95Overflow,
		PushLatencySampleCount:         notify.Push.SampleCount,
		PushWindowSeconds:              notify.Push.WindowSeconds,
		PushOutcomes:                   pushOutcomes(notify.Push.Outcomes),
		PushLatencyBucketUpperBoundsMS: pushBucketUpperBounds(notify.Push),
		PushLatencyBucketCounts:        notify.Push.LatencyBucketCounts,
		Uninstrumented:                 pushUninstrumented,
		RateLimitActive:                snap.RateLimitHits1H,
		RateLimitDenialsCount:          notify.RateLimitDenials.Count,
		RateLimitDenialsWindowSeconds:  notify.RateLimitDenials.WindowSeconds,
		RateLimitDenialsRatePerSecond:  notify.RateLimitDenials.RatePerSecond,
		RateLimitDenialsBySurface:      rateLimitDenialsBySurface(notify.RateLimitDenials.BySurface),
		RateLimitDenialsDropped:        notify.RateLimitDenials.Dropped,
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
	}, nil
}

func notificationSnapshot(notifyMetrics ...*store.NotificationMetrics) store.NotificationMetricsSnapshot {
	if len(notifyMetrics) == 0 || notifyMetrics[0] == nil {
		return store.NotificationMetricsSnapshot{}
	}
	return notifyMetrics[0].Snapshot()
}

func applyNotificationSnapshot(resp *MetricsResponse, notifyMetrics *store.NotificationMetrics) {
	if notifyMetrics == nil {
		return
	}
	notify := notifyMetrics.Snapshot()
	resp.NotifyCount = notify.NotifyCount
	resp.NotifyWindowSeconds = notify.WindowSeconds
	resp.NotifyRatePerSecond = notify.RatePerSecond
	resp.NotifyChannels = notificationChannels(notify.Channels)
	resp.NotifyInvalid = notify.Invalid
	resp.PushLatencyP50 = notify.Push.P50Milliseconds
	resp.PushLatencyP50Overflow = notify.Push.P50Overflow
	resp.PushLatencyP95 = notify.Push.P95Milliseconds
	resp.PushLatencyP95Overflow = notify.Push.P95Overflow
	resp.PushLatencySampleCount = notify.Push.SampleCount
	resp.PushWindowSeconds = notify.Push.WindowSeconds
	resp.PushOutcomes = pushOutcomes(notify.Push.Outcomes)
	resp.PushLatencyBucketUpperBoundsMS = pushBucketUpperBounds(notify.Push)
	resp.PushLatencyBucketCounts = notify.Push.LatencyBucketCounts
	resp.Uninstrumented = removePushPercentileUninstrumented(resp.Uninstrumented)
	resp.RateLimitDenialsCount = notify.RateLimitDenials.Count
	resp.RateLimitDenialsWindowSeconds = notify.RateLimitDenials.WindowSeconds
	resp.RateLimitDenialsRatePerSecond = notify.RateLimitDenials.RatePerSecond
	resp.RateLimitDenialsBySurface = rateLimitDenialsBySurface(notify.RateLimitDenials.BySurface)
	resp.RateLimitDenialsDropped = notify.RateLimitDenials.Dropped
}

func pushMetricsUninstrumented(notifyMetrics ...*store.NotificationMetrics) []string {
	fields := []string{"push_latency_p50_ms", "push_latency_p95_ms"}
	if len(notifyMetrics) > 0 && notifyMetrics[0] != nil {
		return removePushPercentileUninstrumented(fields)
	}
	return fields
}

func removePushPercentileUninstrumented(fields []string) []string {
	if len(fields) == 0 {
		return fields
	}
	filtered := make([]string, 0, len(fields))
	for _, field := range fields {
		if field == "push_latency_p50_ms" || field == "push_latency_p95_ms" {
			continue
		}
		filtered = append(filtered, field)
	}
	return filtered
}

func notificationChannels(channels store.NotificationChannelCounts) NotifyChannels {
	return NotifyChannels{
		Updates:      channels.Updates,
		Typing:       channels.Typing,
		Evict:        channels.Evict,
		ChannelPost:  channels.ChannelPost,
		Encryption:   channels.Encryption,
		Status:       channels.Status,
		EncryptedMsg: channels.EncryptedMsg,
		Reactions:    channels.Reactions,
		Pinned:       channels.Pinned,
	}
}

func pushOutcomes(outcomes store.PushOutcomeCounts) PushOutcomes {
	return PushOutcomes{
		Success:       outcomes.Success,
		OwnerMismatch: outcomes.OwnerMismatch,
		EncodeFailure: outcomes.EncodeFailure,
		WriteFailure:  outcomes.WriteFailure,
	}
}

func rateLimitDenialsBySurface(counts store.RateLimitDenialSurfaceCounts) RateLimitDenialsBySurface {
	return RateLimitDenialsBySurface{
		MessageSend:               counts.MessageSend,
		CreateChat:                counts.CreateChat,
		AddChatUser:               counts.AddChatUser,
		CreateChannel:             counts.CreateChannel,
		MessagesSearch:            counts.MessagesSearch,
		ContactsSearch:            counts.ContactsSearch,
		MessagesSearchGlobal:      counts.MessagesSearchGlobal,
		SaveFilePart:              counts.SaveFilePart,
		UploadGetFile:             counts.UploadGetFile,
		SendCodeIPCalls:           counts.SendCodeIPCalls,
		SendCodeIPDistinctNumbers: counts.SendCodeIPDistinctNumbers,
		SignInFailIP:              counts.SignInFailIP,
		CheckPassword:             counts.CheckPassword,
		CheckPasswordIP:           counts.CheckPasswordIP,
		GetPasswordIP:             counts.GetPasswordIP,
		SignUpIP:                  counts.SignUpIP,
		PasswordProof:             counts.PasswordProof,
		GetPassword:               counts.GetPassword,
		UpdateProfile:             counts.UpdateProfile,
	}
}

func pushBucketUpperBounds(push store.PushMetricsSnapshot) [15]float64 {
	if push.LatencyBucketUpperBoundsMilliseconds == ([15]float64{}) {
		return store.PushLatencyBucketUpperBoundsMilliseconds()
	}
	return push.LatencyBucketUpperBoundsMilliseconds
}

// get returns the cached metrics response.
func (c *metricsCache) get() MetricsResponse {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resp
}

// failed reports whether the most recent refresh attempt produced an error.
// When true, the cached snapshot is stale due to a DB failure.
func (c *metricsCache) failed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastErr
}

// Handler returns an http.HandlerFunc that serves operational metrics as JSON.
// The handler reads from the provided registry and store on each request, but
// caches results for at least cacheRefresh (10 s) to prevent N dashboard tabs
// from costing N full query sets. No background goroutines are started.
func Handler(registry *mtproto.SessionRegistry, st *store.Store, notifyMetrics ...*store.NotificationMetrics) http.HandlerFunc {
	return HandlerWithDeliveryLag(registry, st, NewDeliveryLagSampler(), notifyMetrics...)
}

// HandlerWithDeliveryLag returns an admin metrics handler using a supplied
// lag sampler. The sampler can be shared with the SSE broadcaster so both
// authenticated surfaces expose the same retained complete sample.
func HandlerWithDeliveryLag(registry *mtproto.SessionRegistry, st *store.Store, deliveryLag *DeliveryLagSampler, notifyMetrics ...*store.NotificationMetrics) http.HandlerFunc {
	if deliveryLag == nil {
		deliveryLag = NewDeliveryLagSampler()
	}
	var cache metricsCache

	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		cache.refresh(r.Context(), registry, st, deliveryLag, notifyMetrics...)
		if cache.failed() {
			http.Error(w, "metrics unavailable", http.StatusServiceUnavailable)
			return
		}
		resp := cache.get()
		resp.DeliveryLag = deliveryLag.Snapshot()
		if len(notifyMetrics) > 0 {
			applyNotificationSnapshot(&resp, notifyMetrics[0])
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
		}
	}
}
