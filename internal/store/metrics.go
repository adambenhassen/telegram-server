package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// MetricsSnapshot holds the database-side values needed for the admin metrics
// endpoint. All counts are approximate point-in-time snapshots.
type MetricsSnapshot struct {
	// TotalUsers is the number of registered accounts.
	TotalUsers int64
	// ActiveUsers1H is the number of users with activity in the last hour.
	ActiveUsers1H int64
	// ActiveUsers24H is the number of users with activity in the last 24 hours.
	ActiveUsers24H int64
	// TotalChannels is the number of channels.
	TotalChannels int64
	// TotalChats is the number of group chats.
	TotalChats int64
	// Messages1H is the number of message events (new, edit, delete) in the
	// last hour across all owners.
	Messages1H int64
	// Messages24H is the number of message events in the last 24 hours.
	Messages24H int64
	// RateLimitHits1H is the number of rate-limit rows currently active
	// (rows that have not yet expired). This approximates recent throttling
	// activity.
	RateLimitHits1H int64
	// StorageRows is an approximate row count across key tables, for
	// monitoring storage growth without a full-table scan.
	StorageRows StorageRows
}

// StorageRows holds approximate row counts for key tables.
type StorageRows struct {
	Users           int64
	Messages        int64
	Events          int64
	Channels        int64
	ChannelMessages int64
	Chats           int64
	Files           int64
	AuthKeys        int64
}

// Metrics returns a snapshot of operational metrics from the database.
func (s *Store) Metrics(ctx context.Context) (MetricsSnapshot, error) {
	var snap MetricsSnapshot

	now := time.Now()
	hourAgo := now.Add(-time.Hour)
	dayAgo := now.Add(-24 * time.Hour)

	// Total users.
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM users`).Scan(&snap.TotalUsers); err != nil {
		return snap, fmt.Errorf("count users: %w", err)
	}

	// Active users (last_seen_at within window).
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM users WHERE last_seen_at >= $1`, hourAgo,
	).Scan(&snap.ActiveUsers1H); err != nil {
		return snap, fmt.Errorf("count active users 1h: %w", err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM users WHERE last_seen_at >= $1`, dayAgo,
	).Scan(&snap.ActiveUsers24H); err != nil {
		return snap, fmt.Errorf("count active users 24h: %w", err)
	}

	// Channels and chats.
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM channels`).Scan(&snap.TotalChannels); err != nil {
		return snap, fmt.Errorf("count channels: %w", err)
	}
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM chats`).Scan(&snap.TotalChats); err != nil {
		return snap, fmt.Errorf("count chats: %w", err)
	}

	// Message volume in recent windows: counts non-deleted messages from the
	// messages table (1:1 + chat copies) and channel_messages, using the
	// messages.date column. message_events has no timestamp — it is ordered
	// by pts, not by wall clock.
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT count(*) FROM messages WHERE date >= $1 AND NOT deleted), 0)
		 + COALESCE((SELECT count(*) FROM channel_messages WHERE date >= $1 AND NOT deleted), 0)`,
		hourAgo,
	).Scan(&snap.Messages1H); err != nil {
		return snap, fmt.Errorf("count messages 1h: %w", err)
	}
	if err := s.pool.QueryRow(ctx,
		`SELECT COALESCE((SELECT count(*) FROM messages WHERE date >= $1 AND NOT deleted), 0)
		 + COALESCE((SELECT count(*) FROM channel_messages WHERE date >= $1 AND NOT deleted), 0)`,
		dayAgo,
	).Scan(&snap.Messages24H); err != nil {
		return snap, fmt.Errorf("count messages 24h: %w", err)
	}

	// Rate-limit rows currently active (not yet expired).
	if err := s.pool.QueryRow(ctx,
		`SELECT count(*) FROM rate_limits WHERE expires_at > $1`, now,
	).Scan(&snap.RateLimitHits1H); err != nil {
		return snap, fmt.Errorf("count rate limits: %w", err)
	}

	// Approximate row counts for storage monitoring. Users, channels, and chats
	// were already counted above; reuse those values. The remaining tables use
	// pg_class.reltuples — an estimate maintained by autovacuum — so the query
	// never triggers a sequential scan on a large table.
	var sr StorageRows
	sr.Users = snap.TotalUsers
	sr.Channels = snap.TotalChannels
	sr.Chats = snap.TotalChats
	sr.Messages = estimatedRows(ctx, s.pool, "messages")
	sr.Events = estimatedRows(ctx, s.pool, "message_events")
	sr.ChannelMessages = estimatedRows(ctx, s.pool, "channel_messages")
	sr.Files = estimatedRows(ctx, s.pool, "files")
	sr.AuthKeys = estimatedRows(ctx, s.pool, "auth_keys")

	snap.StorageRows = sr
	return snap, nil
}

// estimatedRows returns pg_class.reltuples for relation name as an int64.
// It is an estimate maintained by autovacuum and avoids a sequential scan.
func estimatedRows(ctx context.Context, pool *pgxpool.Pool, tableName string) int64 {
	var rows int64
	if err := pool.QueryRow(ctx,
		`SELECT reltuples::int8 FROM pg_class WHERE relname = $1`, tableName,
	).Scan(&rows); err != nil {
		return 0
	}
	return rows
}

// MaxPtsGap returns the maximum pts spread across accounts seen within the
// last five minutes. Accounts with older or missing last_seen_at values are
// excluded, and no recently seen accounts report a spread of 0.
func (s *Store) MaxPtsGap(ctx context.Context) (int64, error) {
	var gap int64
	err := s.pool.QueryRow(ctx, `
		SELECT COALESCE(MAX(us.pts) - MIN(us.pts), 0)
		FROM update_state us
		JOIN users u ON u.id = us.user_id
		WHERE u.last_seen_at >= NOW() - INTERVAL '5 minutes'
	`).Scan(&gap)
	if err != nil {
		return 0, fmt.Errorf("max pts gap: %w", err)
	}
	return gap, nil
}
