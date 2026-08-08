package store

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// RateLimitConfig holds the parameters for a single rate-limit surface.
type RateLimitConfig struct {
	// Limit is the maximum number of tokens (requests) allowed per window.
	// Zero disables enforcement for this surface.
	Limit int
	// Window is the duration of the sliding window.
	Window time.Duration
}

// RateLimitResult is returned by CheckRateLimit when the request is denied.
type RateLimitResult struct {
	// Wait is how long the subject must wait before the next request is allowed.
	// Always >= 1 second.
	Wait time.Duration
}

// CheckRateLimit checks whether subjectID is allowed to make a request on the
// given surface, consuming a token if allowed.
//
// It returns nil when the request is allowed. When denied, it returns a
// RateLimitResult with the remaining wait time. A wrapped error indicates a
// storage failure.
//
// The advisory lock key is derived from (subjectID, surface) so that
// concurrent requests for the same subject are serialised, but different
// subjects never block each other.
func (s *Store) CheckRateLimit(ctx context.Context, subjectID int64, surface string, cfg RateLimitConfig) (*RateLimitResult, error) {
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		// Disabled: allow everything.
		return nil, nil //nolint:nilnil // disabled config is not an error
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	// Serialize concurrent checks for the same (subject, surface). The lock key
	// is a hash of both values so different subjects never contend.
	lockKey := hashLockKey(subjectID, surface)
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", lockKey); err != nil {
		return nil, fmt.Errorf("advisory lock: %w", err)
	}

	qtx := s.q.WithTx(tx)

	// Try to update an existing row.
	rows, err := qtx.UpsertRateLimit(ctx, db.UpsertRateLimitParams{
		SubjectID:  subjectID,
		Surface:    surface,
		Column3:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err != nil {
		return nil, fmt.Errorf("upsert rate limit: %w", err)
	}

	if rows == 0 {
		// No row existed — seed it with the first token consumed.
		if err := qtx.InsertRateLimit(ctx, db.InsertRateLimitParams{
			SubjectID: subjectID,
			Surface:   surface,
		}); err != nil {
			return nil, fmt.Errorf("insert rate limit: %w", err)
		}
	}

	// Read the current state to determine if allowed or denied.
	row, err := qtx.GetRateLimit(ctx, db.GetRateLimitParams{
		SubjectID: subjectID,
		Surface:   surface,
	})
	if err != nil {
		return nil, fmt.Errorf("get rate limit: %w", err)
	}

	if row.TokenCount <= int32(cfg.Limit) { //nolint:gosec // rate limits are small positive ints
		// Allowed — token was consumed above.
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit: %w", err)
		}
		return nil, nil //nolint:nilnil // allowed is not an error
	}

	// Denied — compute wait time.
	wait := cfg.Window - time.Since(row.WindowStart.Time)
	wait = max(wait, time.Second)

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return &RateLimitResult{Wait: wait}, nil
}

// hashLockKey produces a deterministic int64 lock key from a subject ID and
// surface name. It uses a simple FNV-1a hash combined with the subject ID.
// Collisions are harmless — they just serialise more requests than necessary.
func hashLockKey(subjectID int64, surface string) int64 {
	// FNV-1a 64-bit hash of the surface, XOR'd with subjectID.
	var h uint64 = 14695981039346656037
	for i := range surface {
		h ^= uint64(surface[i])
		h *= 1099511628211
	}
	return int64(h) ^ subjectID //nolint:gosec // hash collision is acceptable for advisory locks
}
