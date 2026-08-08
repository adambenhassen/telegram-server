package store

import (
	"context"
	"fmt"
	"math"
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
	// Always >= 1 second, rounded up from the actual remainder.
	Wait time.Duration
}

// CheckRateLimit checks whether subjectID is allowed to make a request on the
// given surface, consuming a token if allowed.
//
// It returns nil when the request is allowed. When denied, it returns a
// RateLimitResult with the remaining wait time. A wrapped error indicates a
// storage failure.
//
// Exactness under concurrency comes from the row-level lock taken by the
// INSERT ... ON CONFLICT query — different subjects never block each other.
func (s *Store) CheckRateLimit(ctx context.Context, subjectID int64, surface string, cfg RateLimitConfig) (*RateLimitResult, error) {
	if cfg.Limit <= 0 || cfg.Window <= 0 {
		// Disabled: allow everything.
		return nil, nil //nolint:nilnil // disabled config is not an error
	}

	row, err := s.q.CheckAndConsumeRateLimit(ctx, db.CheckAndConsumeRateLimitParams{
		SubjectID:  subjectID,
		Surface:    surface,
		Column3:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err != nil {
		return nil, fmt.Errorf("check rate limit: %w", err)
	}

	if row.Consumed {
		// Allowed — token was consumed.
		return nil, nil //nolint:nilnil // allowed is not an error
	}

	// Denied — compute wait time using Postgres timestamps only.
	// Window is the configured window; window_start is when the current
	// window began. The remaining wait is window - (now - window_start).
	// We use the Go clock here but only for the sub-second remainder — the
	// window_start came from Postgres and the window is a constant, so the
	// error is bounded to sub-second clock skew.
	wait := cfg.Window - time.Since(row.WindowStart.Time)
	// Round up to whole seconds and enforce minimum of 1.
	waitSecs := int(math.Ceil(float64(wait) / float64(time.Second)))
	waitSecs = max(waitSecs, 1)

	return &RateLimitResult{Wait: time.Duration(waitSecs) * time.Second}, nil
}

// SweepExpiredRateLimits deletes rate-limit rows whose window has fully
// expired, preventing unbounded growth of stale entries. Call periodically
// (e.g. on startup or via a scheduled sweep) rather than on every check — the
// window index keeps it efficient regardless.
func (s *Store) SweepExpiredRateLimits(ctx context.Context, window time.Duration) error {
	return s.q.SweepExpiredRateLimits(ctx, pgtype.Interval{Microseconds: window.Microseconds(), Valid: true})
}
