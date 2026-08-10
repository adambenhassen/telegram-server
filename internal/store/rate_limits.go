package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// RateLimitConfig holds the parameters for a single rate-limit surface.
type RateLimitConfig struct {
	// Limit is the maximum number of tokens (requests) allowed per window.
	// Zero disables enforcement for this surface.
	Limit int
	// Window is the duration of the fixed window.
	Window time.Duration
}

// enabled reports whether this config enforces anything. One definition, so the
// surfaces that check a limit and the ones that skip a disabled one cannot
// drift apart on what "disabled" means.
func (c RateLimitConfig) enabled() bool {
	return c.Limit > 0 && c.Window > 0
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
	if !cfg.enabled() {
		// Disabled: allow everything.
		return nil, nil //nolint:nilnil // disabled config is not an error
	}

	_, err := s.q.TryConsumeRateLimit(ctx, db.TryConsumeRateLimitParams{
		SubjectID:  subjectID,
		Surface:    surface,
		Column3:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err == nil {
		// Allowed — token was consumed.
		return nil, nil //nolint:nilnil // allowed is not an error
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("check rate limit: %w", err)
	}

	// Test hook: fires after INSERT denial, before GET.
	if s.deniedHook != nil {
		s.deniedHook()
	}

	// Denied — read expires_at in a fresh snapshot so we see the row committed
	// by the concurrent transaction that won the race. The sweep can delete the
	// row between the two reads: when it laps and the sweeper deletes it before
	// this GET fires, the row is gone. In that case the request is allowed — the
	// window that denied it has expired, and the sweep has already cleaned up.
	expiresAt, err := s.q.GetRateLimitExpiresAt(ctx, db.GetRateLimitExpiresAtParams{
		SubjectID: subjectID,
		Surface:   surface,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Row swept between INSERT and GET — window has expired, allow.
			return nil, nil //nolint:nilnil // swept row means expired window
		}
		return nil, fmt.Errorf("get rate limit: %w", err)
	}

	return &RateLimitResult{Wait: waitUntil(s.now(), expiresAt.Time)}, nil
}

// SweepExpiredRateLimits deletes rate-limit rows whose per-row expiry deadline
// has passed. The deadline is stored on the row (expires_at), so the sweep
// does not need to know per-surface window durations. Wired to a 5-minute
// background sweep in cmd/telegramd/main.go.
func (s *Store) SweepExpiredRateLimits(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredRateLimits(ctx)
}
