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

// Enabled reports whether this config enforces anything. One definition, so
// the surfaces that check a limit and the ones that skip a disabled one cannot
// drift apart on what "disabled" means.
func (c RateLimitConfig) Enabled() bool {
	return c.Limit > 0 && c.Window > 0
}

// enabled is the unexported alias used inside this package.
func (c RateLimitConfig) enabled() bool {
	return c.Enabled()
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
	return s.CheckRateLimitCost(ctx, subjectID, surface, cfg, 1)
}

// CheckRateLimitCost checks whether subjectID may spend cost tokens on the
// given surface, consuming them atomically if allowed. It returns nil when the
// request is allowed. When denied, it returns a RateLimitResult with the
// remaining wait time. A wrapped error indicates a storage failure.
func (s *Store) CheckRateLimitCost(ctx context.Context, subjectID int64, surface string, cfg RateLimitConfig, cost int) (*RateLimitResult, error) {
	if !cfg.enabled() {
		// Disabled: allow everything.
		return nil, nil //nolint:nilnil // disabled config is not an error
	}
	if cost <= 0 {
		return nil, fmt.Errorf("check rate limit: invalid cost %d", cost)
	}

	_, err := s.q.TryConsumeRateLimitCost(ctx, db.TryConsumeRateLimitCostParams{
		SubjectID:      subjectID,
		Surface:        surface,
		Cost:           int32(cost), //nolint:gosec // request costs are bounded at the RPC boundary
		WindowDuration: pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		LimitCount:     int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
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
			// A cost larger than the whole budget cannot fit even in a fresh
			// window, so a missing row is still a denial in that case. Otherwise
			// the row was swept between INSERT and GET and the window expired.
			if cost > cfg.Limit {
				now := s.now()
				return &RateLimitResult{Wait: waitUntil(now, now.Add(cfg.Window))}, nil
			}
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

// CheckRateLimitBudget reads the current budget for a subject/surface pair
// without consuming a token. Returns nil when no row exists (budget not yet
// exhausted) or when the window has expired. Returns a RateLimitResult when
// the budget is exhausted and the window is still open.
//
// This is the read-only half of the check-then-charge pattern used by
// auth.checkPassword: the limit is checked first, SRP verification runs second,
// and a failure charge follows only on bad proofs.
func (s *Store) CheckRateLimitBudget(ctx context.Context, subjectID int64, surface string, cfg RateLimitConfig) (*RateLimitResult, error) {
	if !cfg.enabled() {
		return nil, nil //nolint:nilnil // disabled config is not an error
	}

	row, err := s.q.CheckRateLimitBudget(ctx, db.CheckRateLimitBudgetParams{
		SubjectID: subjectID,
		Surface:   surface,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// No row — budget not exhausted.
		return nil, nil //nolint:nilnil
	case err != nil:
		return nil, fmt.Errorf("check rate limit budget: %w", err)
	}

	// Row exists — check if budget is exhausted.
	if row.ExpiresAt.Time.After(s.now()) && row.TokenCount >= int32(cfg.Limit) { //nolint:gosec // rate limits are small positive ints
		return &RateLimitResult{Wait: waitUntil(s.now(), row.ExpiresAt.Time)}, nil
	}
	// Window expired or under limit — proceed.
	return nil, nil //nolint:nilnil
}

// ChargeRateLimit charges a rate-limit counter after a failed attempt.
// It is the write half of the check-then-charge pattern: called only after
// a failure, so it always increments (or seeds at 1).
func (s *Store) ChargeRateLimit(ctx context.Context, subjectID int64, surface string, cfg RateLimitConfig) error {
	if !cfg.enabled() {
		return nil
	}
	return s.q.ChargeRateLimit(ctx, db.ChargeRateLimitParams{
		SubjectID: subjectID,
		Surface:   surface,
		Column3:   pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
	})
}

// ReserveRateLimit atomically reserves a token for the given subject/surface.
// Returns a RateLimitReservation on success (carrying the window start for
// potential refunds), a RateLimitResult on denial, or an error on storage
// failure.
//
// This is the admission step for the reserve-then-refund pattern used by
// auth.checkPassword: a token is consumed before SRP verification, and
// refunded on a valid proof.
func (s *Store) ReserveRateLimit(ctx context.Context, subjectID int64, surface string, cfg RateLimitConfig) (*RateLimitReservation, *RateLimitResult, error) {
	if !cfg.enabled() {
		return nil, nil, nil
	}

	row, err := s.q.TryConsumeRateLimit(ctx, db.TryConsumeRateLimitParams{
		SubjectID:  subjectID,
		Surface:    surface,
		Column3:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err == nil {
		// Allowed — token was consumed.
		return &RateLimitReservation{WindowStart: row.WindowStart.Time}, nil, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, nil, fmt.Errorf("reserve rate limit: %w", err)
	}

	// Test hook: fires after INSERT denial, before GET.
	if s.deniedHook != nil {
		s.deniedHook()
	}

	// Denied — read expires_at in a fresh snapshot.
	expiresAt, err := s.q.GetRateLimitExpiresAt(ctx, db.GetRateLimitExpiresAtParams{
		SubjectID: subjectID,
		Surface:   surface,
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			// Row swept between INSERT and GET — window has expired, allow.
			return nil, nil, nil
		}
		return nil, nil, fmt.Errorf("get rate limit: %w", err)
	}

	return nil, &RateLimitResult{Wait: waitUntil(s.now(), expiresAt.Time)}, nil
}

// RateLimitReservation carries the window start from a successful token
// reservation, so a refund can be scoped to the same window.
type RateLimitReservation struct {
	WindowStart time.Time
}

// RefundRateLimit refunds one token for a previously reserved rate-limit
// counter. Guarded on window_start so a refund never lands in a later window.
func (s *Store) RefundRateLimit(ctx context.Context, subjectID int64, surface string, res *RateLimitReservation) error {
	if res == nil {
		return nil
	}
	return s.q.RefundRateLimit(ctx, db.RefundRateLimitParams{
		SubjectID:   subjectID,
		Surface:     surface,
		WindowStart: pgtype.Timestamptz{Time: res.WindowStart, Valid: true},
	})
}
