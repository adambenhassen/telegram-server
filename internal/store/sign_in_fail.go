package store

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrSignInFailBudgetExhausted is returned by ChargeSignInFailIP when the
// conditional upsert finds the budget already at the limit. This happens when
// concurrent requests pass the budget check simultaneously and race to charge
// — only one wins, the rest get this error. The handler fails closed on it.
var ErrSignInFailBudgetExhausted = errors.New("sign in fail budget exhausted")

// ConsumeSignInFailBudget atomically reserves one slot from the per-IP
// signIn-failure budget. Called before VerifyCode so that the attempt holds
// a slot regardless of the code's correctness:
//
// - Wrong code → slot is kept (net +1 to the counter)
// - Correct code → slot is refunded via RefundSignInFailBudget (net zero)
// - Internal error → slot is refunded (net zero)
//
// Returns a RateLimitResult when the budget is exhausted (no slot available),
// nil when a slot was reserved. Returns ErrNoClientAddr when the connection
// carries no address to attribute.
func (s *Store) ConsumeSignInFailBudget(ctx context.Context, addr netip.Addr, cfg RateLimitConfig) (*RateLimitResult, error) {
	if !cfg.enabled() {
		return nil, nil //nolint:nilnil // disabled config is not an error
	}
	key, ok := IPBucketKey(addr)
	if !ok {
		return nil, ErrNoClientAddr
	}

	_, err := s.q.ChargeSignInFailCall(ctx, db.ChargeSignInFailCallParams{
		IpKey:      key,
		Column2:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err == nil {
		return nil, nil //nolint:nilnil // slot reserved
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("consume sign in fail budget: %w", err)
	}
	// Budget exhausted — read expires_at to compute the wait.
	expiresAt, err := s.q.GetSignInFailCallExpiry(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("sign in fail call expiry: %w", err)
	}
	return &RateLimitResult{Wait: waitUntil(time.Now(), expiresAt.Time)}, nil
}

// RefundSignInFailBudget returns a previously reserved slot to the per-IP
// signIn-failure counter. Called after a successful verification or an internal
// error (no verification happened). Errors are logged but non-fatal.
func (s *Store) RefundSignInFailBudget(ctx context.Context, addr netip.Addr) error {
	key, ok := IPBucketKey(addr)
	if !ok {
		return ErrNoClientAddr
	}

	return s.q.RefundSignInFailCall(ctx, key)
}

// SweepExpiredSignInFailCalls deletes per-IP signIn-failure rows past their
// deadline. Returns the total rows deleted.
func (s *Store) SweepExpiredSignInFailCalls(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredSignInFailCalls(ctx)
}
