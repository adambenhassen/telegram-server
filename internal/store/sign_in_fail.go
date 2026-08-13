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

// CheckSignInFailIP reads the per-IP signIn-failure counter for addr and
// returns a RateLimitResult when the budget is exhausted. Read-only, no lock
// acquired — the race between check and charge is handled by ChargeSignInFailIP
// using a conditional upsert that returns pgx.ErrNoRows when the budget is
// already at the limit. Called before VerifyCode so that even correct codes
// are blocked when the IP has burned its failure budget.
//
// Returns nil (no error, no result) when the budget is not exhausted or the
// config is disabled. Returns ErrNoClientAddr when the connection carries no
// address to attribute.
func (s *Store) CheckSignInFailIP(ctx context.Context, addr netip.Addr, cfg RateLimitConfig) (*RateLimitResult, error) {
	if !cfg.enabled() {
		return nil, nil //nolint:nilnil // disabled config is not an error
	}
	key, ok := IPBucketKey(addr)
	if !ok {
		return nil, ErrNoClientAddr
	}

	row, err := s.q.CheckSignInFailBudget(ctx, key)
	switch {
	case err == nil:
		// Row exists — check if at or over the limit.
		if int(row.TokenCount) >= cfg.Limit && row.ExpiresAt.Time.After(time.Now()) {
			return &RateLimitResult{Wait: waitUntil(time.Now(), row.ExpiresAt.Time)}, nil
		}
		return nil, nil //nolint:nilnil // under limit or expired
	case errors.Is(err, pgx.ErrNoRows):
		return nil, nil //nolint:nilnil // no row = no failures yet
	default:
		return nil, fmt.Errorf("check sign in fail budget: %w", err)
	}
}

// ChargeSignInFailIP adds one token to the per-IP signIn-failure counter.
// Called only after VerifyCode returns an error. Uses a conditional upsert
// that returns pgx.ErrNoRows when the budget is already at the limit — this
// is what prevents the race: if N concurrent requests pass the check and
// all try to charge, only one succeeds and the rest get pgx.ErrNoRows,
// which the handler fails closed on (INTERNAL instead of PHONE_CODE_INVALID).
//
// No-op when the config is disabled. Returns an error when the budget is
// already exhausted (race case) or the connection has no client address.
func (s *Store) ChargeSignInFailIP(ctx context.Context, addr netip.Addr, cfg RateLimitConfig) error {
	if !cfg.enabled() {
		return nil
	}
	key, ok := IPBucketKey(addr)
	if !ok {
		return ErrNoClientAddr
	}

	_, err := s.q.ChargeSignInFailCall(ctx, db.ChargeSignInFailCallParams{
		IpKey:      key,
		Column2:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrSignInFailBudgetExhausted
	}
	if err != nil {
		return fmt.Errorf("charge sign in fail call: %w", err)
	}
	return nil
}

// SweepExpiredSignInFailCalls deletes per-IP signIn-failure rows past their
// deadline. Returns the total rows deleted.
func (s *Store) SweepExpiredSignInFailCalls(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredSignInFailCalls(ctx)
}
