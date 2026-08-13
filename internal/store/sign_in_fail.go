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

// signInFailLockClass namespaces the advisory locks used by AttemptSignIn.
// Two-argument form keeps this keyspace disjoint from other advisory locks.
const signInFailLockClass = 0x7369676e // "sign"

// AttemptSignIn atomically checks the per-IP failure budget, verifies the code,
// and charges the counter on failure — all within a single Postgres transaction
// protected by an advisory lock keyed on the IP bucket.
//
// Return contract:
//   - (nil, nil)            — code correct, proceed to auth
//   - (&RateLimitResult{}, nil) — budget exhausted (FLOOD_WAIT)
//   - (nil, ErrCodeInvalid) — wrong code (handler calls verifyToRPC)
//   - (nil, ErrCodeExpired) — expired code
//   - (nil, ErrCodeExhausted) — code exhausted
//   - (nil, error)          — internal error
//
// The advisory lock serializes same-IP requests across replicas. No refund
// step is needed: correct codes never touch the counter.
func (s *Store) AttemptSignIn(ctx context.Context, addr netip.Addr, phone, hash, code string, cfg RateLimitConfig) (*RateLimitResult, error) {
	// If rate limit is disabled, just verify the code.
	if !cfg.enabled() {
		return nil, s.verifyCodeWith(ctx, s.q, phone, hash, code)
	}

	key, ok := IPBucketKey(addr)
	if !ok {
		return &RateLimitResult{Wait: time.Second}, nil
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("attempt sign in: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Advisory lock on the IP bucket serializes same-IP requests.
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1, hashtext($2))",
		signInFailLockClass, key.String(),
	); err != nil {
		return nil, fmt.Errorf("attempt sign in: %w", err)
	}

	// Check the budget.
	row, err := qtx.CheckSignInFailBudget(ctx, key)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// No row exists — budget not exhausted. Proceed to verify.
	case err != nil:
		return nil, fmt.Errorf("attempt sign in: %w", err)
	default:
		// Row exists — check if budget is exhausted.
		if row.ExpiresAt.Time.After(time.Now()) && row.TokenCount >= int32(cfg.Limit) { //nolint:gosec // rate limits are small positive ints
			// Budget exhausted — compute wait.
			return &RateLimitResult{Wait: waitUntil(time.Now(), row.ExpiresAt.Time)}, nil
		}
		// Window expired or under limit — proceed to verify.
	}

	// Verify the code within the same transaction.
	verifyErr := s.verifyCodeWith(ctx, qtx, phone, hash, code)
	if verifyErr != nil {
		if errors.Is(verifyErr, ErrCodeInvalid) ||
			errors.Is(verifyErr, ErrCodeExpired) ||
			errors.Is(verifyErr, ErrCodeExhausted) {
			// Wrong code: charge within the same transaction. ErrNoRows is
			// expected if the budget hit between check and verify; other errors
			// are ignored because the verify error is the authoritative response.
			_ = qtx.ChargeSignInFailCall(ctx, db.ChargeSignInFailCallParams{ //nolint:errcheck // see comment
				IpKey:      key,
				Column2:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
				TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
			})
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("attempt sign in: %w", err)
		}
		return nil, verifyErr
	}

	// Correct code: commit with no charge.
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("attempt sign in: %w", err)
	}
	return nil, nil //nolint:nilnil // success returns nil result and nil error
}

// SweepExpiredSignInFailCalls deletes per-IP signIn-failure rows past their
// deadline. Returns the total rows deleted.
func (s *Store) SweepExpiredSignInFailCalls(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredSignInFailCalls(ctx)
}
