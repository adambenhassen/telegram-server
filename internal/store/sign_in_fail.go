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
// checks that the phone belongs to an existing account, and charges the counter
// on failure. All of this happens within a single Postgres transaction protected by an
// advisory lock keyed on the IP bucket.
//
// Return contract:
//   - (nil, nil)            — code correct, proceed to auth
//   - (&RateLimitResult{}, nil) — budget exhausted (FLOOD_WAIT)
//   - (nil, ErrCodeInvalid) — wrong code or unknown phone (handler calls verifyToRPC)
//   - (nil, ErrCodeExpired) — expired code
//   - (nil, ErrCodeExhausted) — code exhausted
//   - (nil, error)          — internal error
//
// The advisory lock serializes same-IP requests across replicas. No refund
// step is needed because correct codes for existing accounts never touch the
// counter.
func (s *Store) AttemptSignIn(ctx context.Context, addr netip.Addr, phone, hash, code string, cfg RateLimitConfig) (*RateLimitResult, error) {
	// If rate limit is disabled, verify the code and require an existing account.
	if !cfg.enabled() {
		if err := s.verifyCodeWith(ctx, s.q, phone, hash, code); err != nil {
			return nil, err
		}
		registered, err := phoneUserExists(ctx, s.q, phone)
		if err != nil {
			return nil, fmt.Errorf("attempt sign in: lookup phone: %w", err)
		}
		if !registered {
			return nil, ErrCodeInvalid
		}
		return nil, nil //nolint:nilnil // successful existing-account sign-in
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
			// Wrong code: charge within the same transaction. Fail closed if
			// the charge fails or affects no rows — the rollback undoes the
			// verify writes (IncrementCodeAttempts / ConsumeCode) so the code
			// is not consumed and the client can retry.
			if err := chargeSignInFailure(ctx, qtx, key, cfg); err != nil {
				return nil, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("attempt sign in: %w", err)
		}
		return nil, verifyErr
	}

	// A valid code for an unknown phone must still look like a failed code
	// attempt. The account lookup occurs only after the budget gate above, and
	// the verification, refusal, and failure charge commit together.
	registered, err := phoneUserExists(ctx, qtx, phone)
	if err != nil {
		return nil, fmt.Errorf("attempt sign in: lookup phone: %w", err)
	}
	if !registered {
		if err := chargeSignInFailure(ctx, qtx, key, cfg); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("attempt sign in: %w", err)
		}
		return nil, ErrCodeInvalid
	}

	// Correct code for an existing account: commit with no charge.
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("attempt sign in: %w", err)
	}
	return nil, nil //nolint:nilnil // success returns nil result and nil error
}

func phoneUserExists(ctx context.Context, q *db.Queries, phone string) (bool, error) {
	phone = NormalizePhone(phone)
	_, err := q.UserByPhone(ctx, &phone)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return false, nil
	case err != nil:
		return false, err
	default:
		return true, nil
	}
}

func chargeSignInFailure(ctx context.Context, q *db.Queries, key netip.Prefix, cfg RateLimitConfig) error {
	// Wrong code and unknown-account refusals use the same conditional charge.
	// A zero-row result means another failure won the budget while this request
	// was waiting and must fail closed rather than granting a free attempt.
	n, err := q.ChargeSignInFailCall(ctx, db.ChargeSignInFailCallParams{
		IpKey:      key,
		Column2:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err != nil {
		return fmt.Errorf("sign in fail: charge: %w", err)
	}
	if n == 0 {
		return errors.New("sign in fail: charge affected no rows")
	}
	return nil
}

// SweepExpiredSignInFailCalls deletes per-IP signIn-failure rows past their
// deadline. Returns the total rows deleted.
func (s *Store) SweepExpiredSignInFailCalls(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredSignInFailCalls(ctx)
}
