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

const (
	// signInFailLockClass namespaces this limiter's advisory locks. Distinct from
	// sendCodeIPLockClass so concurrent sendCode and failed-signIn checks from
	// the same IP do not unnecessarily serialize against each other.
	signInFailLockClass = 0x73696746 // "sigF"
)

// CheckAndChargeSignInFailIP checks and charges the per-IP signIn-failure
// counter for a failed auth.signIn attempt arriving from addr.
//
// It returns nil when the attempt is allowed (failure counter not exhausted),
// and a RateLimitResult carrying the remaining wait when the IP has exhausted
// its failure budget. A denied attempt has written nothing: the counter is
// charged inside a transaction, so a denial rolls back the token.
//
// The subject is the network the connection came from (IPv4 /32 or IPv6 /64),
// not the identifier being targeted. This ensures only the attacker's own
// network budget is consumed — a per-identifier key would let an attacker
// spend the victim's failure budget.
func (s *Store) CheckAndChargeSignInFailIP(ctx context.Context, addr netip.Addr, cfg RateLimitConfig) (*RateLimitResult, error) {
	if !cfg.enabled() {
		return nil, nil //nolint:nilnil // disabled config is not an error
	}
	key, ok := IPBucketKey(addr)
	if !ok {
		return nil, ErrNoClientAddr
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1, hashtext($2))",
		signInFailLockClass, key.String(),
	); err != nil {
		return nil, fmt.Errorf("advisory lock: %w", err)
	}
	qtx := s.q.WithTx(tx)

	_, err = qtx.TryConsumeSignInFailCall(ctx, db.TryConsumeSignInFailCallParams{
		IpKey:      key,
		Column2:    pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
		TokenCount: int32(cfg.Limit), //nolint:gosec // rate limits are small positive ints
	})
	if err == nil {
		// Allowed — token was consumed.
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit: %w", err)
		}
		return nil, nil //nolint:nilnil // allowed is not an error
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("consume sign in fail call: %w", err)
	}
	// Denied — read expires_at while the advisory lock is still held.
	expiresAt, err := qtx.GetSignInFailCallExpiry(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("sign in fail call expiry: %w", err)
	}
	return &RateLimitResult{Wait: waitUntil(time.Now(), expiresAt.Time)}, nil
}

// SweepExpiredSignInFailCalls deletes per-IP signIn-failure rows past their
// deadline. Returns the total rows deleted.
func (s *Store) SweepExpiredSignInFailCalls(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredSignInFailCalls(ctx)
}
