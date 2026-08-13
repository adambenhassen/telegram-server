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

// signInFailLockClass namespaces the advisory lock that serializes the
// check/verify/charge sequence for a single IP. Without it, concurrent same-IP
// requests can all pass the budget check before any charge commits, exceeding
// the configured guess limit.
const signInFailLockClass = 0x73696746 // "sigF"

// CheckSignInFailIP reads the per-IP signIn-failure counter for addr and
// returns a RateLimitResult when the budget is exhausted. It acquires a
// transaction-scoped advisory lock so that concurrent same-IP requests cannot
// race the budget check and collectively exceed the configured limit. Called
// before VerifyCode so that even correct codes are blocked when the IP has
// burned its failure budget.
//
// The advisory lock must also be acquired before VerifyCode, meaning this call
// returns two callbacks: acquire() grabs the lock for the handler, and
// chargeLocked charges only while holding it (to preserve per-guess exclusivity).
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

	row, err := qtx.CheckSignInFailBudget(ctx, key)
	switch {
	case err == nil:
		// Row exists — check if at or over the limit.
		if int(row.TokenCount) >= cfg.Limit && row.ExpiresAt.Time.After(time.Now()) {
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit: %w", err)
			}
			return &RateLimitResult{Wait: waitUntil(time.Now(), row.ExpiresAt.Time)}, nil
		}
		// Under limit — commit the lock transaction so the lock is released.
		// The handler will re-acquire it via ChargeSignInFailIP.
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit: %w", err)
		}
		return nil, nil //nolint:nilnil // under limit or expired
	case errors.Is(err, pgx.ErrNoRows):
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("commit: %w", err)
		}
		return nil, nil //nolint:nilnil // no row = no failures yet
	default:
		return nil, fmt.Errorf("check sign in fail budget: %w", err)
	}
}

// ChargeSignInFailIP adds one token to the per-IP signIn-failure counter.
// Called only after VerifyCode returns an error. Upserts the row if it does
// not exist. Acquires the same advisory lock as CheckSignInFailIP so that
// the charge is serialized with the budget check across concurrent same-IP
// requests.
//
// No-op when the config is disabled.
func (s *Store) ChargeSignInFailIP(ctx context.Context, addr netip.Addr, cfg RateLimitConfig) error {
	if !cfg.enabled() {
		return nil
	}
	key, ok := IPBucketKey(addr)
	if !ok {
		return ErrNoClientAddr
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1, hashtext($2))",
		signInFailLockClass, key.String(),
	); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}
	qtx := s.q.WithTx(tx)

	if err := qtx.ChargeSignInFailCall(ctx, db.ChargeSignInFailCallParams{
		IpKey:   key,
		Column2: pgtype.Interval{Microseconds: cfg.Window.Microseconds(), Valid: true},
	}); err != nil {
		return fmt.Errorf("charge sign in fail call: %w", err)
	}

	return tx.Commit(ctx)
}

// SweepExpiredSignInFailCalls deletes per-IP signIn-failure rows past their
// deadline. Returns the total rows deleted.
func (s *Store) SweepExpiredSignInFailCalls(ctx context.Context) (int64, error) {
	return s.q.SweepExpiredSignInFailCalls(ctx)
}
