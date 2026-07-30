package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrLookupQuotaExceeded is returned when a caller has exhausted their
// per-account phone lookup quota for the current window.
var ErrLookupQuotaExceeded = errors.New("phone lookup quota exceeded")

// LookupWindow is the rolling window for the per-account phone lookup quota.
const LookupWindow = 24 * time.Hour

// LookupLimit is the maximum number of distinct phone numbers a single account
// may look up within LookupWindow.
const LookupLimit = 20

// CheckAndChargeLookup checks whether callerID has room in their lookup quota
// for the given phone number, and records the attempt. It returns nil when the
// lookup is allowed, ErrLookupQuotaExceeded when the caller has exhausted their
// quota, and a wrapped error on storage failure.
//
// The check is atomic: expired rows are pruned, a new row is inserted, and the
// distinct phone count is verified against the limit. COUNT DISTINCT handles
// dedup so retries of the same phone do not double-charge.
func (s *Store) CheckAndChargeLookup(ctx context.Context, callerID int64, phone string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	qtx := s.q.WithTx(tx)
	cutoff := pgtype.Timestamptz{Time: time.Now().Add(-LookupWindow), Valid: true}

	// Prune expired rows so the count reflects only the current window.
	if err := qtx.DeleteExpiredPhoneLookups(ctx, cutoff); err != nil {
		return fmt.Errorf("prune lookups: %w", err)
	}

	// Insert the attempt. COUNT DISTINCT handles dedup for retries.
	if err := qtx.InsertPhoneLookup(ctx, db.InsertPhoneLookupParams{
		CallerID: callerID,
		Phone:    phone,
	}); err != nil {
		return fmt.Errorf("insert lookup: %w", err)
	}

	// Re-count after insert to verify the limit.
	count, err := qtx.CountPhoneLookups(ctx, db.CountPhoneLookupsParams{
		CallerID:   callerID,
		LookedUpAt: cutoff,
	})
	if err != nil {
		return fmt.Errorf("count lookups: %w", err)
	}
	if count > LookupLimit {
		return ErrLookupQuotaExceeded
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
