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

// ErrUsernameLookupQuotaExceeded is returned when a caller has exhausted their
// per-account username lookup quota for the current window.
var ErrUsernameLookupQuotaExceeded = errors.New("username lookup quota exceeded")

// UsernameLookupWindow is the rolling window for the per-account username
// lookup quota.
const UsernameLookupWindow = 24 * time.Hour

// UsernameLookupLimit is the maximum number of distinct usernames a single
// account may look up within UsernameLookupWindow.
const UsernameLookupLimit = 100

// UsernameLookupBurstWindow is the short window for the per-minute burst cap.
const UsernameLookupBurstWindow = 1 * time.Minute

// UsernameLookupBurstLimit is the maximum number of distinct username lookups
// within the burst window.
const UsernameLookupBurstLimit = 20

// CheckAndChargeUsernameLookup checks whether callerID has room in their
// username lookup quota for the given handle, and records the attempt. It
// returns nil when the lookup is allowed, ErrUsernameLookupQuotaExceeded when
// the caller has exhausted their quota, and a wrapped error on storage failure.
//
// Two independent counters are enforced: a 24-hour rolling window (100 distinct
// handles) and a per-minute burst cap (20 distinct handles). Both are checked
// before the lookup executes so the quota is charged identically on hit and miss.
//
// The check is atomic: expired rows are pruned, a new row is inserted, and the
// distinct handle count is verified against both limits. COUNT DISTINCT handles
// dedup so retries of the same handle do not double-charge.
func (s *Store) CheckAndChargeUsernameLookup(ctx context.Context, callerID int64, handle string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	// Serialize concurrent lookups for the same caller so the prune/count/insert
	// sequence cannot race with itself.
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock($1)", callerID); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}

	qtx := s.q.WithTx(tx)
	windowCutoff := pgtype.Timestamptz{Time: time.Now().Add(-UsernameLookupWindow), Valid: true}
	burstCutoff := pgtype.Timestamptz{Time: time.Now().Add(-UsernameLookupBurstWindow), Valid: true}

	// Prune expired rows for this caller only.
	if err := qtx.DeleteExpiredUsernameLookups(ctx, db.DeleteExpiredUsernameLookupsParams{
		CallerID:   callerID,
		LookedUpAt: windowCutoff,
	}); err != nil {
		return fmt.Errorf("prune lookups: %w", err)
	}

	// Insert the attempt. COUNT DISTINCT handles dedup for retries.
	if err := qtx.InsertUsernameLookup(ctx, db.InsertUsernameLookupParams{
		CallerID: callerID,
		Handle:   handle,
	}); err != nil {
		return fmt.Errorf("insert lookup: %w", err)
	}

	// Check 24-hour rolling window.
	count, err := qtx.CountUsernameLookups(ctx, db.CountUsernameLookupsParams{
		CallerID:   callerID,
		LookedUpAt: windowCutoff,
	})
	if err != nil {
		return fmt.Errorf("count window lookups: %w", err)
	}
	if count > UsernameLookupLimit {
		return ErrUsernameLookupQuotaExceeded
	}

	// Check per-minute burst cap independently.
	burstCount, err := qtx.CountUsernameLookups(ctx, db.CountUsernameLookupsParams{
		CallerID:   callerID,
		LookedUpAt: burstCutoff,
	})
	if err != nil {
		return fmt.Errorf("count burst lookups: %w", err)
	}
	if burstCount > UsernameLookupBurstLimit {
		return ErrUsernameLookupQuotaExceeded
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// UsernameByHandle looks up a normalized handle in the usernames table and
// returns the resolved entity. It returns the user or channel along with
// whether it was found. A handle with no row returns ok=false.
//
// This is a single query against the usernames table — not found → false,
// found → load the entity. There is no extra query path for "found in usernames
// but entity has no username" because the usernames table is the source of
// truth: if the row exists, the entity's username is current. If the entity
// cleared its username, the usernames row is deleted in the same transaction
// (see UpdateUsername), so a stale row cannot exist.
func (s *Store) UsernameByHandle(ctx context.Context, handle string) (UsernameResolution, bool, error) {
	row, err := s.q.GetUsernameByHandle(ctx, handle)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return UsernameResolution{}, false, nil
	case err != nil:
		return UsernameResolution{}, false, fmt.Errorf("username lookup: %w", err)
	}

	switch row.OwnerType {
	case "user":
		u, err := s.q.GetUserByUsername(ctx, handle)
		if err != nil {
			return UsernameResolution{}, false, fmt.Errorf("load user: %w", err)
		}
		return UsernameResolution{Kind: UsernameKindUser, User: UserFromDB(u)}, true, nil
	case "channel":
		c, err := s.q.GetChannelByUsername(ctx, handle)
		if err != nil {
			return UsernameResolution{}, false, fmt.Errorf("load channel: %w", err)
		}
		return UsernameResolution{Kind: UsernameKindChannel, Channel: channelFromRow(c)}, true, nil
	default:
		return UsernameResolution{}, false, fmt.Errorf("unknown owner type: %s", row.OwnerType)
	}
}

// UsernameKind discriminates the type of entity a username resolves to.
type UsernameKind int

const (
	UsernameKindUser UsernameKind = iota
	UsernameKindChannel
)

// UsernameResolution carries the result of a username lookup.
type UsernameResolution struct {
	Kind    UsernameKind
	User    User
	Channel Channel
}
