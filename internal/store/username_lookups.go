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

	// Capture wall-clock time AFTER the advisory lock. PostgreSQL's now() is
	// pinned to transaction-start time, so a request stalled behind a lock
	// holder for more than a minute would insert a row outside the burst window
	// and commit uncounted.
	now := time.Now()

	qtx := s.q.WithTx(tx)
	windowCutoff := pgtype.Timestamptz{Time: now.Add(-UsernameLookupWindow), Valid: true}
	burstCutoff := pgtype.Timestamptz{Time: now.Add(-UsernameLookupBurstWindow), Valid: true}

	// Prune expired rows for this caller only.
	if err := qtx.DeleteExpiredUsernameLookups(ctx, db.DeleteExpiredUsernameLookupsParams{
		CallerID:   callerID,
		LookedUpAt: windowCutoff,
	}); err != nil {
		return fmt.Errorf("prune lookups: %w", err)
	}

	// Insert the attempt with the post-lock timestamp. COUNT DISTINCT handles
	// dedup for retries.
	if err := qtx.InsertUsernameLookup(ctx, db.InsertUsernameLookupParams{
		CallerID:   callerID,
		Handle:     handle,
		LookedUpAt: pgtype.Timestamptz{Time: now, Valid: true},
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
		// Username comes off the joined usernames row — the handle that admitted
		// this account to the result — for the same reason the channel arm below
		// takes it off the row: the denormalized users.username copy is not
		// authoritative, and a writer that released the handle without clearing
		// the copy must not make two RPCs name the same account differently.
		return UsernameResolution{Kind: UsernameKindUser, User: UserFromDB(db.UserByIDRow{
			ID:         u.ID,
			Phone:      u.Phone,
			FirstName:  u.FirstName,
			LastName:   u.LastName,
			CreatedAt:  u.CreatedAt,
			IsOnline:   u.IsOnline,
			LastSeenAt: u.LastSeenAt,
			Username:   &u.Username,
		})}, true, nil
	case "channel":
		c, err := s.q.GetChannelByUsername(ctx, handle)
		if err != nil {
			return UsernameResolution{}, false, fmt.Errorf("load channel: %w", err)
		}
		ch := channelFromRow(c)
		// The handle that resolved the channel, off the usernames row, replaces
		// the denormalized channels.username copy the row carries. The two are
		// written in one transaction today, but only this row is authoritative,
		// and contacts.search already reports the handle from it — a writer that
		// released the handle without clearing the copy must not make the two
		// RPCs name the same channel differently.
		ch.Username = &row.Handle
		return UsernameResolution{Kind: UsernameKindChannel, Channel: ch}, true, nil
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
