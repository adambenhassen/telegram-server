package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrInvalidBlock is returned when a block operation names no user or the
// caller themselves. The API validates this at its peer boundary as well; the
// store keeps the invariant true for every caller.
var ErrInvalidBlock = errors.New("invalid block target")

// BlockedUser is one entry in an account's directed block list.
type BlockedUser struct {
	UserID int64
	Date   time.Time
}

// BlockedUsersPage is a page of an account's directed block list.
type BlockedUsersPage struct {
	Users []BlockedUser
	Total int
}

func validBlockIDs(blockerID, blockedID int64) bool {
	return blockerID > 0 && blockedID > 0 && blockerID != blockedID
}

// BlockUser adds blockedID to blockerID's directed block list. The sorted
// owner locks are the same locks membership adds take, so a block committed
// before an add cannot be missed by the add's check.
func (s *Store) BlockUser(ctx context.Context, blockerID, blockedID int64) (changed bool, err error) {
	if !validBlockIDs(blockerID, blockedID) {
		return false, ErrInvalidBlock
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if err := lockOwners(ctx, tx, blockerID, blockedID); err != nil {
		return false, err
	}
	qtx := s.q.WithTx(tx)
	n, err := qtx.InsertBlockedUser(ctx, db.InsertBlockedUserParams{
		BlockerID: blockerID,
		BlockedID: blockedID,
	})
	if err != nil {
		return false, fmt.Errorf("insert blocked user: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return n > 0, nil
}

// UnblockUser removes blockedID from blockerID's directed block list.
// Removing an absent row is a successful no-op.
func (s *Store) UnblockUser(ctx context.Context, blockerID, blockedID int64) (changed bool, err error) {
	if !validBlockIDs(blockerID, blockedID) {
		return false, ErrInvalidBlock
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	if err := lockOwners(ctx, tx, blockerID, blockedID); err != nil {
		return false, err
	}
	qtx := s.q.WithTx(tx)
	n, err := qtx.DeleteBlockedUser(ctx, db.DeleteBlockedUserParams{
		BlockerID: blockerID,
		BlockedID: blockedID,
	})
	if err != nil {
		return false, fmt.Errorf("delete blocked user: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return n > 0, nil
}

// IsBlocked reports whether blockerID has blocked blockedID for callers that
// need a direct store read. Membership enforcement uses the transaction-scoped
// db.Queries.IsBlocked query directly.
func (s *Store) IsBlocked(ctx context.Context, blockerID, blockedID int64) (bool, error) {
	blocked, err := s.q.IsBlocked(ctx, db.IsBlockedParams{
		BlockerID: blockerID,
		BlockedID: blockedID,
	})
	if err != nil {
		return false, fmt.Errorf("is blocked: %w", err)
	}
	return blocked, nil
}

// BlockedUsers returns one deterministic page of the blocker's directed block
// list. Newest blocks sort first; blocked_id breaks ties from the same clock
// value. Total is the count before pagination.
func (s *Store) BlockedUsers(ctx context.Context, blockerID int64, offset, limit int) (BlockedUsersPage, error) {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.q.BlockedUsers(ctx, db.BlockedUsersParams{
		BlockerID: blockerID,
		Lim:       int32(limit),  // API bounds this page to a small value.
		Off:       int32(offset), // API rejects negative offsets and bounds pages.
	})
	if err != nil {
		return BlockedUsersPage{}, fmt.Errorf("blocked users: %w", err)
	}
	page := BlockedUsersPage{Users: make([]BlockedUser, len(rows))}
	if len(rows) == 0 {
		total, err := s.q.CountBlockedUsers(ctx, blockerID)
		if err != nil {
			return BlockedUsersPage{}, fmt.Errorf("count blocked users: %w", err)
		}
		page.Total = int(total) // a table row count fits in int on supported hosts.
		return page, nil
	}
	for i, row := range rows {
		page.Users[i] = BlockedUser{UserID: row.BlockedID, Date: row.CreatedAt.Time}
		page.Total = int(row.Total) // a table row count fits in int on supported hosts.
	}
	return page, nil
}
