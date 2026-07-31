package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// User is a persisted account.
type User struct {
	ID         int64
	Phone      string
	FirstName  string
	LastName   string
	IsOnline   bool
	LastSeenAt *time.Time
}

// NormalizePhone strips an optional leading '+' so that '+1555...' and
// '1555...' resolve to the same value. Used on both read and write paths.
func NormalizePhone(phone string) string {
	return strings.TrimPrefix(phone, "+")
}

// CreateUser inserts a user for phone, or returns the existing row. It also
// provisions the account's update_state row in the same transaction so the
// update APIs and the two-sided send lock ordering never race a missing row.
func (s *Store) CreateUser(ctx context.Context, phone string) (User, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return User{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	u, err := qtx.CreateUser(ctx, NormalizePhone(phone))
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	if err := qtx.EnsureUpdateState(ctx, u.ID); err != nil {
		return User{}, fmt.Errorf("ensure update state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return User{}, fmt.Errorf("commit: %w", err)
	}
	return UserFromDB(u), nil
}

// UserByID returns the user for id, ok=false when absent.
func (s *Store) UserByID(ctx context.Context, id int64) (User, bool, error) {
	u, err := s.q.UserByID(ctx, id)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, false, nil
	case err != nil:
		return User{}, false, fmt.Errorf("user by id: %w", err)
	}
	return UserFromDB(u), true, nil
}

// UserByPhone returns the user for phone, ok=false when absent.
func (s *Store) UserByPhone(ctx context.Context, phone string) (User, bool, error) {
	u, err := s.q.UserByPhone(ctx, NormalizePhone(phone))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, false, nil
	case err != nil:
		return User{}, false, fmt.Errorf("user by phone: %w", err)
	}
	return UserFromDB(u), true, nil
}

// UserFromDB converts a sqlc-generated User model to the store's User type.
func UserFromDB(u db.User) User {
	var lastSeen *time.Time
	if u.LastSeenAt.Valid {
		t := u.LastSeenAt.Time
		lastSeen = &t
	}
	return User{
		ID:         u.ID,
		Phone:      u.Phone,
		FirstName:  u.FirstName,
		LastName:   u.LastName,
		IsOnline:   u.IsOnline,
		LastSeenAt: lastSeen,
	}
}

// SetUserStatus updates a user's online status and last-seen timestamp in one
// write. When online is true the user is marked online; when false the user is
// marked offline. last_seen_at is set to NOW() in both cases. Returns an error
// when the user does not exist.
func (s *Store) SetUserStatus(ctx context.Context, userID int64, online bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Verify user exists before writing.
	if _, err := qtx.UserByID(ctx, userID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("set user status: user %d not found", userID)
		}
		return fmt.Errorf("set user status: %w", err)
	}

	if err := qtx.SetUserStatus(ctx, db.SetUserStatusParams{ID: userID, IsOnline: online}); err != nil {
		return fmt.Errorf("set user status: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

// DialogPartners returns the distinct set of user IDs that share a non-deleted
// 1:1 dialog with userID. Used as the fan-out target set for status changes.
func (s *Store) DialogPartners(ctx context.Context, userID int64) ([]int64, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT DISTINCT CASE d.owner_id WHEN $1 THEN d.peer_id ELSE d.owner_id END AS partner_id
		FROM dialogs d
		WHERE d.peer_type = 1
		  AND (d.owner_id = $1 OR d.peer_id = $1)
	`, userID)
	if err != nil {
		return nil, fmt.Errorf("dialog partners: %w", err)
	}
	defer rows.Close()

	var partners []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("dialog partners scan: %w", err)
		}
		partners = append(partners, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dialog partners: %w", err)
	}
	return partners, nil
}
