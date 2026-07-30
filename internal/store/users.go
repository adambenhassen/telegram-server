package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// User is a persisted account.
type User struct {
	ID        int64
	Phone     string
	FirstName string
	LastName  string
}

// NormalizePhone strips an optional leading '+' so that '+1555...' and
// '1555...' resolve to the same value. Used on the lookup read path only;
// write-path normalization requires a separate backfill migration.
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

	u, err := qtx.CreateUser(ctx, phone)
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	if err := qtx.EnsureUpdateState(ctx, u.ID); err != nil {
		return User{}, fmt.Errorf("ensure update state: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return User{}, fmt.Errorf("commit: %w", err)
	}
	return User{ID: u.ID, Phone: u.Phone, FirstName: u.FirstName, LastName: u.LastName}, nil
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
	return User{ID: u.ID, Phone: u.Phone, FirstName: u.FirstName, LastName: u.LastName}, true, nil
}

// UserByPhone returns the user for phone, ok=false when absent.
func (s *Store) UserByPhone(ctx context.Context, phone string) (User, bool, error) {
	u, err := s.q.UserByPhone(ctx, phone)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, false, nil
	case err != nil:
		return User{}, false, fmt.Errorf("user by phone: %w", err)
	}
	return User{ID: u.ID, Phone: u.Phone, FirstName: u.FirstName, LastName: u.LastName}, true, nil
}
