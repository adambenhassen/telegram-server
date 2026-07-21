package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// User is a persisted account.
type User struct {
	ID        int64
	Phone     string
	FirstName string
	LastName  string
}

// CreateUser inserts a user for phone, or returns the existing row.
func (s *Store) CreateUser(ctx context.Context, phone string) (User, error) {
	u, err := s.q.CreateUser(ctx, phone)
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return User{ID: u.ID, Phone: u.Phone, FirstName: u.FirstName, LastName: u.LastName}, nil
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
