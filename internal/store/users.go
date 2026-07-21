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
	u := User{Phone: phone}
	err := s.pool.QueryRow(ctx,
		`INSERT INTO users (phone) VALUES ($1)
		 ON CONFLICT (phone) DO UPDATE SET phone = EXCLUDED.phone
		 RETURNING id, phone, first_name, last_name`,
		phone,
	).Scan(&u.ID, &u.Phone, &u.FirstName, &u.LastName)
	if err != nil {
		return User{}, fmt.Errorf("create user: %w", err)
	}
	return u, nil
}

// UserByPhone returns the user for phone, ok=false when absent.
func (s *Store) UserByPhone(ctx context.Context, phone string) (User, bool, error) {
	u := User{}
	err := s.pool.QueryRow(ctx,
		`SELECT id, phone, first_name, last_name FROM users WHERE phone = $1`,
		phone,
	).Scan(&u.ID, &u.Phone, &u.FirstName, &u.LastName)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return User{}, false, nil
	case err != nil:
		return User{}, false, fmt.Errorf("user by phone: %w", err)
	}
	return u, true, nil
}
