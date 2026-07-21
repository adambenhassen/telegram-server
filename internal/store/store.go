// Package store persists users and login codes in Postgres.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Store is the Postgres-backed persistence layer.
type Store struct {
	pool *pgxpool.Pool
	q    *db.Queries
}

// Sentinel errors returned by the login-code methods.
var (
	ErrCodeInvalid = errors.New("phone code invalid")
	ErrCodeExpired = errors.New("phone code expired")
	// ErrCodeExhausted is returned by VerifyCode when the per-code attempt cap
	// has been reached; the code can no longer be verified.
	ErrCodeExhausted = errors.New("phone code exhausted")
	// ErrResendTooSoon is returned by IssueCode when an active code was issued
	// within the resend cooldown window.
	ErrResendTooSoon = errors.New("phone code resend too soon")
	// ErrAuthKeyNotFound is returned when an auth-key operation matches no row.
	ErrAuthKeyNotFound = errors.New("auth key not found")
)

// Open connects to Postgres. The schema is owned by the Atlas migrations, so
// Open assumes the target database is already migrated and only opens the pool.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	return &Store{pool: pool, q: db.New(pool)}, nil
}

// Close releases the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}
