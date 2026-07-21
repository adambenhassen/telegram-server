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

// Sentinel errors returned by VerifyCode.
var (
	ErrCodeInvalid = errors.New("phone code invalid")
	ErrCodeExpired = errors.New("phone code expired")
)

// Open connects to Postgres. The schema is owned by the Atlas migrations, so
// Open assumes the target database is already migrated and only opens the pool.
func Open(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &Store{pool: pool, q: db.New(pool)}, nil
}

// Close releases the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}
