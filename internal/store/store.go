// Package store persists users and login codes in Postgres.
package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/keycrypt"
	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// Store is the Postgres-backed persistence layer.
type Store struct {
	pool   *pgxpool.Pool
	q      *db.Queries
	cipher *keycrypt.Cipher
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

// Open connects to Postgres and verifies the schema is migrated. encKey is the
// 32-byte master key used to encrypt auth keys at rest. The schema is owned by
// the Atlas migrations; Open does not apply them, but fails fast if the target
// database has not been migrated.
func Open(ctx context.Context, dsn string, encKey []byte) (*Store, error) {
	cipher, err := keycrypt.New(encKey)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	s := &Store{pool: pool, q: db.New(pool), cipher: cipher}
	if err := s.checkSchema(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

// checkSchema fails fast when the database has not been migrated to the current
// version, turning a confusing runtime "column does not exist" into a clear
// startup error.
//
// ponytail: presence-check of the newest migration's artifacts, not a version
// table — pgtest applies raw SQL and has no Atlas revisions table to read. The
// sentinels track the latest migration (message_events table +
// update_state.next_local_id column); update them when a migration adds new schema.
func (s *Store) checkSchema(ctx context.Context) error {
	var hasEvents, hasNextLocalID bool
	err := s.pool.QueryRow(ctx, `
		SELECT to_regclass('public.message_events') IS NOT NULL,
		       EXISTS(SELECT 1 FROM information_schema.columns
		              WHERE table_name = 'update_state' AND column_name = 'next_local_id')`,
	).Scan(&hasEvents, &hasNextLocalID)
	if err != nil {
		return fmt.Errorf("schema check: %w", err)
	}
	if !hasEvents || !hasNextLocalID {
		return errors.New("database schema is not migrated; run: atlas migrate apply --env local")
	}
	return nil
}

// Close releases the connection pool.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}
