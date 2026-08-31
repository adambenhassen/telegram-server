package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

const (
	// DefaultInviteLifetime is the lifetime used when IssueInvite receives no
	// override, or an explicit zero override.
	DefaultInviteLifetime = 7 * 24 * time.Hour
	// MaxInviteLifetime bounds how long an issued authorization can remain
	// usable. There is deliberately no unbounded-lifetime option.
	MaxInviteLifetime = 30 * 24 * time.Hour
	inviteSecretBytes = 32
)

// InviteState is the operator-visible lifecycle state of a registration
// invite. An issued row whose database expiry has passed is reported as
// InviteExpired even before a writer retires the row.
type InviteState string

const (
	InviteIssued   InviteState = "issued"
	InviteConsumed InviteState = "consumed"
	InviteRevoked  InviteState = "revoked"
	InviteExpired  InviteState = "expired"
)

// RegistrationInvite is the metadata operators may list. It intentionally has
// no secret or secret digest field: the cleartext is returned only by
// IssueInvite, and the digest is an internal verification value.
type RegistrationInvite struct {
	ID         int64
	Handle     string
	IssuedAt   time.Time
	ExpiresAt  time.Time
	State      InviteState
	ConsumedAt *time.Time
	RevokedAt  *time.Time
}

// Invite is a short alias for callers that do not need to distinguish this
// record from the other invite types in the store.
type Invite = RegistrationInvite

// ErrInviteLive is returned when an unexpired, unrevoked and unconsumed invite
// already exists for the canonical handle.
var ErrInviteLive = errors.New("registration invite already live")

// ErrInviteLifetimeInvalid is returned when the requested lifetime is not
// positive at database timestamp precision or exceeds MaxInviteLifetime.
var ErrInviteLifetimeInvalid = errors.New("registration invite lifetime invalid")

// ErrInviteHandleInvalid is returned when no handle was supplied.
var ErrInviteHandleInvalid = errors.New("registration invite handle invalid")

// ErrInviteTransactionRequired is returned when a caller tries to consume an
// invite without supplying the transaction that must own the state change.
var ErrInviteTransactionRequired = errors.New("registration invite transaction required")

// IssueInvite creates one invite for handle and returns its metadata plus the
// cleartext secret. The secret is generated once with crypto/rand and only its
// SHA-256 digest is persisted. A missing or zero lifetime selects
// DefaultInviteLifetime; one non-zero override is accepted and cannot exceed
// MaxInviteLifetime.
//
// The expired-row transition and the insert are in one transaction. The
// partial unique index on issued rows arbitrates the race where two callers
// try to issue the first live invite for a handle; a unique conflict is the
// same ErrInviteLive refusal as the serialized path.
func (s *Store) IssueInvite(ctx context.Context, handle string, lifetimes ...time.Duration) (RegistrationInvite, string, error) {
	handle = strings.ToLower(handle)
	if handle == "" {
		return RegistrationInvite{}, "", ErrInviteHandleInvalid
	}
	lifetime, err := inviteLifetime(lifetimes)
	if err != nil {
		return RegistrationInvite{}, "", err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return RegistrationInvite{}, "", fmt.Errorf("issue invite: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	if _, err := qtx.ExpireRegistrationInvite(ctx, handle); err != nil {
		return RegistrationInvite{}, "", fmt.Errorf("issue invite: expire old row: %w", err)
	}

	var secretBytes [inviteSecretBytes]byte
	if _, err := io.ReadFull(rand.Reader, secretBytes[:]); err != nil {
		return RegistrationInvite{}, "", fmt.Errorf("issue invite: random secret: %w", err)
	}
	secret := hex.EncodeToString(secretBytes[:])
	digest := sha256.Sum256([]byte(secret))

	row, err := qtx.InsertRegistrationInvite(ctx, db.InsertRegistrationInviteParams{
		Handle:       handle,
		SecretDigest: digest[:],
		LifetimeUs:   lifetime.Microseconds(),
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" && pgErr.ConstraintName == "registration_invites_live_handle_idx" {
			return RegistrationInvite{}, "", ErrInviteLive
		}
		return RegistrationInvite{}, "", fmt.Errorf("issue invite: insert: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return RegistrationInvite{}, "", fmt.Errorf("issue invite: commit: %w", err)
	}
	return registrationInviteFromInsert(row), secret, nil
}

func inviteLifetime(lifetimes []time.Duration) (time.Duration, error) {
	if len(lifetimes) > 1 {
		return 0, ErrInviteLifetimeInvalid
	}
	lifetime := DefaultInviteLifetime
	if len(lifetimes) == 1 && lifetimes[0] != 0 {
		lifetime = lifetimes[0]
	}
	if lifetime < time.Microsecond || lifetime > MaxInviteLifetime {
		return 0, ErrInviteLifetimeInvalid
	}
	return lifetime, nil
}

// VerifyInvite checks a secret against a live invite. All semantic failures
// return ErrInviteInvalid, including an absent handle and a terminal or
// expired row. The lookup and decision run in a transaction with the row lock,
// and expiry is evaluated using PostgreSQL's clock_timestamp.
func (s *Store) VerifyInvite(ctx context.Context, handle, secret string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("verify invite: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // read-only transaction
	return s.verifyInviteTx(ctx, tx, handle, secret)
}

// verifyInviteTx performs the locked read for the standalone verifier. It
// leaves the transaction open until VerifyInvite returns so the row lock is
// released without committing a read-only transaction.
func (s *Store) verifyInviteTx(ctx context.Context, tx pgx.Tx, handle, secret string) error {
	row, err := s.liveInviteForUpdate(ctx, tx, handle)
	if errors.Is(err, pgx.ErrNoRows) {
		// Compare against a fixed-size zero digest so the missing-row path still
		// performs the same constant-time digest comparison operation.
		var zero [sha256.Size]byte
		_ = inviteSecretMatches(secret, zero[:])
		return ErrInviteInvalid
	}
	if err != nil {
		return fmt.Errorf("verify invite: load: %w", err)
	}
	live, err := s.q.WithTx(tx).RegistrationInviteIsLive(ctx, row.ID)
	if err != nil {
		return fmt.Errorf("verify invite: check expiry: %w", err)
	}
	if !live {
		var zero [sha256.Size]byte
		_ = inviteSecretMatches(secret, zero[:])
		return ErrInviteInvalid
	}
	if !inviteSecretMatches(secret, row.SecretDigest) {
		return ErrInviteInvalid
	}
	return nil
}

func (s *Store) liveInviteForUpdate(ctx context.Context, tx pgx.Tx, handle string) (db.RegistrationInvite, error) {
	return s.q.WithTx(tx).LiveRegistrationInviteForUpdate(ctx, strings.ToLower(handle))
}

// inviteSecretMatches hashes the presented secret and compares two fixed-size
// SHA-256 values with subtle.ConstantTimeCompare. The length check is done
// after the comparison so a malformed stored value cannot skip the comparison
// operation.
func inviteSecretMatches(secret string, stored []byte) bool {
	var storedDigest [sha256.Size]byte
	copy(storedDigest[:], stored)
	presented := sha256.Sum256([]byte(secret))
	compared := subtle.ConstantTimeCompare(presented[:], storedDigest[:])
	return len(stored) == sha256.Size && compared == 1
}

// ConsumeInvite verifies and marks the live invite consumed inside tx. The
// caller owns tx and decides whether the invite and its surrounding account
// work commit together. The row is locked before the constant-time comparison,
// and the guarded update is the final compare-and-swap; a rollback leaves the
// invite live.
func (s *Store) ConsumeInvite(ctx context.Context, tx pgx.Tx, handle, secret string) error {
	if tx == nil {
		return ErrInviteTransactionRequired
	}
	qtx := s.q.WithTx(tx)
	row, err := s.liveInviteForUpdate(ctx, tx, handle)
	if errors.Is(err, pgx.ErrNoRows) {
		var zero [sha256.Size]byte
		_ = inviteSecretMatches(secret, zero[:])
		return ErrInviteInvalid
	}
	if err != nil {
		return fmt.Errorf("consume invite: load: %w", err)
	}
	live, err := qtx.RegistrationInviteIsLive(ctx, row.ID)
	if err != nil {
		return fmt.Errorf("consume invite: check expiry: %w", err)
	}
	if !live {
		var zero [sha256.Size]byte
		_ = inviteSecretMatches(secret, zero[:])
		return ErrInviteInvalid
	}
	if !inviteSecretMatches(secret, row.SecretDigest) {
		return ErrInviteInvalid
	}
	changed, err := qtx.ConsumeRegistrationInvite(ctx, row.ID)
	if err != nil {
		return fmt.Errorf("consume invite: compare-and-swap: %w", err)
	}
	if changed != 1 {
		return ErrInviteInvalid
	}
	return nil
}

// RevokeInvite marks a live invite revoked. It is idempotent for an absent,
// expired, already-consumed, or already-revoked id. The UPDATE's row lock is
// the same decision boundary as ConsumeInvite: whichever operation acquires
// that lock first wins, and the loser observes the terminal state.
func (s *Store) RevokeInvite(ctx context.Context, id int64) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("revoke invite: begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	if _, err := qtx.LockRegistrationInvite(ctx, id); errors.Is(err, pgx.ErrNoRows) {
		return nil
	} else if err != nil {
		return fmt.Errorf("revoke invite: lock: %w", err)
	}
	if _, err := qtx.RevokeRegistrationInvite(ctx, id); err != nil {
		return fmt.Errorf("revoke invite: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("revoke invite: commit: %w", err)
	}
	return nil
}

// ListInvites returns invite metadata in issuance order. The query projects a
// naturally expired issued row as expired using database time, and selects no
// secret or digest column.
func (s *Store) ListInvites(ctx context.Context) ([]RegistrationInvite, error) {
	rows, err := s.q.ListRegistrationInvites(ctx)
	if err != nil {
		return nil, fmt.Errorf("list invites: %w", err)
	}
	out := make([]RegistrationInvite, len(rows))
	for i, row := range rows {
		out[i] = RegistrationInvite{
			ID:         row.ID,
			Handle:     row.Handle,
			IssuedAt:   row.IssuedAt.Time,
			ExpiresAt:  row.ExpiresAt.Time,
			State:      InviteState(row.State),
			ConsumedAt: inviteTimestampPtr(row.ConsumedAt),
			RevokedAt:  inviteTimestampPtr(row.RevokedAt),
		}
	}
	return out, nil
}

func registrationInviteFromInsert(row db.InsertRegistrationInviteRow) RegistrationInvite {
	return RegistrationInvite{
		ID:         row.ID,
		Handle:     row.Handle,
		IssuedAt:   row.IssuedAt.Time,
		ExpiresAt:  row.ExpiresAt.Time,
		State:      InviteState(row.State),
		ConsumedAt: inviteTimestampPtr(row.ConsumedAt),
		RevokedAt:  inviteTimestampPtr(row.RevokedAt),
	}
}

func inviteTimestampPtr(value pgtype.Timestamptz) *time.Time {
	if !value.Valid {
		return nil
	}
	result := value.Time
	return &result
}
