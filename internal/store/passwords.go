package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// UserPassword is a persisted 2FA cloud password. Verifier holds the decrypted
// SRP v; it is sealed at rest with the store's master key exactly like auth key
// values. RecoveryEmail is passthrough only (no verification is performed).
type UserPassword struct {
	UserID        int64
	Salt1         []byte
	Salt2         []byte
	Verifier      []byte
	Hint          string
	RecoveryEmail string
	HasRecovery   bool
}

// PasswordByUser returns the 2FA password row for userID, ok=false when the user
// has no cloud password. The verifier is decrypted; a decrypt failure (wrong
// master key or tampered row) is returned, never a silent bypass.
func (s *Store) PasswordByUser(ctx context.Context, userID int64) (UserPassword, bool, error) {
	row, err := s.q.PasswordByUser(ctx, userID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return UserPassword{}, false, nil
	case err != nil:
		return UserPassword{}, false, fmt.Errorf("password by user: %w", err)
	}
	verifier, err := s.cipher.Open(row.Verifier)
	if err != nil {
		return UserPassword{}, false, fmt.Errorf("decrypt verifier for user %d: %w", userID, err)
	}
	var email string
	if row.RecoveryEmail != nil {
		email = *row.RecoveryEmail
	}
	return UserPassword{
		UserID:        row.UserID,
		Salt1:         row.Salt1,
		Salt2:         row.Salt2,
		Verifier:      verifier,
		Hint:          row.Hint,
		RecoveryEmail: email,
		HasRecovery:   row.HasRecovery,
	}, true, nil
}

// UpsertPassword inserts or replaces the 2FA password for p.UserID. The verifier
// is encrypted before storage. Used for both initial set and change.
func (s *Store) UpsertPassword(ctx context.Context, p UserPassword) error {
	enc, err := s.cipher.Seal(p.Verifier)
	if err != nil {
		return fmt.Errorf("upsert password: %w", err)
	}
	var email *string
	if p.HasRecovery || p.RecoveryEmail != "" {
		e := p.RecoveryEmail
		email = &e
	}
	err = s.q.UpsertPassword(ctx, db.UpsertPasswordParams{
		UserID:        p.UserID,
		Salt1:         p.Salt1,
		Salt2:         p.Salt2,
		Verifier:      enc,
		Hint:          p.Hint,
		RecoveryEmail: email,
		HasRecovery:   p.HasRecovery,
	})
	if err != nil {
		return fmt.Errorf("upsert password: %w", err)
	}
	return nil
}

// DeletePassword removes the 2FA password for userID (the remove-password path).
// found is false when the user had no cloud password.
func (s *Store) DeletePassword(ctx context.Context, userID int64) (bool, error) {
	rows, err := s.q.DeletePassword(ctx, userID)
	if err != nil {
		return false, fmt.Errorf("delete password: %w", err)
	}
	return rows > 0, nil
}
