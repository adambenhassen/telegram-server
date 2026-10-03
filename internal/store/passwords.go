package store

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/store/db"
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

var (
	ErrPasswordResetUsernameNotFound = errors.New("username account not found")
	ErrPasswordResetChannelOwned     = errors.New("channel-owned handle cannot be reset")
	ErrPasswordResetPhoneMode        = errors.New("phone-mode account cannot be reset")
	ErrPasswordResetMissingPassword  = errors.New("username account has no password row")
)

// UsernamePasswordReset identifies the username account whose password was
// replaced. Handle is the canonical handle from the usernames table.
type UsernamePasswordReset struct {
	Handle string
	UserID int64
}

// ResetUsernamePassword atomically replaces an existing username-mode account's
// SRP verifier. It verifies the current verifier with the configured master key
// before writing so a misconfigured key cannot strand the account.
func (s *Store) ResetUsernamePassword(ctx context.Context, handle string, salt1, salt2, verifier []byte) (UsernamePasswordReset, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return UsernamePasswordReset{}, fmt.Errorf("begin password reset: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	var result UsernamePasswordReset
	var ownerType string
	if err := tx.QueryRow(ctx, `
		SELECT handle, owner_type, owner_id
		FROM usernames
		WHERE handle = $1
		FOR UPDATE`, handle).Scan(&result.Handle, &ownerType, &result.UserID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return UsernamePasswordReset{}, ErrPasswordResetUsernameNotFound
		}
		return UsernamePasswordReset{}, fmt.Errorf("resolve username for password reset: %w", err)
	}
	if ownerType == "channel" {
		return UsernamePasswordReset{}, ErrPasswordResetChannelOwned
	}
	if ownerType != "user" {
		return UsernamePasswordReset{}, fmt.Errorf("resolve username for password reset: unsupported owner type %q", ownerType)
	}

	var loginMode string
	if err := tx.QueryRow(ctx, `SELECT login_mode FROM users WHERE id = $1 FOR SHARE`, result.UserID).Scan(&loginMode); err != nil {
		return UsernamePasswordReset{}, fmt.Errorf("load username account for password reset: %w", err)
	}
	if loginMode != "username" {
		return UsernamePasswordReset{}, ErrPasswordResetPhoneMode
	}

	var encryptedVerifier []byte
	if err := tx.QueryRow(ctx, `
		SELECT verifier
		FROM user_passwords
		WHERE user_id = $1
		FOR UPDATE`, result.UserID).Scan(&encryptedVerifier); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return UsernamePasswordReset{}, ErrPasswordResetMissingPassword
		}
		return UsernamePasswordReset{}, fmt.Errorf("lock existing password for reset: %w", err)
	}
	existingVerifier, err := s.cipher.Open(encryptedVerifier)
	if err != nil {
		return UsernamePasswordReset{}, fmt.Errorf("decrypt existing password verifier: %w", err)
	}
	clear(existingVerifier)

	sealedVerifier, err := s.cipher.Seal(verifier)
	if err != nil {
		return UsernamePasswordReset{}, fmt.Errorf("encrypt replacement password verifier: %w", err)
	}
	tag, err := tx.Exec(ctx, `
		UPDATE user_passwords
		SET salt1 = $2, salt2 = $3, verifier = $4, hint = '', updated_at = now()
		WHERE user_id = $1`, result.UserID, salt1, salt2, sealedVerifier)
	if err != nil {
		return UsernamePasswordReset{}, fmt.Errorf("write replacement password verifier: %w", err)
	}
	if tag.RowsAffected() != 1 {
		return UsernamePasswordReset{}, fmt.Errorf("write replacement password verifier: updated %d rows, want 1", tag.RowsAffected())
	}
	if err := tx.Commit(ctx); err != nil {
		return UsernamePasswordReset{}, fmt.Errorf("commit password reset: %w", err)
	}
	return result, nil
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
