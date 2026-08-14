package store

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// bootstrapUsernameRe validates a normalized (lowercase) username: 5–32 chars,
// ASCII letters/digits/underscore, first char must be a letter.
var bootstrapUsernameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{4,31}$`)

// ErrBootstrapSquatted is returned when the bootstrap username already exists
// but does not match the expected credential (wrong owner type, wrong login
// mode, or verifier mismatch).
var ErrBootstrapSquatted = errors.New("bootstrap username squatted")

// ErrBootstrapReserved is returned when the bootstrap username is on the
// reserved blocklist.
var ErrBootstrapReserved = errors.New("bootstrap username is reserved")

// ErrBootstrapInvalid is returned when the bootstrap username fails format
// validation.
var ErrBootstrapInvalid = errors.New("bootstrap username is invalid")

// BootstrapParams holds the inputs for creating a bootstrap account.
type BootstrapParams struct {
	// Handle is the normalized (lowercase) username.
	Handle string
	// FirstName is the display first name.
	FirstName string
	// LastName is the display last name.
	LastName string
	// Salt1 is the first KDF salt (32 bytes).
	Salt1 []byte
	// Salt2 is the second KDF salt (32 bytes).
	Salt2 []byte
	// Verifier is the SRP v value (256 bytes, padded).
	Verifier []byte
}

// BootstrapResult holds the outcome of a bootstrap operation.
type BootstrapResult struct {
	UserID int64
	// Created is true when a new account was inserted; false when the existing
	// account already matched (idempotent no-op).
	Created bool
}

// BootstrapAccount creates the first username-mode operator account, or verifies
// that an existing one matches the bootstrap credential.
//
// It performs three writes in a single transaction:
//  1. INSERT users (username-mode, no phone)
//  2. INSERT usernames (claim the handle)
//  3. INSERT user_passwords (SRP verifier)
//
// If the username already exists, it checks for an idempotent match:
//   - owner_type='user' AND login_mode='username' AND verifier matches → no-op (Created=false)
//   - any other mismatch → ErrBootstrapSquatted
//
// Returns BootstrapResult with the user ID and whether a new account was created.
func (s *Store) BootstrapAccount(ctx context.Context, p BootstrapParams) (BootstrapResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Check if the username already exists.
	var existingUsername db.Username
	err = tx.QueryRow(ctx, `
		SELECT handle, owner_type, owner_id
		FROM usernames
		WHERE handle = $1
	`, p.Handle).Scan(&existingUsername.Handle, &existingUsername.OwnerType, &existingUsername.OwnerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// Username is free — proceed with creation.
	case err != nil:
		return BootstrapResult{}, fmt.Errorf("check existing username: %w", err)
	default:
		// Username exists — check for idempotent match.
		return s.bootstrapCheckExisting(ctx, qtx, p, existingUsername)
	}

	// Create the user row.
	u, err := qtx.CreateUsernameUser(ctx, db.CreateUsernameUserParams{
		FirstName: p.FirstName,
		LastName:  p.LastName,
	})
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("create user: %w", err)
	}

	// Claim the username.
	_, err = qtx.ClaimUsername(ctx, db.ClaimUsernameParams{
		Handle:    p.Handle,
		OwnerType: "user",
		OwnerID:   u.ID,
	})
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "23505" {
			// Race: another process claimed it between our check and this insert.
			return BootstrapResult{}, ErrBootstrapSquatted
		}
		return BootstrapResult{}, fmt.Errorf("claim username: %w", err)
	}

	// Update the denormalized username column.
	if _, err := qtx.SetUsername(ctx, db.SetUsernameParams{ID: u.ID, Username: &p.Handle}); err != nil {
		return BootstrapResult{}, fmt.Errorf("set username: %w", err)
	}

	// Provision update_state so the account is usable.
	if err := qtx.EnsureUpdateState(ctx, u.ID); err != nil {
		return BootstrapResult{}, fmt.Errorf("ensure update state: %w", err)
	}

	// Store the verifier (encrypted).
	enc, err := s.cipher.Seal(p.Verifier)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("seal verifier: %w", err)
	}
	err = qtx.UpsertPassword(ctx, db.UpsertPasswordParams{
		UserID:   u.ID,
		Salt1:    p.Salt1,
		Salt2:    p.Salt2,
		Verifier: enc,
	})
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("upsert password: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return BootstrapResult{}, fmt.Errorf("commit: %w", err)
	}
	return BootstrapResult{UserID: u.ID, Created: true}, nil
}

// bootstrapCheckExisting verifies that an existing username row matches the
// bootstrap credential. Returns idempotent no-op on match, error on mismatch.
func (s *Store) bootstrapCheckExisting(ctx context.Context, qtx *db.Queries, p BootstrapParams, existing db.Username) (BootstrapResult, error) {
	// Must be owned by a user, not a channel.
	if existing.OwnerType != "user" {
		return BootstrapResult{}, fmt.Errorf("%w: handle %q is owned by a %s", ErrBootstrapSquatted, p.Handle, existing.OwnerType)
	}

	userID := existing.OwnerID

	// Check login_mode.
	loginMode, err := qtx.GetUserLoginMode(ctx, userID)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("get login mode: %w", err)
	}
	if loginMode != "username" {
		return BootstrapResult{}, fmt.Errorf("%w: user %d has login_mode=%q, expected username", ErrBootstrapSquatted, userID, loginMode)
	}

	// Check verifier match.
	pw, err := qtx.PasswordByUser(ctx, userID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return BootstrapResult{}, fmt.Errorf("%w: user %d has no verifier", ErrBootstrapSquatted, userID)
	case err != nil:
		return BootstrapResult{}, fmt.Errorf("load password: %w", err)
	}

	// Decrypt the stored verifier.
	storedVerifier, err := s.cipher.Open(pw.Verifier)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("decrypt stored verifier: %w", err)
	}

	// Compare verifiers.
	if !bytesEqual(storedVerifier, p.Verifier) {
		return BootstrapResult{}, fmt.Errorf("%w: verifier mismatch for user %d", ErrBootstrapSquatted, userID)
	}

	// Idempotent match — no-op.
	return BootstrapResult{UserID: userID, Created: false}, nil
}

// bytesEqual is a constant-time comparison for verifier matching.
func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// ValidateBootstrapUsername checks the bootstrap username against the same
// validation rules applied to any username claim: format and reserved blocklist.
// The username must already be lowercase-normalized.
func ValidateBootstrapUsername(handle string) error {
	if !bootstrapUsernameRe.MatchString(handle) {
		return ErrBootstrapInvalid
	}
	if reservedUsernames[handle] {
		return ErrBootstrapReserved
	}
	return nil
}

// reservedUsernames is the blocklist of handles that must never be claimed.
var reservedUsernames = map[string]bool{
	"admin":    true,
	"support":  true,
	"help":     true,
	"me":       true,
	"settings": true,
	"telegram": true,
	"channel":  true,
	"channels": true,
	"bot":      true,
	"bots":     true,
	"login":    true,
	"signup":   true,
}
