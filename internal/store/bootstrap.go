package store

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"regexp"

	"github.com/gotd/td/crypto/srp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	tsrp "github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store/db"
)

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

// bootstrapUsernameRe validates a normalized (lowercase) username: 5–32 chars,
// ASCII letters/digits/underscore, first char must be a letter.
var bootstrapUsernameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{4,31}$`)

// bootstrapReserved is the blocklist of handles that must never be claimed.
// Mirrors the canonical list in internal/api/account.go.
var bootstrapReserved = map[string]bool{
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

// BootstrapParams holds the inputs for creating a bootstrap account.
type BootstrapParams struct {
	// Handle is the normalized (lowercase) username.
	Handle string
	// FirstName is the display first name.
	FirstName string
	// LastName is the display last name.
	LastName string
	// Password is the cleartext password. Used for verifier computation and
	// idempotency checks. Zeroed by the caller after BootstrapAccount returns.
	Password []byte
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
// If the username already exists, it checks for an idempotent match by running
// a real SRP round trip against the stored verifier.
//   - owner_type='user' AND login_mode='username' AND SRP Verify succeeds → no-op (Created=false)
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

	// Generate KDF salts and compute verifier.
	verifier, salt1, salt2, err := computeSRPVerifier(p.Password)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("compute verifier: %w", err)
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
			// Postgres has aborted this transaction — nothing can reuse it.
			// Roll back explicitly, open a fresh transaction, and run the
			// idempotency check on that.
			if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
				return BootstrapResult{}, fmt.Errorf("rollback after race: %w", rbErr)
			}
			return s.bootstrapCheckExistingTx(ctx, p)
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
	enc, err := s.cipher.Seal(verifier)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("seal verifier: %w", err)
	}
	err = qtx.UpsertPassword(ctx, db.UpsertPasswordParams{
		UserID:   u.ID,
		Salt1:    salt1,
		Salt2:    salt2,
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

// bootstrapCheckExistingTx opens a fresh transaction and runs the idempotency
// check. Used when the original transaction was aborted (e.g. after a 23505
// unique violation) and must not be reused.
func (s *Store) bootstrapCheckExistingTx(ctx context.Context, p BootstrapParams) (res BootstrapResult, err error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if rbErr := tx.Rollback(ctx); rbErr != nil && !errors.Is(rbErr, pgx.ErrTxClosed) {
			err = fmt.Errorf("rollback in fresh tx: %w", rbErr)
		}
	}()

	var existing db.Username
	err = tx.QueryRow(ctx, `
		SELECT handle, owner_type, owner_id
		FROM usernames
		WHERE handle = $1
	`, p.Handle).Scan(&existing.Handle, &existing.OwnerType, &existing.OwnerID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return BootstrapResult{}, ErrBootstrapSquatted
	case err != nil:
		return BootstrapResult{}, fmt.Errorf("re-check username after race: %w", err)
	}
	return s.bootstrapCheckExisting(ctx, s.q.WithTx(tx), p, existing)
}

// bootstrapCheckExisting verifies that an existing username row matches the
// bootstrap credential. Returns idempotent no-op on match, error on mismatch.
//
// It reads the stored verifier and salts from the passwords row, then runs a
// real SRP round trip: tsrp.Challenge (server) → srp.Hash (client) → tsrp.Verify
// (server). This exercises the exact code path a real login takes, rather than
// re-implementing the KDF chain.
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

	// Read the stored password row to get salt1/salt2/verifier.
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

	// Run a real SRP round trip to verify the password.
	// Server side: generate challenge from stored verifier.
	bPub, bSecret, err := tsrp.Challenge(storedVerifier)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("srp challenge: %w", err)
	}

	// Client side: compute SRP answer (A, M1) using the bootstrap password
	// and stored salts.
	random := make([]byte, 256)
	if _, err := io.ReadFull(rand.Reader, random); err != nil {
		return BootstrapResult{}, fmt.Errorf("srp random: %w", err)
	}
	srpClient := srp.NewSRP(rand.Reader)
	algo := srp.Input{
		Salt1: pw.Salt1,
		Salt2: pw.Salt2,
		G:     3,
		P:     tsrp.PBytes(),
	}
	answer, err := srpClient.Hash(p.Password, bPub, random, algo)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("srp hash: %w", err)
	}

	// Server side: verify the client's proof.
	if !tsrp.Verify(storedVerifier, pw.Salt1, pw.Salt2, answer.A, answer.M1, bPub, bSecret) {
		return BootstrapResult{}, fmt.Errorf("%w: srp verify failed for user %d", ErrBootstrapSquatted, userID)
	}

	// Idempotent match — no-op.
	return BootstrapResult{UserID: userID, Created: false}, nil
}

// computeSRPVerifier generates fresh salts and computes the SRP verifier for
// the given password. It uses gotd's crypto/srp.NewHash which augments salt1
// with 32 random bytes (as per Telegram's SRP spec). Returns the verifier,
// the augmented salt1, and salt2.
func computeSRPVerifier(password []byte) (verifier, salt1, salt2 []byte, err error) {
	salt1 = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt1); err != nil {
		return nil, nil, nil, fmt.Errorf("generate salt1: %w", err)
	}
	salt2 = make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, salt2); err != nil {
		return nil, nil, nil, fmt.Errorf("generate salt2: %w", err)
	}

	srpClient := srp.NewSRP(rand.Reader)
	algo := srp.Input{
		Salt1: salt1,
		Salt2: salt2,
		G:     3,
		P:     tsrp.PBytes(),
	}
	verifier, augmentedSalt1, err := srpClient.NewHash(password, algo)
	if err != nil {
		return nil, nil, nil, err
	}
	// NewHash returns the augmented salt1 (original + 32 random bytes).
	// Store the augmented one — that's what the client will use for verification.
	return verifier, augmentedSalt1, salt2, nil
}

// ValidateBootstrapUsername checks the bootstrap username against the same
// validation rules applied to any username claim: format and reserved blocklist.
// The username must already be lowercase-normalized.
func ValidateBootstrapUsername(handle string) error {
	if !bootstrapUsernameRe.MatchString(handle) {
		return ErrBootstrapInvalid
	}
	if bootstrapReserved[handle] {
		return ErrBootstrapReserved
	}
	return nil
}
