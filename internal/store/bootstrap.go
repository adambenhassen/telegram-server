package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"

	"github.com/gotd/td/crypto/srp"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"golang.org/x/crypto/pbkdf2"

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
// If the username already exists, it checks for an idempotent match by reading
// the stored salts and recomputing the verifier from the bootstrap password.
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
			// Re-check existence and try idempotent path.
			var raceUsername db.Username
			err = tx.QueryRow(ctx, `
				SELECT handle, owner_type, owner_id
				FROM usernames
				WHERE handle = $1
			`, p.Handle).Scan(&raceUsername.Handle, &raceUsername.OwnerType, &raceUsername.OwnerID)
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return BootstrapResult{}, ErrBootstrapSquatted
				}
				return BootstrapResult{}, fmt.Errorf("re-check username after race: %w", err)
			}
			_ = tx.Rollback(ctx) //nolint:errcheck // best-effort
			return s.bootstrapCheckExisting(ctx, s.q.WithTx(tx), p, raceUsername)
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

// bootstrapCheckExisting verifies that an existing username row matches the
// bootstrap credential. Returns idempotent no-op on match, error on mismatch.
//
// It reads the stored salt1/salt2 from the passwords row, recomputes the
// verifier from the bootstrap password using those salts, and compares
// against the stored verifier. This ensures repeated bootstrap calls with
// the same password correctly match on restart.
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

	// Recompute the verifier using the stored salts and the bootstrap password.
	recomputed, err := computeVerifierFromSalts(p.Password, pw.Salt1, pw.Salt2)
	if err != nil {
		return BootstrapResult{}, fmt.Errorf("recompute verifier: %w", err)
	}

	// Compare verifiers in constant time.
	if subtle.ConstantTimeCompare(storedVerifier, recomputed) != 1 {
		return BootstrapResult{}, fmt.Errorf("%w: verifier mismatch for user %d", ErrBootstrapSquatted, userID)
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

// computeVerifierFromSalts computes the SRP verifier v = g^x mod p from the
// password and given salts. This is the same computation the client performs
// when checking a password, and is what the server-side SRP Verify function
// expects to match.
func computeVerifierFromSalts(password, salt1, salt2 []byte) ([]byte, error) {
	p := new(big.Int).SetBytes(tsrp.PBytes())
	g := big.NewInt(3)

	// x = PH2(password, salt1, salt2)
	x := computeX(password, salt1, salt2)
	// v = g^x mod p
	v := new(big.Int).Exp(g, x, p)
	verifier := make([]byte, 256)
	copy(verifier[256-len(v.Bytes()):], v.Bytes())
	return verifier, nil
}

// computeX computes x = PH2(password, salt1, salt2) using the same KDF chain
// as gotd's crypto/srp package.
//
// PH2(password, salt1, salt2) = SH(PBKDF2-SHA512(PH1(password, salt1, salt2), salt1, 100000), salt2)
// PH1(password, salt1, salt2) = SH(SH(password, salt1), salt2)
// SH(data, salt) = SHA256(salt || data || salt)
func computeX(password, salt1, salt2 []byte) *big.Int {
	ph1 := saltHash(saltHash(password, salt1), salt2)
	pbkdf2Result := pbkdf2SHA512(ph1, salt1, 100000)
	ph2 := saltHash(pbkdf2Result, salt2)
	return new(big.Int).SetBytes(ph2)
}

func saltHash(data, salt []byte) []byte {
	h := sha256.New()
	h.Write(salt)
	h.Write(data)
	h.Write(salt)
	return h.Sum(nil)
}

func pbkdf2SHA512(password, salt []byte, iterations int) []byte {
	return pbkdf2.Key(password, salt, iterations, 64, sha512.New)
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
