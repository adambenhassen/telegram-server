package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

const (
	codeTTL = 5 * time.Minute
	// maxAttempts caps wrong verify attempts per issued code before it is
	// exhausted (fail-closed against brute force of the 5-digit code).
	maxAttempts = 3
	// resendCooldown is the minimum interval between issuing codes for a phone
	// while a prior code is still active.
	resendCooldown = 60 * time.Second
	// issueCodeLockClass namespaces this limiter's advisory locks. Two-argument
	// advisory locks occupy a space of their own, disjoint from the
	// single-argument ones used elsewhere in the store.
	issueCodeLockClass = 0x7467434f // "tgCO" (code)
)

// IssueCode generates a 5-digit login code and hash for phone, storing it with
// a TTL. It returns ErrResendTooSoon if an unconsumed prior code was issued
// within resendCooldown. Each call inserts a new row keyed by code_hash, so
// attempt counters are isolated between callers. The cooldown check and insert
// run inside a single transaction: pg_advisory_xact_lock hashes the normalized
// identifier so concurrent callers for the same phone are serialized regardless
// of whether a row exists yet.
func (s *Store) IssueCode(ctx context.Context, phone string) (string, string, error) {
	phone = NormalizePhone(phone)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return "", "", fmt.Errorf("issue code: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// Advisory lock on the normalized identifier serializes concurrent IssueCode
	// calls for the same phone, preventing TOCTTOU on the cooldown check. The
	// lock is transaction-scoped (xact) so it is released on commit or rollback.
	// Two-argument form with a class constant keeps this keyspace disjoint from
	// single-argument advisory locks used elsewhere in the store.
	if _, err := tx.Exec(ctx,
		"SELECT pg_advisory_xact_lock($1, hashtext($2))",
		issueCodeLockClass, phone,
	); err != nil {
		return "", "", fmt.Errorf("issue code: %w", err)
	}

	existing, err := qtx.GetLatestCode(ctx, phone)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// No prior code: nothing blocks a fresh issue.
	case err != nil:
		return "", "", fmt.Errorf("issue code: %w", err)
	default:
		// Gate on "not consumed", not "still active": an exhausted code (attempts
		// >= maxAttempts) must keep serving the cooldown, else exhausting a code
		// with wrong guesses would bypass the limit and reopen the brute force. A
		// consumed code (successful login) bypasses so a real user can re-login;
		// an expired code is already >codeTTL old so time.Since clears the window.
		if !existing.ConsumedAt.Valid && time.Since(existing.CreatedAt.Time) < resendCooldown {
			return "", "", ErrResendTooSoon
		}
	}

	code, err := randDigits5()
	if err != nil {
		return "", "", err
	}
	hash, err := randHex()
	if err != nil {
		return "", "", err
	}
	err = qtx.InsertCode(ctx, db.InsertCodeParams{
		Phone:     phone,
		CodeHash:  hash,
		Code:      code,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(codeTTL), Valid: true},
	})
	if err != nil {
		return "", "", fmt.Errorf("issue code: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return "", "", fmt.Errorf("issue code: %w", err)
	}
	return hash, code, nil
}

// IssueCodeForUsername generates a 5-digit login code and hash for a username
// identifier. Unlike IssueCode, it does not enforce a resend cooldown — each
// call inserts a new row keyed by code_hash, so attempt counters are isolated
// between callers even for the same identifier.
func (s *Store) IssueCodeForUsername(ctx context.Context, username string) (string, string, error) {
	code, err := randDigits5()
	if err != nil {
		return "", "", err
	}
	hash, err := randHex()
	if err != nil {
		return "", "", err
	}
	err = s.q.InsertCode(ctx, db.InsertCodeParams{
		Phone:     username,
		CodeHash:  hash,
		Code:      code,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(codeTTL), Valid: true},
	})
	if err != nil {
		return "", "", fmt.Errorf("issue code for username: %w", err)
	}
	return hash, code, nil
}

// VerifyCode checks the code+hash. It is single-use and fail-closed: an
// already-consumed, expired, or exhausted code never verifies. The lookup is by
// code_hash so the row found belongs exclusively to the caller who received
// that hash — attempts are charged only against that row, never against a
// different caller's code for the same phone. After the hash lookup, the phone
// binding is confirmed (row.Phone == phone); mismatch rejects without charging
// an attempt to prevent the attacker from learning anything by scanning. Success
// is decided by a compare-and-swap scoped to (phone + code_hash + code) with the
// terminal-state guards in the WHERE, so a concurrent resend/consume/expiry that
// slips between the read and the write makes the swap affect zero rows →
// ErrCodeInvalid. Both a wrong hash and a wrong code return ErrCodeInvalid
// without revealing which field was wrong, but only a wrong code under the
// correct hash charges an attempt. The attempt that reaches maxAttempts exhausts
// the code.
func (s *Store) VerifyCode(ctx context.Context, phone, hash, code string) error {
	return s.verifyCodeWith(ctx, s.q, phone, hash, code)
}

// verifyCodeWith is the body of VerifyCode, parameterized on the queries
// interface so it can be called within a transaction.
func (s *Store) verifyCodeWith(ctx context.Context, q *db.Queries, phone, hash, code string) error {
	row, err := q.GetCodeByHash(ctx, hash)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrCodeInvalid
	case err != nil:
		return fmt.Errorf("verify code: %w", err)
	}
	// Bind the code to the identifier it was issued for. An attacker who
	// obtained a valid code_hash for one identifier must not be able to verify
	// it under a different one. Reject without incrementing attempts — charging
	// on an identifier mismatch would hand back the cross-caller charging
	// primitive this ticket exists to remove.
	phone = NormalizePhone(phone)
	if phone != row.Phone {
		return ErrCodeInvalid
	}
	if row.ConsumedAt.Valid {
		return ErrCodeInvalid
	}
	if time.Now().After(row.ExpiresAt.Time) {
		return ErrCodeExpired
	}
	if row.Attempts >= maxAttempts {
		return ErrCodeExhausted
	}
	if hash != row.CodeHash || code != row.Code {
		if err := q.IncrementCodeAttempts(ctx, hash); err != nil {
			return fmt.Errorf("verify code: %w", err)
		}
		return ErrCodeInvalid
	}
	rows, err := q.ConsumeCode(ctx, db.ConsumeCodeParams{
		Phone:    phone,
		CodeHash: hash,
		Code:     code,
		Attempts: maxAttempts,
	})
	if err != nil {
		return fmt.Errorf("verify code: %w", err)
	}
	if rows == 0 {
		// The code was consumed/expired/exhausted/replaced concurrently between
		// the read above and this swap. Fail closed.
		return ErrCodeInvalid
	}
	return nil
}

// CheckCodeHash validates a code hash without verifying the actual code value.
// It confirms the hash exists, is bound to the expected identifier, is not
// consumed, not expired, and not exhausted. Used by username-mode signIn where
// the code field is ignored — only the hash is validated.
func (s *Store) CheckCodeHash(ctx context.Context, phone, hash string) error {
	phone = NormalizePhone(phone)
	row, err := s.q.GetCodeByHashAndPhone(ctx, db.GetCodeByHashAndPhoneParams{
		CodeHash: hash,
		Phone:    phone,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrCodeInvalid
	case err != nil:
		return fmt.Errorf("check code hash: %w", err)
	}
	return validateCodeHash(row)
}

// SetCodeForUsername stores the caller-supplied signIn code on its validated
// username code row. The hash remains the only value CheckCodeHash validates;
// the code is persisted so the following auth.signUp can use it as the invite
// secret.
func (s *Store) SetCodeForUsername(ctx context.Context, username, hash, code string) error {
	rows, err := s.q.SetCodeByHashAndPhone(ctx, db.SetCodeByHashAndPhoneParams{
		CodeHash: hash,
		Phone:    NormalizePhone(username),
		Code:     code,
	})
	if err != nil {
		return fmt.Errorf("set username code: %w", err)
	}
	if rows != 1 {
		return ErrCodeInvalid
	}
	return nil
}

func validateCodeHash(row db.GetCodeByHashAndPhoneRow) error {
	if row.ConsumedAt.Valid {
		return ErrCodeInvalid
	}
	if time.Now().After(row.ExpiresAt.Time) {
		return ErrCodeExpired
	}
	if row.Attempts >= maxAttempts {
		return ErrCodeExhausted
	}
	return nil
}

// DeleteExpiredCodes removes all login codes past their expiry and returns
// the number of rows deleted.
func (s *Store) DeleteExpiredCodes(ctx context.Context) (int64, error) {
	n, err := s.q.DeleteExpiredCodes(ctx)
	if err != nil {
		return 0, fmt.Errorf("delete expired codes: %w", err)
	}
	return n, nil
}

func randDigits5() (string, error) {
	b := make([]byte, 5)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	out := make([]byte, 5)
	for i, v := range b {
		out[i] = '0' + v%10
	}
	return string(out), nil
}

func randHex() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	return hex.EncodeToString(b), nil
}
