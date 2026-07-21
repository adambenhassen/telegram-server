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
)

// IssueCode generates a 5-digit login code and hash for phone, storing it with
// a TTL. It returns ErrResendTooSoon if an active (unconsumed, unexpired, not
// exhausted) code was issued within resendCooldown. On success it resets the
// per-code hardening state (attempts, consumed_at, created_at).
func (s *Store) IssueCode(ctx context.Context, phone string) (string, string, error) {
	existing, err := s.q.GetCode(ctx, phone)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// No prior code: nothing blocks a fresh issue.
	case err != nil:
		return "", "", fmt.Errorf("issue code: %w", err)
	default:
		if codeActive(existing) && time.Since(existing.CreatedAt.Time) < resendCooldown {
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
	err = s.q.UpsertCode(ctx, db.UpsertCodeParams{
		Phone:     phone,
		CodeHash:  hash,
		Code:      code,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(codeTTL), Valid: true},
	})
	if err != nil {
		return "", "", fmt.Errorf("issue code: %w", err)
	}
	return hash, code, nil
}

// codeActive reports whether an issued code can still be verified: not yet
// consumed, not expired, and under the attempt cap.
func codeActive(row db.PhoneCode) bool {
	return !row.ConsumedAt.Valid &&
		!time.Now().After(row.ExpiresAt.Time) &&
		row.Attempts < maxAttempts
}

// VerifyCode checks the code+hash for phone. It is single-use and fail-closed:
// an already-consumed, expired, or exhausted code never verifies. Checks run in
// order — consumed → expired → exhausted → credential match — so the strictest
// terminal state wins. A wrong hash or code increments the attempt counter and
// returns ErrCodeInvalid without revealing which field was wrong; on the attempt
// that reaches maxAttempts the code becomes exhausted. A correct match consumes
// the code and returns nil.
func (s *Store) VerifyCode(ctx context.Context, phone, hash, code string) error {
	row, err := s.q.GetCode(ctx, phone)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrCodeInvalid
	case err != nil:
		return fmt.Errorf("verify code: %w", err)
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
		if err := s.q.IncrementCodeAttempts(ctx, phone); err != nil {
			return fmt.Errorf("verify code: %w", err)
		}
		return ErrCodeInvalid
	}
	if err := s.q.ConsumeCode(ctx, phone); err != nil {
		return fmt.Errorf("verify code: %w", err)
	}
	return nil
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
