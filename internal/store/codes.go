package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

const codeTTL = 5 * time.Minute

// IssueCode generates a 5-digit login code and hash for phone, storing it with
// a TTL. Returns (hash, code).
func (s *Store) IssueCode(ctx context.Context, phone string) (string, string, error) {
	code, err := randDigits5()
	if err != nil {
		return "", "", err
	}
	hash, err := randHex()
	if err != nil {
		return "", "", err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO phone_codes (phone, code_hash, code, expires_at)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (phone) DO UPDATE
		   SET code_hash = EXCLUDED.code_hash,
		       code = EXCLUDED.code,
		       expires_at = EXCLUDED.expires_at`,
		phone, hash, code, time.Now().Add(codeTTL),
	)
	if err != nil {
		return "", "", fmt.Errorf("issue code: %w", err)
	}
	return hash, code, nil
}

// VerifyCode checks the code+hash for phone. Returns ErrCodeInvalid or
// ErrCodeExpired on failure.
func (s *Store) VerifyCode(ctx context.Context, phone, hash, code string) error {
	var storedCode, storedHash string
	var expires time.Time
	err := s.pool.QueryRow(ctx,
		`SELECT code, code_hash, expires_at FROM phone_codes WHERE phone = $1`,
		phone,
	).Scan(&storedCode, &storedHash, &expires)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrCodeInvalid
	case err != nil:
		return fmt.Errorf("verify code: %w", err)
	}
	if time.Now().After(expires) {
		return ErrCodeExpired
	}
	if hash != storedHash || code != storedCode {
		return ErrCodeInvalid
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
