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

// VerifyCode checks the code+hash for phone. Returns ErrCodeInvalid or
// ErrCodeExpired on failure.
func (s *Store) VerifyCode(ctx context.Context, phone, hash, code string) error {
	row, err := s.q.GetCode(ctx, phone)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return ErrCodeInvalid
	case err != nil:
		return fmt.Errorf("verify code: %w", err)
	}
	if time.Now().After(row.ExpiresAt.Time) {
		return ErrCodeExpired
	}
	if hash != row.CodeHash || code != row.Code {
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
