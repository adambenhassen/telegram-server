package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// wrongCode returns a 5-digit code guaranteed to differ from code.
func wrongCode(code string) string {
	if code == "00000" {
		return "11111"
	}
	return "00000"
}

func TestVerifyCodeSingleUse(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const phone = "+15551250001"

	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if err := s.VerifyCode(ctx, phone, hash, code); err != nil {
		t.Fatalf("first verify: %v", err)
	}
	// Single-use: the same valid code must not verify a second time.
	if err := s.VerifyCode(ctx, phone, hash, code); !errors.Is(err, store.ErrCodeInvalid) {
		t.Fatalf("second verify: got %v, want ErrCodeInvalid", err)
	}
}

func TestVerifyCodeAttemptCap(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const phone = "+15551250002"

	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	bad := wrongCode(code)
	// Three wrong attempts exhaust the code.
	for i := range 3 {
		if err := s.VerifyCode(ctx, phone, hash, bad); !errors.Is(err, store.ErrCodeInvalid) {
			t.Fatalf("wrong attempt %d: got %v, want ErrCodeInvalid", i+1, err)
		}
	}
	// Even the correct code no longer verifies once exhausted (fail-closed).
	if err := s.VerifyCode(ctx, phone, hash, code); !errors.Is(err, store.ErrCodeExhausted) {
		t.Fatalf("post-exhaustion verify: got %v, want ErrCodeExhausted", err)
	}
}

func TestIssueCodeResendCooldown(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const phone = "+15551250003"

	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatalf("first issue: %v", err)
	}
	// A second issue while the first code is still active is rejected.
	if _, _, err := s.IssueCode(ctx, phone); !errors.Is(err, store.ErrResendTooSoon) {
		t.Fatalf("resend within cooldown: got %v, want ErrResendTooSoon", err)
	}
	// Once the active code is consumed, a fresh issue is allowed again.
	if err := s.VerifyCode(ctx, phone, hash, code); err != nil {
		t.Fatalf("verify to consume: %v", err)
	}
	if _, _, err := s.IssueCode(ctx, phone); err != nil {
		t.Fatalf("reissue after consume: %v", err)
	}
}

func TestVerifyCodeExpired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	const phone = "+15551250004"

	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	// Force the code past its TTL via a direct connection to the same DB.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("raw connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx,
		`UPDATE phone_codes SET expires_at = now() - interval '1 minute' WHERE phone = $1`,
		phone,
	); err != nil {
		t.Fatalf("expire code: %v", err)
	}
	if err := s.VerifyCode(ctx, phone, hash, code); !errors.Is(err, store.ErrCodeExpired) {
		t.Fatalf("verify expired: got %v, want ErrCodeExpired", err)
	}
}
