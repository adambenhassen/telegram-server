package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func open(t *testing.T) *store.Store {
	t.Helper()
	s, err := store.Open(context.Background(), pgtest.DSN(t))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s
}

func TestCreateUserAndLookup(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const phone = "+15551230001"

	u, err := s.CreateUser(ctx, phone)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if u.ID == 0 {
		t.Fatal("user id is zero")
	}
	got, ok, err := s.UserByPhone(ctx, phone)
	if err != nil || !ok {
		t.Fatalf("lookup: ok=%v err=%v", ok, err)
	}
	if got.ID != u.ID {
		t.Errorf("id mismatch: %d vs %d", got.ID, u.ID)
	}
}

func TestCreateUserIsIdempotent(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const phone = "+15551230002"

	a, err := s.CreateUser(ctx, phone)
	if err != nil {
		t.Fatalf("first create: %v", err)
	}
	b, err := s.CreateUser(ctx, phone)
	if err != nil {
		t.Fatalf("second create: %v", err)
	}
	if a.ID != b.ID {
		t.Errorf("duplicate phone made new id: %d != %d", a.ID, b.ID)
	}
}

func TestCodeLifecycle(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const phone = "+15551230003"

	hash, code, err := s.IssueCode(ctx, phone)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if err := s.VerifyCode(ctx, phone, hash, code); err != nil {
		t.Fatalf("verify valid: %v", err)
	}
	wrong := "00000"
	if code == wrong {
		wrong = "11111"
	}
	if err := s.VerifyCode(ctx, phone, hash, wrong); !errors.Is(err, store.ErrCodeInvalid) {
		t.Errorf("want ErrCodeInvalid, got %v", err)
	}
	if err := s.VerifyCode(ctx, phone, "wronghash", code); !errors.Is(err, store.ErrCodeInvalid) {
		t.Errorf("wrong hash: want ErrCodeInvalid, got %v", err)
	}
}
