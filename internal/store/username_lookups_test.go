package store_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestUsernameLookupQuota(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Fill quota with distinct handles.
	for i := range store.UsernameLookupLimit {
		handle := fmt.Sprintf("handle%09d", i)
		if err := s.CheckAndChargeUsernameLookup(context.Background(), user.ID, handle); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}

	// 101st distinct lookup should exceed quota.
	err = s.CheckAndChargeUsernameLookup(context.Background(), user.ID, "handle999999999")
	if !errors.Is(err, store.ErrUsernameLookupQuotaExceeded) {
		t.Fatalf("101st lookup: got %v, want ErrUsernameLookupQuotaExceeded", err)
	}
}

func TestUsernameLookupRetrySame(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Fill quota with distinct handles.
	for i := range store.UsernameLookupLimit {
		handle := fmt.Sprintf("handle%09d", i)
		if err := s.CheckAndChargeUsernameLookup(context.Background(), user.ID, handle); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}

	// Retry the same handle as loop iteration 0 — distinct count stays at 100.
	retryHandle := fmt.Sprintf("handle%09d", 0)
	if err := s.CheckAndChargeUsernameLookup(context.Background(), user.ID, retryHandle); err != nil {
		t.Fatalf("retry of same handle: %v", err)
	}
}

func TestUsernameLookupBurstCap(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)
	s, err := store.Open(context.Background(), dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }() //nolint:errcheck // best-effort close

	user, err := s.CreateUser(context.Background(), "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	// Burst cap is 20 per minute. The 21st distinct lookup in the same minute
	// should exceed the burst cap.
	for i := range store.UsernameLookupBurstLimit {
		handle := fmt.Sprintf("burst%09d", i)
		if err := s.CheckAndChargeUsernameLookup(context.Background(), user.ID, handle); err != nil {
			t.Fatalf("lookup %d: %v", i, err)
		}
	}

	// 21st distinct lookup in same minute should be rate-limited.
	err = s.CheckAndChargeUsernameLookup(context.Background(), user.ID, "burst999999999")
	if !errors.Is(err, store.ErrUsernameLookupQuotaExceeded) {
		t.Fatalf("21st burst lookup: got %v, want ErrUsernameLookupQuotaExceeded", err)
	}
}
