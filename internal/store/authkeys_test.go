package store_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSaveAndGetAuthKey(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x1001)
	value := []byte{0x00, 0xff, 0x10, 0x20, 0x00}

	if err := s.SaveAuthKey(ctx, id, value); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.ID != id {
		t.Errorf("id: got %d want %d", got.ID, id)
	}
	if !bytes.Equal(got.Value, value) {
		t.Errorf("value: got %v want %v", got.Value, value)
	}
	if got.UserID != 0 {
		t.Errorf("unbound key has UserID %d, want 0", got.UserID)
	}
}

func TestAuthKeyByIDMissing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	_, ok, err := s.AuthKeyByID(ctx, 0xdead)
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if ok {
		t.Error("ok=true for absent key")
	}
}

func TestSaveAuthKeyIsIdempotentUpsert(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x1002)

	if err := s.SaveAuthKey(ctx, id, []byte("first")); err != nil {
		t.Fatalf("first save: %v", err)
	}
	if err := s.SaveAuthKey(ctx, id, []byte("second")); err != nil {
		t.Fatalf("second save: %v", err)
	}
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got.Value, []byte("second")) {
		t.Errorf("value not updated: got %q want %q", got.Value, "second")
	}
}

func TestBindAuthKeyUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x1003)

	u, err := s.CreateUser(ctx, "+15551240003")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.BindAuthKeyUser(ctx, id, u.ID); err != nil {
		t.Fatalf("bind: %v", err)
	}
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.UserID != u.ID {
		t.Errorf("UserID: got %d want %d", got.UserID, u.ID)
	}
}

func TestBindAuthKeyUserMissingKeyFailsClosed(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551240099")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	// No SaveAuthKey: the key row does not exist, so the bind must not silently
	// succeed.
	err = s.BindAuthKeyUser(ctx, 0xdead, u.ID)
	if !errors.Is(err, store.ErrAuthKeyNotFound) {
		t.Fatalf("bind missing key: got %v, want ErrAuthKeyNotFound", err)
	}
}

func TestAuthKeysByUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551240004")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	ids := []int64{0x2001, 0x2002}
	for _, id := range ids {
		if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
			t.Fatalf("save %d: %v", id, err)
		}
		if err := s.BindAuthKeyUser(ctx, id, u.ID); err != nil {
			t.Fatalf("bind %d: %v", id, err)
		}
	}
	keys, err := s.AuthKeysByUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(keys) != len(ids) {
		t.Fatalf("got %d keys, want %d", len(keys), len(ids))
	}
	for _, k := range keys {
		if k.UserID != u.ID {
			t.Errorf("key %d bound to %d, want %d", k.ID, k.UserID, u.ID)
		}
	}
}

func TestDeleteAuthKey(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x1004)

	if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.DeleteAuthKey(ctx, id); err != nil {
		t.Fatalf("delete: %v", err)
	}
	_, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil {
		t.Fatalf("get after delete: %v", err)
	}
	if ok {
		t.Error("key still present after delete")
	}
}

// --- Provisional state tests ---

func TestAuthKeyProvisionalUnbound(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x4001)

	if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.Provisional {
		t.Error("unbound key should not be provisional")
	}
}

func TestAuthKeyProvisionalPhoneMode(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x4002)

	u, err := s.CreateUser(ctx, "+15551270001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.BindAuthKeyUser(ctx, id, u.ID); err != nil {
		t.Fatalf("bind: %v", err)
	}
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.Provisional {
		t.Error("phone-mode key should not be provisional")
	}
}

func TestAuthKeyProvisionalUsernameModeNoVerifier(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x4003)

	u, err := s.CreateUsernameUser(ctx, "alice1", "Alice", "Smith")
	if err != nil {
		t.Fatalf("create username user: %v", err)
	}
	if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.BindAuthKeyUser(ctx, id, u.ID); err != nil {
		t.Fatalf("bind: %v", err)
	}
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !got.Provisional {
		t.Error("username-mode key with no verifier should be provisional")
	}
}

func TestAuthKeyProvisionalClearedAfterVerifier(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x4004)

	u, err := s.CreateUsernameUser(ctx, "bob1", "Bob", "Jones")
	if err != nil {
		t.Fatalf("create username user: %v", err)
	}
	if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
		t.Fatalf("save: %v", err)
	}
	if err := s.BindAuthKeyUser(ctx, id, u.ID); err != nil {
		t.Fatalf("bind: %v", err)
	}
	// Before verifier: provisional.
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !got.Provisional {
		t.Error("should be provisional before verifier")
	}

	// Store a verifier (simulates checkPassword completing SRP proof).
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID:   u.ID,
		Salt1:    []byte("salt1"),
		Salt2:    []byte("salt2"),
		Verifier: []byte{0x01, 0x02},
	}); err != nil {
		t.Fatalf("upsert password: %v", err)
	}

	// After verifier: no longer provisional.
	got, ok, err = s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if got.Provisional {
		t.Error("should not be provisional after verifier")
	}
}

func TestAuthKeyProvisionalPendingUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const id = int64(0x4005)

	u, err := s.CreateUsernameUser(ctx, "carol1", "Carol", "White")
	if err != nil {
		t.Fatalf("create username user: %v", err)
	}
	if err := s.SaveAuthKey(ctx, id, []byte("k")); err != nil {
		t.Fatalf("save: %v", err)
	}
	// Set pending (user_id cleared, pending_user_id set).
	if err := s.SetPendingUser(ctx, id, u.ID); err != nil {
		t.Fatalf("set pending: %v", err)
	}
	got, ok, err := s.AuthKeyByID(ctx, id)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	// Pending key has user_id = NULL, so Provisional must be false
	// (unbound keys are not provisional).
	if got.Provisional {
		t.Error("pending key should not be provisional (user_id is NULL)")
	}
}
