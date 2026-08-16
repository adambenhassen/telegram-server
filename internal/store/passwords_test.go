package store_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestPasswordCRUD(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551250001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Absent row: no cloud password.
	if _, ok, err := s.PasswordByUser(ctx, u.ID); err != nil || ok {
		t.Fatalf("absent password: ok=%v err=%v", ok, err)
	}

	want := store.UserPassword{
		UserID:   u.ID,
		Salt1:    []byte("salt-one-0123456789abcdef"),
		Salt2:    []byte("salt-two-0123456789abcdef"),
		Verifier: []byte{0x00, 0xde, 0xad, 0xbe, 0xef, 0x00},
		Hint:     "my hint",
	}
	if err := s.UpsertPassword(ctx, want); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	got, ok, err := s.PasswordByUser(ctx, u.ID)
	if err != nil || !ok {
		t.Fatalf("get: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got.Verifier, want.Verifier) {
		t.Errorf("verifier round-trip: got %x want %x", got.Verifier, want.Verifier)
	}
	if !bytes.Equal(got.Salt1, want.Salt1) || !bytes.Equal(got.Salt2, want.Salt2) {
		t.Errorf("salts round-trip mismatch")
	}
	if got.Hint != want.Hint {
		t.Errorf("hint: got %q want %q", got.Hint, want.Hint)
	}

	// Change: replace verifier/hint.
	want.Verifier = []byte{0x11, 0x22, 0x33}
	want.Hint = "new hint"
	if err := s.UpsertPassword(ctx, want); err != nil {
		t.Fatalf("upsert change: %v", err)
	}
	got, _, err = s.PasswordByUser(ctx, u.ID)
	if err != nil {
		t.Fatalf("get after change: %v", err)
	}
	if !bytes.Equal(got.Verifier, want.Verifier) || got.Hint != "new hint" {
		t.Errorf("change not applied: verifier=%x hint=%q", got.Verifier, got.Hint)
	}

	// Remove.
	found, err := s.DeletePassword(ctx, u.ID)
	if err != nil || !found {
		t.Fatalf("delete: found=%v err=%v", found, err)
	}
	if _, ok, err := s.PasswordByUser(ctx, u.ID); err != nil || ok {
		t.Fatalf("password still present after delete: ok=%v err=%v", ok, err)
	}
	// Deleting again reports not found.
	if found, err := s.DeletePassword(ctx, u.ID); err != nil || found {
		t.Fatalf("second delete: found=%v err=%v", found, err)
	}
}

func TestPasswordVerifierEncryptedAtRest(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	})

	u, err := s.CreateUser(ctx, "+15551250002")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	verifier := []byte("super-secret-verifier-material")
	if err := s.UpsertPassword(ctx, store.UserPassword{
		UserID: u.ID, Salt1: []byte("a"), Salt2: []byte("b"), Verifier: verifier,
	}); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Read the raw stored bytes via a direct connection: they must not contain
	// the plaintext verifier.
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("raw connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	var raw []byte
	if err := conn.QueryRow(ctx, `SELECT verifier FROM user_passwords WHERE user_id = $1`, u.ID).Scan(&raw); err != nil {
		t.Fatalf("read raw verifier: %v", err)
	}
	if bytes.Contains(raw, verifier) {
		t.Fatal("verifier stored in plaintext")
	}
}

func TestPendingUserSetPromoteClear(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const keyID = int64(0x3001)

	u, err := s.CreateUser(ctx, "+15551250003")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.SaveAuthKey(ctx, keyID, []byte("k")); err != nil {
		t.Fatalf("save key: %v", err)
	}

	// Set pending: the key stays unbound (UserID 0).
	if err := s.SetPendingUser(ctx, keyID, u.ID); err != nil {
		t.Fatalf("set pending: %v", err)
	}
	got, _, err := s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.UserID != 0 {
		t.Fatalf("pending must not authorize: UserID=%d", got.UserID)
	}

	// Promote: pending → user_id.
	if err := s.PromotePendingUser(ctx, keyID, u.ID); err != nil {
		t.Fatalf("promote: %v", err)
	}
	got, _, err = s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.UserID != u.ID {
		t.Fatalf("promote did not bind: UserID=%d want %d", got.UserID, u.ID)
	}

	// Second promote is a no-op (pending already cleared) → fail closed.
	if err := s.PromotePendingUser(ctx, keyID, u.ID); !errors.Is(err, store.ErrAuthKeyNotFound) {
		t.Fatalf("re-promote: got %v want ErrAuthKeyNotFound", err)
	}
}

func TestPromotePendingUserMismatchFailsClosed(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const keyID = int64(0x3002)

	victim, err := s.CreateUser(ctx, "+15551250004")
	if err != nil {
		t.Fatalf("create victim: %v", err)
	}
	attacker, err := s.CreateUser(ctx, "+15551250005")
	if err != nil {
		t.Fatalf("create attacker: %v", err)
	}
	if err := s.SaveAuthKey(ctx, keyID, []byte("k")); err != nil {
		t.Fatalf("save key: %v", err)
	}
	if err := s.SetPendingUser(ctx, keyID, victim.ID); err != nil {
		t.Fatalf("set pending: %v", err)
	}
	// Promoting for a different user than the staged pending must not bind.
	if err := s.PromotePendingUser(ctx, keyID, attacker.ID); !errors.Is(err, store.ErrAuthKeyNotFound) {
		t.Fatalf("cross-user promote: got %v want ErrAuthKeyNotFound", err)
	}
	got, _, err := s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.UserID != 0 {
		t.Fatalf("key authorized after mismatched promote: UserID=%d", got.UserID)
	}
}

func TestSetPendingUserClearsExistingBinding(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const keyID = int64(0x3003)

	u, err := s.CreateUser(ctx, "+15551250007")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.SaveAuthKey(ctx, keyID, []byte("k")); err != nil {
		t.Fatalf("save key: %v", err)
	}
	if err := s.BindAuthKeyUser(ctx, keyID, u.ID); err != nil {
		t.Fatalf("bind: %v", err)
	}
	// A re-signIn that stages 2FA must drop the existing authorization: user_id
	// cleared, pending set, so the key is not authorized until checkPassword.
	if err := s.SetPendingUser(ctx, keyID, u.ID); err != nil {
		t.Fatalf("set pending: %v", err)
	}
	got, _, err := s.AuthKeyByID(ctx, keyID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.UserID != 0 {
		t.Fatalf("user_id not cleared on set pending: %d", got.UserID)
	}
	if got.PendingUserID != u.ID {
		t.Fatalf("pending not set: %d want %d", got.PendingUserID, u.ID)
	}
}

func TestSetPendingUserMissingKeyFailsClosed(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	u, err := s.CreateUser(ctx, "+15551250006")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.SetPendingUser(ctx, 0xdead, u.ID); !errors.Is(err, store.ErrAuthKeyNotFound) {
		t.Fatalf("set pending missing key: got %v want ErrAuthKeyNotFound", err)
	}
}
