package mtproto_test

import (
	"context"
	"testing"

	"github.com/gotd/td/crypto"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestMemoryAuthKeyStoreSaveGet(t *testing.T) {
	ctx := context.Background()
	s := mtproto.NewMemoryAuthKeyStore()

	var key crypto.Key
	key[0], key[1], key[255] = 0xAA, 0xBB, 0xCC
	authKey := key.WithID()

	if err := s.Save(ctx, authKey); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, userID, provisional, ok, err := s.Get(ctx, authKey.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("expected key present")
	}
	if userID != 0 {
		t.Fatalf("expected unbound userID 0, got %d", userID)
	}
	if provisional {
		t.Fatal("expected non-provisional for in-memory store")
	}
	if got.ID != authKey.ID || got.Value != authKey.Value {
		t.Fatal("returned key does not match saved key")
	}
}

func TestMemoryAuthKeyStoreMiss(t *testing.T) {
	_, _, provisional, ok, err := mtproto.NewMemoryAuthKeyStore().Get(context.Background(), [8]byte{1, 2, 3})
	if provisional {
		t.Fatal("expected non-provisional for absent key")
	}
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("expected miss for absent key")
	}
}
