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

	got, ok, err := s.Get(ctx, authKey.ID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("expected key present")
	}
	if got.ID != authKey.ID || got.Value != authKey.Value {
		t.Fatal("returned key does not match saved key")
	}
}

func TestMemoryAuthKeyStoreMiss(t *testing.T) {
	_, ok, err := mtproto.NewMemoryAuthKeyStore().Get(context.Background(), [8]byte{1, 2, 3})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("expected miss for absent key")
	}
}
