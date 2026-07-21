package mtproto

import (
	"context"
	"fmt"

	"github.com/gotd/td/crypto"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// pgAuthKeyStore is an AuthKeyStore backed by the Postgres store. It persists
// auth keys across server restarts so a logged-in client stays authorized
// without a new handshake.
type pgAuthKeyStore struct {
	s *store.Store
}

var _ AuthKeyStore = (*pgAuthKeyStore)(nil)

// NewPgAuthKeyStore returns an AuthKeyStore that reads and writes auth keys via s.
func NewPgAuthKeyStore(s *store.Store) AuthKeyStore {
	return &pgAuthKeyStore{s: s}
}

// Save stores the key by its int64 id with the full 256-byte key value.
func (p *pgAuthKeyStore) Save(ctx context.Context, key crypto.AuthKey) error {
	return p.s.SaveAuthKey(ctx, key.IntID(), key.Value[:])
}

// Get loads the stored key value and reconstructs the AuthKey. The id is
// recomputed from the key bytes (crypto.Key.WithID), so it round-trips exactly:
// identical value bytes yield an identical fingerprint and int64 id.
func (p *pgAuthKeyStore) Get(ctx context.Context, id [8]byte) (crypto.AuthKey, bool, error) {
	row, ok, err := p.s.AuthKeyByID(ctx, AuthKeyIDInt64(id))
	if err != nil {
		return crypto.AuthKey{}, false, err
	}
	if !ok {
		return crypto.AuthKey{}, false, nil
	}
	if len(row.Value) != len(crypto.Key{}) {
		return crypto.AuthKey{}, false, fmt.Errorf("auth key %d: stored value is %d bytes, want %d", row.ID, len(row.Value), len(crypto.Key{}))
	}
	var value crypto.Key
	copy(value[:], row.Value)
	return value.WithID(), true, nil
}

// UserID returns the user bound to the auth key, or 0 when unbound/unknown.
func (p *pgAuthKeyStore) UserID(ctx context.Context, id [8]byte) (int64, error) {
	row, ok, err := p.s.AuthKeyByID(ctx, AuthKeyIDInt64(id))
	if err != nil || !ok {
		return 0, err
	}
	return row.UserID, nil
}
