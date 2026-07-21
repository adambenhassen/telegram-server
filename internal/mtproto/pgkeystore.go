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

// Get loads the stored key value and the bound user in one lookup and
// reconstructs the AuthKey. The id is recomputed from the key bytes
// (crypto.Key.WithID), so it round-trips exactly: identical value bytes yield an
// identical fingerprint and int64 id. userID is 0 when the key is unbound.
func (p *pgAuthKeyStore) Get(ctx context.Context, id [8]byte) (crypto.AuthKey, int64, bool, error) {
	row, ok, err := p.s.AuthKeyByID(ctx, AuthKeyIDInt64(id))
	if err != nil {
		return crypto.AuthKey{}, 0, false, err
	}
	if !ok {
		return crypto.AuthKey{}, 0, false, nil
	}
	if len(row.Value) != len(crypto.Key{}) {
		return crypto.AuthKey{}, 0, false, fmt.Errorf("auth key %d: stored value is %d bytes, want %d", row.ID, len(row.Value), len(crypto.Key{}))
	}
	var value crypto.Key
	copy(value[:], row.Value)
	return value.WithID(), row.UserID, true, nil
}

// Touch advances the key's last-seen time via the store.
func (p *pgAuthKeyStore) Touch(ctx context.Context, id [8]byte) error {
	return p.s.TouchAuthKey(ctx, AuthKeyIDInt64(id))
}
