package mtproto

import (
	"context"
	"sync"

	"github.com/gotd/td/crypto"
)

// AuthKeyStore persists MTProto auth keys established during key exchange.
type AuthKeyStore interface {
	// Save stores the auth key, keyed by its ID.
	Save(ctx context.Context, key crypto.AuthKey) error
	// Get returns the auth key for the given ID; ok is false when absent.
	Get(ctx context.Context, id [8]byte) (key crypto.AuthKey, ok bool, err error)
	// UserID returns the user bound to the auth key, or 0 when unbound/unknown.
	UserID(ctx context.Context, id [8]byte) (int64, error)
}

// AuthKeyIDInt64 converts an 8-byte MTProto auth key ID to the int64 storage id,
// delegating to gotd's own little-endian conversion so save and lookup stay
// consistent. It is the single source of truth for the [8]byte<->int64 mapping.
func AuthKeyIDInt64(id [8]byte) int64 {
	return crypto.AuthKey{ID: id}.IntID()
}

// memoryAuthKeyStore is an in-memory AuthKeyStore for tests and single-process
// use. A Postgres-backed store replaces it in a later task.
type memoryAuthKeyStore struct {
	mu   sync.Mutex
	keys map[[8]byte]crypto.AuthKey
}

var _ AuthKeyStore = (*memoryAuthKeyStore)(nil)

// NewMemoryAuthKeyStore creates an empty in-memory AuthKeyStore.
func NewMemoryAuthKeyStore() AuthKeyStore {
	return &memoryAuthKeyStore{keys: map[[8]byte]crypto.AuthKey{}}
}

// Save stores the auth key by its ID.
func (s *memoryAuthKeyStore) Save(_ context.Context, key crypto.AuthKey) error {
	s.mu.Lock()
	s.keys[key.ID] = key
	s.mu.Unlock()
	return nil
}

// Get returns the auth key for the given ID.
func (s *memoryAuthKeyStore) Get(_ context.Context, id [8]byte) (crypto.AuthKey, bool, error) {
	s.mu.Lock()
	key, ok := s.keys[id]
	s.mu.Unlock()
	return key, ok, nil
}

// UserID always returns 0: the in-memory store does not track user bindings.
func (s *memoryAuthKeyStore) UserID(_ context.Context, _ [8]byte) (int64, error) {
	return 0, nil
}
