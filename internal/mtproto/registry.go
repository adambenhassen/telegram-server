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
