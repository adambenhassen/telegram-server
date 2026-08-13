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
	// Get returns the auth key, the user bound to it (0 when unbound), and
	// whether the session is provisional (username-mode with no verifier)
	// in a single lookup; ok is false when the key is absent.
	Get(ctx context.Context, id [8]byte) (key crypto.AuthKey, userID int64, provisional bool, ok bool, err error)
	// Touch advances the key's last-seen time; it is best-effort activity
	// tracking and callers may ignore transient errors.
	Touch(ctx context.Context, id [8]byte) error
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

// Get returns the auth key for the given ID. The in-memory store does not track
// user bindings, so userID is always 0 and provisional is always false.
func (s *memoryAuthKeyStore) Get(_ context.Context, id [8]byte) (crypto.AuthKey, int64, bool, bool, error) {
	s.mu.Lock()
	key, ok := s.keys[id]
	s.mu.Unlock()
	return key, 0, false, ok, nil
}

// Touch is a no-op: the in-memory store does not track last-seen times.
func (s *memoryAuthKeyStore) Touch(_ context.Context, _ [8]byte) error {
	return nil
}
