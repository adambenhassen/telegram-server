package srp

import (
	"math/big"
	"time"
)

// Test-only accessors into unexported internals, kept in a _test.go file so they
// never ship in the production build.

// Pad exposes pad for padding-exactness tests.
func Pad(b []byte) []byte { return pad(b) }

// Prime returns a copy of the modulus p for tests.
func Prime() *big.Int { return new(big.Int).Set(p) }

// SetClock overrides the challenge store's clock for deterministic expiry tests.
func (s *ChallengeStore) SetClock(f func() time.Time) { s.now = f }

// Has reports whether an entry for id is currently held.
func (s *ChallengeStore) Has(id int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.entries[id]
	return ok
}
