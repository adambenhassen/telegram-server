package srp

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"
)

// DefaultTTL is how long an issued SRP challenge stays valid. A client that
// takes longer between account.getPassword and auth.checkPassword must re-fetch.
const DefaultTTL = 5 * time.Minute

// Pending is a live SRP challenge: the server secret b and its public B, bound
// to the user the challenge was issued for.
type Pending struct {
	BSecret []byte
	BPublic []byte
	UserID  int64
}

// entry is one stored challenge with its expiry.
type entry struct {
	pending   Pending
	expiresAt time.Time
}

// ChallengeStore holds outstanding SRP challenges keyed by srp_id. Entries are
// single-use (consumed on the first Consume) and expire after ttl. At most one
// challenge is retained per user: a fresh Issue evicts that user's prior
// challenge, so a client that repeatedly calls account.getPassword cannot grow
// the map without bound (it is capped by the number of distinct users
// mid-challenge, and account creation is itself phone-code gated).
//
// ponytail: in-memory, per-instance, lost on restart — a client just re-calls
// account.getPassword. Cleanup is opportunistic (swept on Issue) rather than a
// background goroutine, so there is no lifecycle to plumb through api.New. Move
// to a shared/DB store with a real sweep only if the server is ever run
// multi-instance.
type ChallengeStore struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[int64]entry // srp_id → challenge
	byUser  map[int64]int64 // user id → its current srp_id
	now     func() time.Time
}

// NewChallengeStore builds an empty challenge store with the given TTL.
func NewChallengeStore(ttl time.Duration) *ChallengeStore {
	return &ChallengeStore{
		ttl:     ttl,
		entries: make(map[int64]entry),
		byUser:  make(map[int64]int64),
		now:     time.Now,
	}
}

// Issue creates a fresh SRP challenge for userID from the stored verifier,
// registers it under a new non-zero srp_id, and returns srp_id and B. Expired
// entries are swept in the same pass.
func (s *ChallengeStore) Issue(userID int64, verifier []byte) (srpID int64, bPublic []byte, err error) {
	bPub, bSecret, err := Challenge(verifier)
	if err != nil {
		return 0, nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked()
	// Evict this user's prior challenge: only the latest is usable.
	if prev, ok := s.byUser[userID]; ok {
		delete(s.entries, prev)
	}
	id, err := s.freeIDLocked()
	if err != nil {
		return 0, nil, err
	}
	s.entries[id] = entry{
		pending:   Pending{BSecret: bSecret, BPublic: bPub, UserID: userID},
		expiresAt: s.now().Add(s.ttl),
	}
	s.byUser[userID] = id
	return id, bPub, nil
}

// Consume returns and removes the challenge for srpID. ok is false when the id
// is unknown, already consumed, or expired — the caller maps that to
// SRP_ID_INVALID. Single-use: a consumed id never returns again.
func (s *ChallengeStore) Consume(srpID int64) (Pending, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[srpID]
	if !ok {
		return Pending{}, false
	}
	delete(s.entries, srpID)
	s.forgetUserLocked(e.pending.UserID, srpID)
	if s.now().After(e.expiresAt) {
		return Pending{}, false
	}
	return e.pending, true
}

// sweepLocked deletes expired entries. Caller holds mu.
func (s *ChallengeStore) sweepLocked() {
	now := s.now()
	for id, e := range s.entries {
		if now.After(e.expiresAt) {
			delete(s.entries, id)
			s.forgetUserLocked(e.pending.UserID, id)
		}
	}
}

// forgetUserLocked drops the byUser back-reference for userID, but only when it
// still points at srpID (a newer challenge may have replaced it). Caller holds mu.
func (s *ChallengeStore) forgetUserLocked(userID, srpID int64) {
	if s.byUser[userID] == srpID {
		delete(s.byUser, userID)
	}
}

// freeIDLocked draws a random non-zero int64 not already in use. Caller holds mu.
func (s *ChallengeStore) freeIDLocked() (int64, error) {
	var buf [8]byte
	for range 100 {
		if _, err := rand.Read(buf[:]); err != nil {
			return 0, fmt.Errorf("srp: rand id: %w", err)
		}
		// The full uint64→int64 range (including negatives) is intended: srp_id is
		// an opaque non-zero identifier, not a magnitude.
		id := int64(binary.BigEndian.Uint64(buf[:])) //nolint:gosec // G115: opaque id, sign irrelevant
		if id == 0 {
			continue
		}
		if _, exists := s.entries[id]; !exists {
			return id, nil
		}
	}
	return 0, errors.New("srp: could not allocate srp_id")
}
