package srp_test

import (
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/srp"
)

func testVerifier(t *testing.T) []byte {
	t.Helper()
	salt1Base, salt2 := newInput(t)
	v, _ := newVerifier(t, "pw", salt1Base, salt2)
	return v
}

func TestChallengeStoreSingleUse(t *testing.T) {
	s := srp.NewChallengeStore(srp.DefaultTTL)
	v := testVerifier(t)
	id, bPub, err := s.Issue(7, v)
	if err != nil {
		t.Fatal(err)
	}
	if id == 0 || len(bPub) != srp.PadLen {
		t.Fatalf("bad issue: id=%d len(B)=%d", id, len(bPub))
	}
	got, ok := s.Consume(id)
	if !ok || got.UserID != 7 {
		t.Fatalf("first consume: ok=%v userID=%d", ok, got.UserID)
	}
	if _, ok := s.Consume(id); ok {
		t.Fatal("second consume returned ok (not single-use)")
	}
}

func TestChallengeStoreUnknownID(t *testing.T) {
	s := srp.NewChallengeStore(srp.DefaultTTL)
	if _, ok := s.Consume(12345); ok {
		t.Fatal("unknown id returned ok")
	}
}

func TestChallengeStoreExpiry(t *testing.T) {
	s := srp.NewChallengeStore(srp.DefaultTTL)
	now := time.Now()
	s.SetClock(func() time.Time { return now })
	v := testVerifier(t)
	id, _, err := s.Issue(1, v)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(srp.DefaultTTL + time.Second)
	if _, ok := s.Consume(id); ok {
		t.Fatal("expired challenge consumed")
	}
}

func TestChallengeStoreSweepOnIssue(t *testing.T) {
	s := srp.NewChallengeStore(srp.DefaultTTL)
	now := time.Now()
	s.SetClock(func() time.Time { return now })
	v := testVerifier(t)
	stale, _, err := s.Issue(1, v)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(srp.DefaultTTL + time.Second)
	if _, _, err := s.Issue(2, v); err != nil { // triggers sweep of stale
		t.Fatal(err)
	}
	if s.Has(stale) {
		t.Fatal("stale entry not swept on Issue")
	}
}

func TestChallengeStorePerUserEviction(t *testing.T) {
	s := srp.NewChallengeStore(srp.DefaultTTL)
	v := testVerifier(t)
	id1, _, err := s.Issue(9, v)
	if err != nil {
		t.Fatal(err)
	}
	id2, _, err := s.Issue(9, v) // same user: evicts id1
	if err != nil {
		t.Fatal(err)
	}
	if s.Has(id1) {
		t.Fatal("prior challenge for user not evicted")
	}
	if !s.Has(id2) {
		t.Fatal("latest challenge missing")
	}
	if _, ok := s.Consume(id1); ok {
		t.Fatal("evicted challenge is still consumable")
	}
	if _, ok := s.Consume(id2); !ok {
		t.Fatal("latest challenge not consumable")
	}
	// After consuming the latest, the user has no outstanding challenge.
	id3, _, err := s.Issue(9, v)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Has(id3) {
		t.Fatal("new challenge after consume missing")
	}
}

func TestChallengeStoreDistinctIDs(t *testing.T) {
	s := srp.NewChallengeStore(srp.DefaultTTL)
	seen := make(map[int64]bool)
	v := testVerifier(t)
	for range 50 {
		id, _, err := s.Issue(1, v)
		if err != nil {
			t.Fatal(err)
		}
		if seen[id] {
			t.Fatalf("duplicate srp_id %d", id)
		}
		seen[id] = true
	}
}
