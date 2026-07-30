package peerhash_test

import (
	"testing"

	"github.com/adambenhassen/telegram-server/internal/peerhash"
)

// master keys used across the table. They are test material only.
var (
	masterA = []byte("0123456789abcdef0123456789abcdef")
	masterB = []byte("fedcba9876543210fedcba9876543210")
)

func deriver(t *testing.T, master []byte) *peerhash.Deriver {
	t.Helper()
	sub, err := peerhash.Subkey(master)
	if err != nil {
		t.Fatalf("Subkey: %v", err)
	}
	d, err := peerhash.New(sub)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d
}

// TestDeriveStable pins the construction to a fixed vector. A change to the
// label, the field order, the field widths or the truncation moves this number,
// which is exactly the point: a hash issued by one process must verify in the
// next one, so the bytes are frozen, not merely deterministic within a run.
func TestDeriveStable(t *testing.T) {
	t.Parallel()

	const want = 3238409131046424869

	d := deriver(t, masterA)
	if got := d.Derive(1001, peerhash.KindUser, 1002); got != want {
		t.Fatalf("Derive(1001, user, 1002) = %d, want %d", got, want)
	}
	// Same inputs through an independently constructed Deriver: the value is a
	// function of the key material alone, with no per-process state.
	if got := deriver(t, masterA).Derive(1001, peerhash.KindUser, 1002); got != want {
		t.Fatalf("second Deriver = %d, want %d", got, want)
	}
}

func TestDeriveKeyDependent(t *testing.T) {
	t.Parallel()

	a := deriver(t, masterA).Derive(1001, peerhash.KindUser, 1002)
	b := deriver(t, masterB).Derive(1001, peerhash.KindUser, 1002)
	if a == b {
		t.Fatalf("different master key material produced the same hash %d", a)
	}
}

// TestDerivePerPair is the test that pins the security property: the hash is
// keyed by the (viewer, peer) pair, so a value leaked from one account's dialog
// list is inert in another account's hands. A per-peer implementation passes
// every other test here and fails this one.
func TestDerivePerPair(t *testing.T) {
	t.Parallel()

	d := deriver(t, masterA)
	ab := d.Derive(1001, peerhash.KindUser, 1002)
	cb := d.Derive(1003, peerhash.KindUser, 1002)
	if ab == cb {
		t.Fatalf("(A,B) and (C,B) both derived %d; hash is per-peer, not per-pair", ab)
	}
}

// TestDeriveKindSeparated covers the collision that is routine rather than
// theoretical: user ids and channel ids come from separate sequences, so the
// same numeric value names two different peers.
func TestDeriveKindSeparated(t *testing.T) {
	t.Parallel()

	d := deriver(t, masterA)
	kinds := []peerhash.Kind{peerhash.KindUser, peerhash.KindChat, peerhash.KindChannel}
	seen := make(map[int64]peerhash.Kind, len(kinds))
	for _, k := range kinds {
		h := d.Derive(1001, k, 7)
		if prev, dup := seen[h]; dup {
			t.Fatalf("kind %d and kind %d both derived %d for peer 7", prev, k, h)
		}
		seen[h] = k
	}
}

// TestDeriveSelfPair covers the caller's own user, which is rendered with an
// access_hash like any other peer and must not be a special case.
func TestDeriveSelfPair(t *testing.T) {
	t.Parallel()

	d := deriver(t, masterA)
	self := d.Derive(1001, peerhash.KindUser, 1001)
	if self == 0 {
		t.Fatal("self pair derived 0")
	}
	if other := d.Derive(1001, peerhash.KindUser, 1002); self == other {
		t.Fatalf("self pair and (1001,1002) both derived %d", self)
	}
	if fromOther := d.Derive(1002, peerhash.KindUser, 1001); self == fromOther {
		t.Fatalf("(1001,1001) and (1002,1001) both derived %d", self)
	}
}

func TestSubkeyRejectsShortMaster(t *testing.T) {
	t.Parallel()

	if _, err := peerhash.Subkey(masterA[:31]); err == nil {
		t.Fatal("Subkey accepted a 31-byte master key")
	}
}

// TestSubkeyIndependent checks the subkey is not the master key handed on under
// another name: only the subkey may reach the RPC layer, so a caller holding it
// must not be holding the storage key.
func TestSubkeyIndependent(t *testing.T) {
	t.Parallel()

	sub, err := peerhash.Subkey(masterA)
	if err != nil {
		t.Fatalf("Subkey: %v", err)
	}
	if string(sub) == string(masterA) {
		t.Fatal("Subkey returned the master key unchanged")
	}
	again, err := peerhash.Subkey(masterA)
	if err != nil {
		t.Fatalf("Subkey: %v", err)
	}
	if string(sub) != string(again) {
		t.Fatal("Subkey is not deterministic")
	}
}

func TestNewRejectsShortSubkey(t *testing.T) {
	t.Parallel()

	if _, err := peerhash.New(make([]byte, peerhash.SubkeyLen-1)); err == nil {
		t.Fatal("New accepted a short subkey")
	}
}
