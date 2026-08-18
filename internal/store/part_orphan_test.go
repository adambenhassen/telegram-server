package store_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// testTTL is the part TTL the orphan tests run against.
const testTTL = 6 * time.Hour

// testCutoff returns a cutoff well past the floor for testTTL.
func testCutoff() time.Time {
	return time.Now().Add(-(testTTL + store.PartOrphanMargin(testTTL) + time.Hour))
}

// ageObject sets an object's mtime to the given time.
func ageObject(t *testing.T, dir, key string, at time.Time) {
	t.Helper()
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), at, at); err != nil {
		t.Fatalf("chtimes %s: %v", key, err)
	}
}

// objectExists reports whether the object is still in the store.
func objectExists(t *testing.T, dir, key string) bool {
	t.Helper()
	_, err := os.Stat(filepath.Join(dir, filepath.FromSlash(key)))
	return err == nil
}

// openOrphanStore opens a store with a local blob store and returns both, so
// the test can age objects directly.
func openOrphanStore(t *testing.T) (*store.Store, *blob.Local) {
	t.Helper()
	dir := t.TempDir()
	b, err := blob.NewLocal(dir)
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	s, err := store.Open(context.Background(), pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(b))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close: %v", err)
		}
	})
	return s, b
}

// TestPartOrphanPassReclaimsCrashWindowObject covers the crash window the
// row-driven sweep cannot see: the row was committed (and then deleted by the
// sweep) but the bytes were never uploaded. The object is old enough and no
// row names it, so the pass reclaims it.
func TestPartOrphanPassReclaimsCrashWindowObject(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	key := blob.PartsPrefix + "deadbeef"
	if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	ageObject(t, l.RootDir(), key, time.Now().Add(-24*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 || res.Bytes != 1 {
		t.Fatalf("reclaimed %+v, want 1 object / 1 byte", res)
	}
	if objectExists(t, l.RootDir(), key) {
		t.Fatal("object still present after pass")
	}
}

// TestPartOrphanPassKeepsLiveRowObject asserts the live-key gate: an object
// whose key a row still names is never removed, whatever its age.
func TestPartOrphanPassKeepsLiveRowObject(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	key := blob.PartsPrefix + "liverow"
	if _, err := l.Put(ctx, key, strings.NewReader("y")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if _, err := s.CreateUser(ctx, "+15551230010"); err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := store.InsertUploadPartWithKey(ctx, s, 1, 7, 0, 1, key); err != nil {
		t.Fatalf("insert row: %v", err)
	}
	ageObject(t, l.RootDir(), key, time.Now().Add(-24*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %+v, want nothing (live row)", res)
	}
	if !objectExists(t, l.RootDir(), key) {
		t.Fatal("live object gone")
	}
}

// TestPartOrphanPassFloorClampsCutoff asserts the floor: a cutoff younger
// than TTL+margin is clamped to the floor, so a misconfigured small cutoff
// cannot reach a live part.
func TestPartOrphanPassFloorClampsCutoff(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	key := blob.PartsPrefix + "youngish"
	if _, err := l.Put(ctx, key, strings.NewReader("z")); err != nil {
		t.Fatalf("put: %v", err)
	}
	// Age it to TTL + margin/2: past a naive TTL cutoff, but not past the
	// floor (TTL + margin).
	ageObject(t, l.RootDir(), key, time.Now().Add(-(testTTL + store.PartOrphanMargin(testTTL)/2)))

	res, err := s.ReclaimOrphanedPartBytes(ctx, time.Now().Add(-time.Minute), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %+v, want nothing (floor)", res)
	}
	if !objectExists(t, l.RootDir(), key) {
		t.Fatal("object gone despite floor")
	}
}

// TestPartOrphanPassIgnoresAssembledKeys asserts the pass only walks the
// parts prefix: an assembled object that is old and unaccounted is not
// touched.
func TestPartOrphanPassIgnoresAssembledKeys(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	key := blob.Key(4242)
	if _, err := l.Put(ctx, key, strings.NewReader("assembled")); err != nil {
		t.Fatalf("put: %v", err)
	}
	ageObject(t, l.RootDir(), key, time.Now().Add(-72*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %+v, want nothing (assembled key)", res)
	}
	if !objectExists(t, l.RootDir(), key) {
		t.Fatal("assembled object gone")
	}
}

// TestPartOrphanPassReachesWholePrefixInOneRun asserts that one run walks the
// entire parts prefix, so an orphan at the top of the keyspace is reclaimed
// even when ineligible objects precede it. This is the invariant the old
// budget-bound reach violated: reach was coupled to the delete budget, so an
// orphan behind a wall of ineligible objects was never examined.
func TestPartOrphanPassReachesWholePrefixInOneRun(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	// Plant 600 ineligible (young) objects in the low keyspace, then one
	// orphan above them. Under the old budget-bound reach with a 500-object
	// budget, the orphan at position 600 was never examined in one run.
	// Under the new contract, one run walks the whole prefix and reclaims it.
	for i := range 600 {
		key := blob.PartsPrefix + fmt.Sprintf("ineligible%03d", i)
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	orphan := blob.PartsPrefix + "zzz_orphan"
	if _, err := l.Put(ctx, orphan, strings.NewReader("orphan")); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	ageObject(t, l.RootDir(), orphan, time.Now().Add(-24*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("reclaimed %d objects, want 1 (the orphan): %+v", res.Objects, res)
	}
	if objectExists(t, l.RootDir(), orphan) {
		t.Fatal("orphan still present after pass")
	}
	// The ineligible objects must all survive.
	for i := range 600 {
		key := blob.PartsPrefix + fmt.Sprintf("ineligible%03d", i)
		if !objectExists(t, l.RootDir(), key) {
			t.Fatalf("ineligible %s gone", key)
		}
	}
}

// TestPartOrphanPassGateSpansBatchBoundary asserts the batched live-key gate
// handles keys that fall on different sides of a batch boundary: an old
// orphan in one batch and an old object with a live row in the next batch.
// Both are old enough to pass the age gate; only the orphan is reclaimed.
func TestPartOrphanPassGateSpansBatchBoundary(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	// Plant 500 young objects to fill the first batch, then one orphan and
	// one live-row object in the second batch. The orphan and the live-row
	// object are both old; only the orphan is unaccounted.
	for i := range 500 {
		key := blob.PartsPrefix + fmt.Sprintf("young%03d", i)
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	orphan := blob.PartsPrefix + "zzz_orphan"
	if _, err := l.Put(ctx, orphan, strings.NewReader("o")); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	ageObject(t, l.RootDir(), orphan, time.Now().Add(-24*time.Hour))

	liveKey := blob.PartsPrefix + "zzz_live"
	if _, err := l.Put(ctx, liveKey, strings.NewReader("l")); err != nil {
		t.Fatalf("put live: %v", err)
	}
	ageObject(t, l.RootDir(), liveKey, time.Now().Add(-24*time.Hour))
	u, err := s.CreateUser(ctx, "+15551230012")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := store.InsertUploadPartWithKey(ctx, s, u.ID, 9, 0, 1, liveKey); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("reclaimed %d objects, want 1 (the orphan): %+v", res.Objects, res)
	}
	if objectExists(t, l.RootDir(), orphan) {
		t.Fatal("orphan still present")
	}
	if !objectExists(t, l.RootDir(), liveKey) {
		t.Fatal("live-row object gone")
	}
}

// TestPartOrphanMarginScalesWithTTL asserts the margin scales with the
// configured TTL: a larger TTL yields a larger margin, so the age gate
// stays independent of the sweep cadence.
func TestPartOrphanMarginScalesWithTTL(t *testing.T) {
	t.Parallel()
	small := store.PartOrphanMargin(time.Hour)
	large := store.PartOrphanMargin(24 * time.Hour)
	if small >= large {
		t.Fatalf("margin(%v)=%v not less than margin(%v)=%v", time.Hour, small, 24*time.Hour, large)
	}
	// The margin must always include at least one hour of clock skew.
	if small < time.Hour {
		t.Fatalf("margin(%v)=%v below the 1h clock-skew floor", time.Hour, small)
	}
}

// TestPartOrphanPassSeparatorFreePrefixContainment asserts that the pass's
// enumeration is confined to the parts prefix: a separator-free prefix
// (a top-level shard such as "92") cannot return keys outside its scope.
// This is the fail-closed containment invariant: the pass must never widen
// to the store root.
func TestPartOrphanPassSeparatorFreePrefixContainment(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	// Plant an old orphan under the parts prefix and an old object under an
	// assembled shard. The pass must reclaim only the part.
	partKey := blob.PartsPrefix + "part_orphan"
	if _, err := l.Put(ctx, partKey, strings.NewReader("p")); err != nil {
		t.Fatalf("put part: %v", err)
	}
	ageObject(t, l.RootDir(), partKey, time.Now().Add(-24*time.Hour))

	shardKey := blob.Key(4242)
	if _, err := l.Put(ctx, shardKey, strings.NewReader("s")); err != nil {
		t.Fatalf("put shard: %v", err)
	}
	ageObject(t, l.RootDir(), shardKey, time.Now().Add(-24*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("reclaimed %d objects, want 1 (the part): %+v", res.Objects, res)
	}
	if !objectExists(t, l.RootDir(), shardKey) {
		t.Fatal("assembled shard object gone")
	}
}
