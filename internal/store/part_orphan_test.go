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

// partKey returns the nth key NewPartKey could have produced. The pass acts on
// part keys and nothing else, so a fixture that is merely under the prefix
// would test the wrong predicate. Zero padding also makes the numeric order
// the key order, which is the order the walk yields them in.
func partKey(n uint64) string {
	return blob.PartsPrefix + fmt.Sprintf("%032x", n)
}

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

	key := partKey(0xdeadbeef)
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

	key := partKey(0x11ffe0)
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

	key := partKey(0x40c)
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
		key := partKey(uint64(i))
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}
	orphan := partKey(1 << 40)
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
		key := partKey(uint64(i))
		if !objectExists(t, l.RootDir(), key) {
			t.Fatalf("ineligible %s gone", key)
		}
	}
}

// TestPartOrphanPassGateSpansBatchBoundary asserts the batched live-key gate
// handles candidates that fall on different sides of a batch boundary. The
// stream fills one whole batch with orphans, so the gate flushes mid-walk, and
// the live-row object lands in the batch after that flush. A gate that only
// ever ran once, or one that dropped what the flush left behind, fails here.
func TestPartOrphanPassGateSpansBatchBoundary(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	// Candidates are what the batch carries — the age and shape gates run
	// before it — so the boundary is only crossed by more than a batch of
	// genuinely eligible objects.
	const orphans = store.PartOrphanGateBatch
	old := time.Now().Add(-24 * time.Hour)
	for i := range orphans {
		key := partKey(uint64(i))
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
		ageObject(t, l.RootDir(), key, old)
	}

	// Sorts after every orphan above, so it is examined in the second batch.
	liveKey := partKey(1 << 40)
	if _, err := l.Put(ctx, liveKey, strings.NewReader("l")); err != nil {
		t.Fatalf("put live: %v", err)
	}
	ageObject(t, l.RootDir(), liveKey, old)
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
	if res.Objects != orphans {
		t.Fatalf("reclaimed %d objects, want %d (every orphan): %+v", res.Objects, orphans, res)
	}
	if !objectExists(t, l.RootDir(), liveKey) {
		t.Fatal("live-row object gone")
	}
	for i := range orphans {
		if objectExists(t, l.RootDir(), partKey(uint64(i))) {
			t.Fatalf("orphan %s still present", partKey(uint64(i)))
		}
	}
}

// writeTemp plants a writer temporary file for key with the given age.
func writeTemp(t *testing.T, l *blob.Local, key string, at time.Time) string {
	t.Helper()
	temp := key + blob.TempSuffix
	p := filepath.Join(l.RootDir(), filepath.FromSlash(temp))
	if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
		t.Fatalf("mkdir parts: %v", err)
	}
	if err := os.WriteFile(p, []byte("half a part"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	ageObject(t, l.RootDir(), temp, at)
	return temp
}

// TestPartOrphanPassKeepsWriteInProgress asserts the pass never unlinks a write
// that is still running. Local.Put writes to key+TempSuffix and renames it into
// place, so a fresh temporary file is bytes a caller is handing over right now,
// and no row ever names it: the cutoff is the only thing between it and a
// delete, which is why it is a test and not a comment.
func TestPartOrphanPassKeepsWriteInProgress(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	temp := writeTemp(t, l, partKey(0x7e77), time.Now())

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %+v, want nothing (write in progress)", res)
	}
	if !objectExists(t, l.RootDir(), temp) {
		t.Fatal("in-flight temporary file deleted")
	}
}

// TestPartOrphanPassReclaimsAbandonedTemporary is the other direction. A part
// is at most MaxPartBytes, so a Put that started before the cutoff is not still
// running: the temporary is a write that died, no row will ever name it, and
// nothing else reclaims it. Skipping it would leave exactly the leak this pass
// exists to close.
func TestPartOrphanPassReclaimsAbandonedTemporary(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	temp := writeTemp(t, l, partKey(0x7e78), time.Now().Add(-72*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 || res.Bytes != 11 {
		t.Fatalf("reclaimed %+v, want 1 object / 11 bytes (the abandoned temporary)", res)
	}
	if objectExists(t, l.RootDir(), temp) {
		t.Fatal("abandoned temporary still present")
	}
}

// TestPartOrphanPassKeepsTemporaryOfLiveRow asserts a temporary is gated on the
// key it is becoming, not on its own path. No row ever names a ".tmp" path, so
// reading the path would make the row gate vacuous for temporaries; reading the
// stripped key keeps AC 3 true for them — an object a row still names is never
// removed, and the writer's file for that object is part of it.
func TestPartOrphanPassKeepsTemporaryOfLiveRow(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	key := partKey(0x7e79)
	temp := writeTemp(t, l, key, time.Now().Add(-72*time.Hour))
	u, err := s.CreateUser(ctx, "+15551230014")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := store.InsertUploadPartWithKey(ctx, s, u.ID, 11, 0, 1, key); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %+v, want nothing (a row still names the key)", res)
	}
	if !objectExists(t, l.RootDir(), temp) {
		t.Fatal("temporary of a live row deleted")
	}
}

// TestPartOrphanPassLeavesUnexplainedPaths asserts the shape gate. The pass
// pages candidates by walking paths rather than by reading rows, so what
// separates a part object from anything else parked under the prefix decides
// what gets deleted. A path NewPartKey could not have produced is not part
// bytes, is not this pass's to reclaim, and is left for blobscan to report.
func TestPartOrphanPassLeavesUnexplainedPaths(t *testing.T) {
	t.Parallel()
	s, l := openOrphanStore(t)
	ctx := context.Background()

	// One real orphan, so an empty result cannot make this pass vacuously.
	orphan := partKey(0xbeef)
	if _, err := l.Put(ctx, orphan, strings.NewReader("o")); err != nil {
		t.Fatalf("put orphan: %v", err)
	}
	ageObject(t, l.RootDir(), orphan, time.Now().Add(-24*time.Hour))

	// Paths under the prefix the writer cannot have produced: a short name, an
	// over-long one, upper-case hex, and a file nested a level down.
	unexplained := []string{
		blob.PartsPrefix + "README",
		blob.PartsPrefix + strings.Repeat("a", 33),
		blob.PartsPrefix + strings.ToUpper(strings.Repeat("ab", 16)),
		blob.PartsPrefix + "sub/" + strings.Repeat("c", 32),
		// The temporary branch is not a way around the shape check: stripping
		// the suffix has to leave a key the writer could have produced.
		blob.PartsPrefix + "README" + blob.TempSuffix,
	}
	for _, key := range unexplained {
		p := filepath.Join(l.RootDir(), filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(p), 0o700); err != nil {
			t.Fatalf("mkdir %s: %v", key, err)
		}
		if err := os.WriteFile(p, []byte("not ours"), 0o600); err != nil {
			t.Fatalf("write %s: %v", key, err)
		}
		ageObject(t, l.RootDir(), key, time.Now().Add(-72*time.Hour))
	}

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("reclaimed %d objects, want 1 (the orphan alone): %+v", res.Objects, res)
	}
	for _, key := range unexplained {
		if !objectExists(t, l.RootDir(), key) {
			t.Fatalf("unexplained path %s deleted", key)
		}
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
	key := partKey(0x9a17)
	if _, err := l.Put(ctx, key, strings.NewReader("p")); err != nil {
		t.Fatalf("put part: %v", err)
	}
	ageObject(t, l.RootDir(), key, time.Now().Add(-24*time.Hour))

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
