package store_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// testTTL is the part TTL the orphan tests run against. The floor is
// TTL + PartOrphanMargin(TTL) = TTL + TTL/4 + 2s, so objects must be aged
// past that to be reclaimable.
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

// TestPartOrphanPassReclaimsCrashWindowBytes is criterion 4: an object whose
// row was committed and then swept, leaving bytes no row names, is reclaimed
// by the age pass. The test simulates the MAIN-341 crash window: save a part
// (row + bytes), then delete the row directly (as the TTL sweep would) while
// leaving the object behind, age it past the cutoff, and run the pass.
func TestPartOrphanPassReclaimsCrashWindowBytes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000201")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Save a part: row committed, bytes written.
	payload := part('x', 4096)
	if err := s.SaveUploadPart(ctx, u.ID, 55, 0, payload, maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	_, key, err := store.UploadPartRow(ctx, s, u.ID, 55, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if key == "" {
		t.Fatal("part has no key")
	}

	// Simulate the crash window: the row is swept (deleted) but the object
	// write was lost or the delete of the object failed. Delete the row
	// directly, leaving the object orphaned.
	if err := store.DeleteUploadPartRow(ctx, s, u.ID, 55, 0); err != nil {
		t.Fatalf("delete row: %v", err)
	}

	// Age the object past the floor (TTL + margin).
	_, dir := localBlobsOf(t, s)
	ageObject(t, dir, key, time.Now().Add(-12*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("reclaimed %d objects, want 1", res.Objects)
	}
	if res.Bytes != int64(len(payload)) {
		t.Fatalf("reclaimed %d bytes, want %d", res.Bytes, int64(len(payload)))
	}

	// The object is gone.
	b, err := s.ReadPartBytes(ctx, key)
	if err == nil && len(b) > 0 {
		t.Fatalf("orphaned object still present, %d bytes", len(b))
	}
}

// TestPartOrphanPassSavesLiveRow is criterion 3: an object whose accounting
// row still exists and still names it is never removed, whatever its age.
func TestPartOrphanPassSavesLiveRow(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000202")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := s.SaveUploadPart(ctx, u.ID, 56, 0, part('y', 2048), maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	_, key, err := store.UploadPartRow(ctx, s, u.ID, 56, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}

	// Age the object well past the floor.
	_, dir := localBlobsOf(t, s)
	ageObject(t, dir, key, time.Now().Add(-48*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %d objects, want 0 (live row protects)", res.Objects)
	}

	// The object is still there.
	b, err := s.ReadPartBytes(ctx, key)
	if err != nil {
		t.Fatalf("object gone: %v", err)
	}
	if len(b) != 2048 {
		t.Fatalf("object size %d, want 2048", len(b))
	}
}

// TestPartOrphanPassRespectsFloor is criterion 2: the cutoff floor is the
// part TTL plus margin. An object younger than the floor is never removed,
// even with a misconfigured small cutoff.
func TestPartOrphanPassRespectsFloor(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000203")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Save and orphan a part that is only 1 hour old.
	if err := s.SaveUploadPart(ctx, u.ID, 57, 0, part('z', 1024), maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	_, key, err := store.UploadPartRow(ctx, s, u.ID, 57, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if err := store.DeleteUploadPartRow(ctx, s, u.ID, 57, 0); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	_, dir := localBlobsOf(t, s)
	ageObject(t, dir, key, time.Now().Add(-time.Hour))

	// A misconfigured cutoff of now-30m would reclaim a 1h-old object if the
	// floor were not enforced.
	cutoff := time.Now().Add(-30 * time.Minute)
	res, err := s.ReclaimOrphanedPartBytes(ctx, cutoff, testTTL, 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %d objects, want 0 (floor protects)", res.Objects)
	}
}

// TestPartOrphanPassDoesNotTouchAssembledPrefix is criterion 6: an object
// under the assembled-blob prefix survives, and is not enumerated at all.
func TestPartOrphanPassDoesNotTouchAssembledPrefix(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	// Plant an old object under the assembled prefix.
	_, dir := localBlobsOf(t, s)
	assembledKey := blob.Key(999)
	payload := []byte("assembled bytes")
	b := store.BlobsOf(s)
	if _, err := b.Put(ctx, assembledKey, bytes.NewReader(payload)); err != nil {
		t.Fatalf("put assembled: %v", err)
	}
	ageObject(t, dir, assembledKey, time.Now().Add(-72*time.Hour))

	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("reclaimed %d objects, want 0 (assembled prefix untouched)", res.Objects)
	}

	// The assembled object survives.
	got, err := b.ReadAt(ctx, assembledKey, 0, 1024)
	if err != nil {
		t.Fatalf("assembled object gone: %v", err)
	}
	if len(got) != len(payload) {
		t.Fatalf("assembled size %d, want %d", len(got), len(payload))
	}
}

// TestPartOrphanPassBounded is criterion 5: the pass is bounded per run.
func TestPartOrphanPassBounded(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000204")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Create 5 orphaned objects, all old.
	_, dir := localBlobsOf(t, s)
	b := store.BlobsOf(s)
	for range 5 {
		key, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("key: %v", err)
		}
		if _, err := b.Put(ctx, key, bytes.NewReader(part('o', 100))); err != nil {
			t.Fatalf("put: %v", err)
		}
		ageObject(t, dir, key, time.Now().Add(-12*time.Hour))
	}

	// Bound the pass to 2 objects per run.
	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, 2)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 2 {
		t.Fatalf("reclaimed %d objects, want 2 (bounded)", res.Objects)
	}

	// A second run reclaims the rest.
	res, err = s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, 2)
	if err != nil {
		t.Fatalf("reclaim 2: %v", err)
	}
	if res.Objects != 2 {
		t.Fatalf("second run reclaimed %d objects, want 2", res.Objects)
	}

	// Third run: only 1 left.
	res, err = s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, 2)
	if err != nil {
		t.Fatalf("reclaim 3: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("third run reclaimed %d objects, want 1", res.Objects)
	}
	_ = u
}

// TestPartOrphanPassCrossesPageBoundary crosses the PartOrphanPageSize
// boundary: PartOrphanPageSize+1 old orphans are all reclaimed over
// successive runs, proving the cursor drains pages rather than restarting
// at the same page every run.
func TestPartOrphanPassCrossesPageBoundary(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000206")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Plant PartOrphanPageSize+1 orphaned objects, all old.
	_, dir := localBlobsOf(t, s)
	b := store.BlobsOf(s)
	n := store.PartOrphanPageSize + 1
	for range n {
		key, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("key: %v", err)
		}
		if _, err := b.Put(ctx, key, bytes.NewReader(part('p', 64))); err != nil {
			t.Fatalf("put: %v", err)
		}
		ageObject(t, dir, key, time.Now().Add(-12*time.Hour))
	}

	// Run the pass with a budget of PartOrphanPageSize: it should reclaim
	// exactly that many, and the next run should get the remainder.
	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, store.PartOrphanPageSize)
	if err != nil {
		t.Fatalf("reclaim 1: %v", err)
	}
	if res.Objects != store.PartOrphanPageSize {
		t.Fatalf("first run reclaimed %d, want %d", res.Objects, store.PartOrphanPageSize)
	}

	res, err = s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, store.PartOrphanPageSize)
	if err != nil {
		t.Fatalf("reclaim 2: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("second run reclaimed %d, want 1", res.Objects)
	}

	// Third run: nothing left.
	res, err = s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, store.PartOrphanPageSize)
	if err != nil {
		t.Fatalf("reclaim 3: %v", err)
	}
	if res.Objects != 0 {
		t.Fatalf("third run reclaimed %d, want 0", res.Objects)
	}
	_ = u
}

// TestPartOrphanPassConcurrentWithSave verifies the pass works correctly
// even when a save is in flight on a different part.
func TestPartOrphanPassConcurrentWithSave(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000205")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Create an orphan.
	if err := s.SaveUploadPart(ctx, u.ID, 58, 0, part('c', 512), maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	_, key, err := store.UploadPartRow(ctx, s, u.ID, 58, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if err := store.DeleteUploadPartRow(ctx, s, u.ID, 58, 0); err != nil {
		t.Fatalf("delete row: %v", err)
	}
	_, dir := localBlobsOf(t, s)
	ageObject(t, dir, key, time.Now().Add(-12*time.Hour))

	// Run the pass concurrently with a save on a different part.
	done := make(chan struct{})
	go func() {
		if err := s.SaveUploadPart(ctx, u.ID, 59, 0, part('n', 256), maxFile); err != nil {
			t.Errorf("concurrent save: %v", err)
		}
		close(done)
	}()
	res, err := s.ReclaimOrphanedPartBytes(ctx, testCutoff(), testTTL, 10)
	<-done
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("reclaimed %d objects, want 1", res.Objects)
	}
}

// TestPartOrphanMarginScalesWithTTL verifies the margin derives from the
// sweep cadence: a longer TTL produces a larger margin, so the age gate
// stays independent of the live-key gate at any configured TTL.
func TestPartOrphanMarginScalesWithTTL(t *testing.T) {
	t.Parallel()
	// At the 6h default, the margin is 1h 1m 2s (6h/4 + 2s).
	m6 := store.PartOrphanMargin(6 * time.Hour)
	if m6 != 90*time.Minute+2*time.Second {
		t.Fatalf("margin(6h) = %v, want %v", m6, 90*time.Minute+2*time.Second)
	}
	// At 24h, the margin is 6h 2s.
	m24 := store.PartOrphanMargin(24 * time.Hour)
	if m24 != 6*time.Hour+2*time.Second {
		t.Fatalf("margin(24h) = %v, want %v", m24, 6*time.Hour+2*time.Second)
	}
	// The margin always exceeds the sweep lag (ttl/4).
	for _, ttl := range []time.Duration{time.Hour, 6 * time.Hour, 24 * time.Hour} {
		if store.PartOrphanMargin(ttl) <= ttl/4 {
			t.Fatalf("margin(%v) = %v does not exceed sweep lag %v", ttl, store.PartOrphanMargin(ttl), ttl/4)
		}
	}
}
