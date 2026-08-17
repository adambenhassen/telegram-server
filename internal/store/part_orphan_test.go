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

	// Age the object past the part TTL plus margin.
	_, dir := localBlobsOf(t, s)
	past := time.Now().Add(-7 * time.Hour) // 6h TTL + 1h margin
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// Run the age pass with a cutoff of now-6h (the part TTL).
	cutoff := time.Now().Add(-6 * time.Hour)
	res, err := s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 100)
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

	// Age the object well past the cutoff.
	_, dir := localBlobsOf(t, s)
	past := time.Now().Add(-48 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// Run the pass with a cutoff that would reclaim it if the row check
	// were absent.
	cutoff := time.Now().Add(-6 * time.Hour)
	res, err := s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 100)
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
	past := time.Now().Add(-1 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// A misconfigured cutoff of now-30m would reclaim a 1h-old object if the
	// floor (TTL + margin = 7h) were not enforced.
	cutoff := time.Now().Add(-30 * time.Minute)
	res, err := s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 100)
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
	past := time.Now().Add(-72 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(assembledKey)), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	cutoff := time.Now().Add(-6 * time.Hour)
	res, err := s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 100)
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
		past := time.Now().Add(-12 * time.Hour)
		if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), past, past); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
	}

	// Bound the pass to 2 objects per run.
	cutoff := time.Now().Add(-6 * time.Hour)
	res, err := s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 2)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 2 {
		t.Fatalf("reclaimed %d objects, want 2 (bounded)", res.Objects)
	}

	// A second run reclaims the rest.
	res, err = s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 2)
	if err != nil {
		t.Fatalf("reclaim 2: %v", err)
	}
	if res.Objects != 2 {
		t.Fatalf("second run reclaimed %d objects, want 2", res.Objects)
	}

	// Third run: only 1 left.
	res, err = s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 2)
	if err != nil {
		t.Fatalf("reclaim 3: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("third run reclaimed %d objects, want 1", res.Objects)
	}
	_ = u
}

// TestPartOrphanPassNoStorageInTransaction is criterion 8: no storage call
// happens inside a transaction holding row or advisory locks. This is a
// structural guarantee verified by reading the implementation, but the test
// verifies the observable contract: the pass works correctly even when the
// database is under load (simulated by a concurrent save).
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
	past := time.Now().Add(-12 * time.Hour)
	if err := os.Chtimes(filepath.Join(dir, filepath.FromSlash(key)), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	// Run the pass concurrently with a save on a different part.
	cutoff := time.Now().Add(-6 * time.Hour)
	done := make(chan struct{})
	go func() {
		if err := s.SaveUploadPart(ctx, u.ID, 59, 0, part('n', 256), maxFile); err != nil {
			t.Errorf("concurrent save: %v", err)
		}
		close(done)
	}()
	res, err := s.ReclaimOrphanedPartBytes(ctx, cutoff, 6*time.Hour, 10)
	<-done
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if res.Objects != 1 {
		t.Fatalf("reclaimed %d objects, want 1", res.Objects)
	}
}
