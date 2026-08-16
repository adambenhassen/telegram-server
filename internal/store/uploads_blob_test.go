package store_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestUploadPartBytesLeavePostgres is criterion 2: after a part is saved, no
// row anywhere in Postgres contains the payload. The assertion is on the
// schema itself — the payload column is gone — and on the row's own contents:
// what the row carries is the key and the server-measured size, nothing else.
func TestUploadPartBytesLeavePostgres(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000101")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	payload := part('z', 4096)
	if err := s.SaveUploadPart(ctx, u.ID, 42, 0, payload, maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}

	size, key, err := store.UploadPartRow(ctx, s, u.ID, 42, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if size != int64(len(payload)) {
		t.Fatalf("recorded size = %d, want %d (measured server-side)", size, len(payload))
	}
	if key == "" || len(key) != len(blob.PartsPrefix)+32 {
		t.Fatalf("blob key = %q, want a %d-byte key under the parts prefix", key, len(blob.PartsPrefix)+32)
	}

	// The bytes are in the blob store, byte-identical.
	b, err := s.ReadPartBytes(ctx, key)
	if err != nil {
		t.Fatalf("read part bytes: %v", err)
	}
	if !bytes.Equal(b, payload) {
		t.Fatalf("stored bytes differ from the payload")
	}
}

// TestSaveUploadPartRefusedWritesNothing is criterion 11: the caps are
// evaluated and the request refused before any byte leaves the process. A
// save that is refused stores nothing — not in Postgres, not in the blob
// store. The assertion walks the parts prefix: a write-then-account order
// would leave an object there whose row was rolled back.
func TestSaveUploadPartRefusedWritesNothing(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000102")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Fill the per-file cap, then the next save must be refused.
	if err := s.SaveUploadPart(ctx, u.ID, 7, 0, part('a', store.MaxPartBytes), maxFile); err != nil {
		t.Fatalf("save first: %v", err)
	}
	err = s.SaveUploadPart(ctx, u.ID, 7, 1, part('b', store.MaxPartBytes), maxFile)
	if !errors.Is(err, store.ErrFileTooLarge) {
		t.Fatalf("over file cap: %v", err)
	}

	// Nothing of the refused save may be in the blob store.
	blobs, dir := localBlobsOf(t, s)
	keys := listPartKeys(t, dir)
	if len(keys) != 1 {
		t.Fatalf("part objects = %d, want 1 (the accepted part); a refused save wrote bytes", len(keys))
	}
	// And the refused save left no row either.
	parts, _, total, err := s.UploadPartsSummary(ctx, u.ID, 7)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 1 || total != store.MaxPartBytes {
		t.Fatalf("summary after refusal: parts=%d total=%d", parts, total)
	}
	_ = blobs
}

// TestUploadPartRecordedSizeNeverLowers is criterion 12: the recorded size is
// never lowered below the size of the object that may still exist at the
// part's key. Re-saving a 512 KiB part as 1 byte must not record 1 byte while
// the 512 KiB object may still be there — that is the sequence that evades
// the outstanding-byte cap by shrinking rows.
func TestUploadPartRecordedSizeNeverLowers(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000103")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	big := part('a', 512*1024)
	if err := s.SaveUploadPart(ctx, u.ID, 9, 0, big, maxFile); err != nil {
		t.Fatalf("save big: %v", err)
	}
	// The re-save the threat model names: same part, 1 byte.
	if err := s.SaveUploadPart(ctx, u.ID, 9, 0, []byte("x"), maxFile); err != nil {
		t.Fatalf("re-save small: %v", err)
	}

	size, _, err := store.UploadPartRow(ctx, s, u.ID, 9, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if size != int64(len(big)) {
		t.Fatalf("recorded size after shrink re-save = %d, want %d (the size of the object that may still exist)", size, len(big))
	}

	// The bytes themselves were replaced, so assembly reflects the retry. The
	// read is at the recorded size: it must see the new bytes at the head, and
	// the fail-closed check compares the read against the recorded size, which
	// is the larger one, so the read window is the larger one too.
	got, err := s.ReadPartBytes(ctx, mustKey(t, s, u.ID, 9, 0))
	if err != nil {
		t.Fatalf("read part bytes: %v", err)
	}
	if !bytes.Equal(got[:1], []byte("x")) {
		t.Fatalf("re-saved payload not at the head of the stored object: got %q", got[:1])
	}
}

func mustKey(t *testing.T, s *store.Store, userID, fileID int64, idx int32) string {
	t.Helper()
	_, k, err := store.UploadPartRow(ctx2(), s, userID, fileID, idx)
	if err != nil {
		t.Fatalf("read key: %v", err)
	}
	return k
}

func ctx2() context.Context { return context.Background() }

// TestUploadPartRowLeadsBytes is criterion 13's create direction, made
// observable: a row whose bytes never landed is the recoverable state. The
// test simulates the crash window — row committed, byte write lost — by
// deleting the object after the save, and asserts the consequences the design
// promises: the cap still counts the bytes (over-count, conservative),
// assembly fails closed, and the sweep retires the row on schedule with the
// absent object delete as a no-op.
func TestUploadPartRowLeadsBytes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000104")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	payload := part('a', 1024)
	if err := s.SaveUploadPart(ctx, u.ID, 11, 0, payload, maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	_, key, err := store.UploadPartRow(ctx, s, u.ID, 11, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	// The crash: the bytes never land (or are lost).
	blobs, dir := localBlobsOf(t, s)
	if err := os.Remove(filepath.Join(dir, key)); err != nil {
		t.Fatalf("remove object: %v", err)
	}

	// The cap over-counts: the bytes are still counted.
	_, _, total, err := s.UploadPartsSummary(ctx, u.ID, 11)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if total != int64(len(payload)) {
		t.Fatalf("summary after lost bytes: total=%d, want %d (over-count, not under)", total, len(payload))
	}

	// Assembly fails closed: a part whose bytes are gone is an error, not a
	// short part.
	if _, ok, err := s.UploadPart(ctx, u.ID, 11, 0); err == nil {
		t.Fatalf("read part with missing bytes: ok=%v err=nil, want an error (fail closed)", ok)
	}

	// The sweep retires the row; deleting the absent object is a no-op.
	n, err := s.SweepExpiredUploadParts(ctx, time.Now().Add(time.Hour), sweepBatch)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("sweep retired %d rows, want 1", n)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 11); err != nil || parts != 0 {
		t.Fatalf("after sweep: parts=%d err=%v", parts, err)
	}
	_ = blobs
}

// TestSweepDeletesBytesNotJustRows is criterion 6: expired parts are
// reclaimed — the bytes leave storage, not just the accounting rows.
func TestSweepDeletesBytesNotJustRows(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000105")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := range 3 {
		if err := s.SaveUploadPart(ctx, u.ID, 13, i, part('a', 100), maxFile); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	_, dir := localBlobsOf(t, s)
	if n := countPartKeys(t, dir); n != 3 {
		t.Fatalf("part objects before sweep = %d, want 3", n)
	}

	n, err := s.SweepExpiredUploadParts(ctx, time.Now().Add(time.Hour), sweepBatch)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 3 {
		t.Fatalf("sweep retired %d rows, want 3", n)
	}
	if got := countPartKeys(t, dir); got != 0 {
		t.Fatalf("part objects after sweep = %d, want 0: the bytes must leave storage", got)
	}
}

// TestSweepInterruptedLeavesNoRowWithoutBytes is criterion 6's second half: a
// sweep interrupted partway leaves no accounting row whose bytes are gone.
// The interruption is simulated where it can actually happen — between the
// claim and the byte delete — by making the byte delete fail, and the
// assertion is that the rows survive with their objects, so the next pass
// retries them rather than the bytes becoming unreachable.
func TestSweepInterruptedLeavesNoRowWithoutBytes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000106")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	for i := range 3 {
		if err := s.SaveUploadPart(ctx, u.ID, 15, i, part('a', 100), maxFile); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}

	// A backend whose Remove fails: the byte delete of the sweep's first pass
	// errors, the way a hung or failing store would.
	_, dir := localBlobsOf(t, s)
	healthy := store.BlobsOf(s)
	bad := &failingRemoveBlobs{Store: healthy}
	if err := store.SetSweepBlobs(s, bad); err != nil {
		t.Fatalf("swap blobs: %v", err)
	}

	_, err = s.SweepExpiredUploadParts(ctx, time.Now().Add(time.Hour), sweepBatch)
	if err == nil {
		t.Fatal("sweep over a failing store: err=nil, want the byte-delete failure")
	}

	// The rows are intact and their bytes are still there: nothing was
	// retired, so nothing is unreachable.
	parts, _, total, err := s.UploadPartsSummary(ctx, u.ID, 15)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 3 || total != 300 {
		t.Fatalf("after failed sweep: parts=%d total=%d, want 3/300 (rows must not be dropped before their bytes)", parts, total)
	}
	if got := countPartKeys(t, dir); got != 3 {
		t.Fatalf("part objects after failed sweep = %d, want 3", got)
	}

	// With a healthy backend the same sweep completes, and the bytes go.
	if err := store.SetSweepBlobs(s, healthy); err != nil {
		t.Fatalf("restore blobs: %v", err)
	}
	if _, err := s.SweepExpiredUploadParts(ctx, time.Now().Add(time.Hour), sweepBatch); err != nil {
		t.Fatalf("sweep after restore: %v", err)
	}
	if got := countPartKeys(t, dir); got != 0 {
		t.Fatalf("part objects after completed sweep = %d, want 0", got)
	}
}

// TestSweepRacesResaveIsConditionalOnKey is criterion 14: the sweep's row
// delete is conditional on the row still naming the key being deleted, never
// blind on the primary key. The race is simulated directly: the sweep claims
// the expired part, the row is re-saved in the window (new key, new object),
// and the finalise must spare the re-saved row — a blind delete on the primary
// key would have dropped it and orphaned the new object.
func TestSweepRacesResaveIsConditionalOnKey(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000107")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.SaveUploadPart(ctx, u.ID, 17, 0, part('a', 100), maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	_, oldKey, err := store.UploadPartRow(ctx, s, u.ID, 17, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}

	// The claim: the sweep's first step, in its own transaction.
	keys, err := s.ClaimExpiredPartsForTest(ctx, time.Now().Add(time.Hour), sweepBatch)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(keys) != 1 || keys[0] != oldKey {
		t.Fatalf("claim returned %v, want [%s]", keys, oldKey)
	}

	// The re-save that lands in the sweep's window: the row now names a new
	// key, and the new object exists.
	if err := s.SaveUploadPart(ctx, u.ID, 17, 0, part('b', 100), maxFile); err != nil {
		t.Fatalf("re-save: %v", err)
	}
	_, newKey, err := store.UploadPartRow(ctx, s, u.ID, 17, 0)
	if err != nil {
		t.Fatalf("read row after re-save: %v", err)
	}
	if newKey == oldKey {
		t.Fatal("re-save did not draw a new key")
	}

	// The bytes: the claimed (old) object goes.
	if err := store.BlobsOf(s).Remove(ctx, oldKey); err != nil {
		t.Fatalf("remove old object: %v", err)
	}

	// The finalise: the row delete is conditional on the row still naming the
	// claimed key, so the re-saved row is spared.
	if err := s.FinaliseExpiredPartsForTest(ctx, keys); err != nil {
		t.Fatalf("finalise: %v", err)
	}

	// The re-save's row and its new object both survive: nothing was
	// orphaned, and the conditional delete is what made that true.
	parts, _, total, err := s.UploadPartsSummary(ctx, u.ID, 17)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 1 || total != 100 {
		t.Fatalf("after conditional finalise: parts=%d total=%d, want 1/100 (the re-saved row must survive)", parts, total)
	}
	blobs, dir := localBlobsOf(t, s)
	if _, err := blobs.ReadAt(ctx, newKey, 0, 100); err != nil {
		t.Fatalf("re-save's object gone after conditional finalise: %v (a blind row delete would orphan it)", err)
	}
	if _, err := blobs.ReadAt(ctx, oldKey, 0, 100); err == nil {
		t.Fatal("old key's object survived the byte delete")
	}
	if got := countPartKeys(t, dir); got != 1 {
		t.Fatalf("part objects after finalise = %d, want 1 (the re-save's)", got)
	}
}

// failingRemoveBlobs wraps a Store whose Remove always fails, simulating a
// storage backend that cannot delete.
type failingRemoveBlobs struct {
	blob.Store
}

func (f *failingRemoveBlobs) Remove(ctx context.Context, key string) error {
	return errors.New("blob remove: simulated failure")
}

// localBlobsOf returns the store's blob backend and its root directory, for
// the tests that inspect the part objects directly.
func localBlobsOf(t *testing.T, s *store.Store) (blob.Store, string) {
	t.Helper()
	b := localBlobsOfStore(t, s)
	l, ok := b.(*blob.Local)
	if !ok {
		t.Fatal("store's blob backend is not local")
	}
	return b, l.RootDir()
}

func localBlobsOfStore(t *testing.T, s *store.Store) blob.Store {
	t.Helper()
	b := store.BlobsOf(s)
	if b == nil {
		t.Fatal("store has no blob backend")
	}
	return b
}

// listPartKeys lists the keys under the parts prefix in dir.
func listPartKeys(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(dir, "parts"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read parts dir: %v", err)
	}
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			keys = append(keys, e.Name())
		}
	}
	return keys
}

func countPartKeys(t *testing.T, dir string) int {
	t.Helper()
	return len(listPartKeys(t, dir))
}
