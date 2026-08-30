package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func plantBlobErasureObject(t *testing.T, s *store.Store, key, body string, at time.Time) {
	t.Helper()
	_, dir := localBlobsOf(t, s)
	name := filepath.Join(dir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		t.Fatalf("mkdir for %s: %v", key, err)
	}
	if err := os.WriteFile(name, []byte(body), 0o600); err != nil {
		t.Fatalf("plant %s: %v", key, err)
	}
	if !at.IsZero() {
		if err := os.Chtimes(name, at, at); err != nil {
			t.Fatalf("age %s: %v", key, err)
		}
	}
}

func blobErasureObjectExists(t *testing.T, s *store.Store, key string) bool {
	t.Helper()
	_, dir := localBlobsOf(t, s)
	_, err := os.Stat(filepath.Join(dir, filepath.FromSlash(key)))
	return err == nil
}

type prefixRecordingBlobs struct {
	blob.Store

	prefixes []string
}

func (p *prefixRecordingBlobs) WalkPrefix(ctx context.Context, prefix string, fn func(blob.Entry) error) error {
	p.prefixes = append(p.prefixes, prefix)
	return p.Store.WalkPrefix(ctx, prefix, fn)
}

type blobErasureWalkHook struct {
	blob.Store

	called bool
	before func()
}

type cancelAfterFirstRemoveBlobs struct {
	blob.Store

	cancel context.CancelFunc
	once   sync.Once
}

func (b *cancelAfterFirstRemoveBlobs) Remove(ctx context.Context, key string) error {
	if err := b.Store.Remove(ctx, key); err != nil {
		return err
	}
	b.once.Do(b.cancel)
	return nil
}

type serializedRemoveBlobs struct {
	blob.Store

	mu      sync.Mutex
	removed map[string]int
}

func (b *serializedRemoveBlobs) Remove(ctx context.Context, key string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, err := b.ReadAt(ctx, key, 0, 1); errors.Is(err, blob.ErrNotFound) {
		return nil
	} else if err != nil {
		return err
	}
	b.removed[key]++
	return b.Store.Remove(ctx, key)
}

func (w *blobErasureWalkHook) WalkPrefix(ctx context.Context, prefix string, fn func(blob.Entry) error) error {
	if !w.called {
		w.called = true
		w.before()
	}
	return w.Store.WalkPrefix(ctx, prefix, fn)
}

// The reporting path is safe by default: it classifies the crash-window
// orphan, an aged temporary, a fresh temporary and an unexplained path, but
// it does not remove any of them.
func TestBlobErasureSummaryIsReadOnlyAndReportsEachClass(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559201001")

	orphan := storedFileWithBytes(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, orphan.ID); err != nil {
		t.Fatalf("erase orphan row: %v", err)
	}

	oldTemp := storedFile(t, s, a.ID)
	oldTempKey := blob.Key(oldTemp.ID) + blob.TempSuffix
	plantBlobErasureObject(t, s, oldTempKey, "old temp", time.Now().Add(-2*time.Hour))
	youngTemp := storedFile(t, s, a.ID)
	youngTempKey := blob.Key(youngTemp.ID) + blob.TempSuffix
	plantBlobErasureObject(t, s, youngTempKey, "young temp", time.Time{})
	const unexplainedKey = "92/notanid.tmp"
	plantBlobErasureObject(t, s, unexplainedKey, "stray", time.Now().Add(-2*time.Hour))

	report, err := s.BlobErasureSummary(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("blob erasure summary: %v", err)
	}
	if report.Orphans != 1 || report.OrphanBytes != int64(len(blobBody)) {
		t.Fatalf("orphan report = %+v, want one orphan and %d bytes", report, len(blobBody))
	}
	if report.AbandonedTemps != 1 || report.AbandonedTempBytes != int64(len("old temp")) {
		t.Fatalf("temp report = %+v, want one abandoned temp", report)
	}
	if report.TempsInFlight != 1 {
		t.Fatalf("fresh temp report = %+v, want one in-flight temp", report)
	}
	if report.Unexplained != 1 || report.UnexplainedBytes != int64(len("stray")) {
		t.Fatalf("unexplained report = %+v, want one unexplained path", report)
	}
	if len(report.UnexplainedPaths) != 1 || report.UnexplainedPaths[0] != unexplainedKey {
		t.Fatalf("unexplained paths = %v, want %q", report.UnexplainedPaths, unexplainedKey)
	}
	if !blobPresent(t, s, orphan.ID) || !blobErasureObjectExists(t, s, oldTempKey) || !blobErasureObjectExists(t, s, unexplainedKey) {
		t.Fatal("read-only report removed a blob")
	}
	if strings.Contains(fmt.Sprintf("%+v", report), strconv.FormatInt(orphan.AccessHash, 10)) {
		t.Fatal("blob erasure report contains an access hash")
	}
}

// The destructive pass reclaims only the two owned classes. Parts are a
// separate keyspace, and a malformed temporary remains unexplained rather than
// becoming an id-based deletion candidate.
func TestBlobErasureSweepReclaimsOwnedClassesAndLeavesParts(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559201002")

	orphan := storedFileWithBytes(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, orphan.ID); err != nil {
		t.Fatalf("erase orphan row: %v", err)
	}
	accounted := storedFileWithBytes(t, s, a.ID)
	oldTemp := storedFile(t, s, a.ID)
	if err := store.SetFileUnstored(ctx, s, oldTemp.ID); err != nil {
		t.Fatalf("clear temporary's stored flag: %v", err)
	}
	if err := store.SetFileDate(ctx, s, oldTemp.ID, time.Now().Add(-2*time.Hour)); err != nil {
		t.Fatalf("age temporary's row: %v", err)
	}
	oldTempKey := blob.Key(oldTemp.ID) + blob.TempSuffix
	plantBlobErasureObject(t, s, oldTempKey, "old temp", time.Now().Add(-2*time.Hour))
	youngTemp := storedFile(t, s, a.ID)
	youngTempKey := blob.Key(youngTemp.ID) + blob.TempSuffix
	plantBlobErasureObject(t, s, youngTempKey, "young temp", time.Time{})
	const unexplainedKey = "92/notanid.tmp"
	plantBlobErasureObject(t, s, unexplainedKey, "stray", time.Now().Add(-2*time.Hour))
	if err := s.SaveUploadPart(ctx, a.ID, 17, 0, []byte("part"), maxFile); err != nil {
		t.Fatalf("save upload part: %v", err)
	}
	_, partKey, err := store.UploadPartRow(ctx, s, a.ID, 17, 0)
	if err != nil {
		t.Fatalf("read upload part: %v", err)
	}

	recorder := &prefixRecordingBlobs{Store: store.BlobsOf(s)}
	if err := store.SetPartBlobs(s, recorder); err != nil {
		t.Fatalf("record prefixes: %v", err)
	}
	defer func() {
		if err := store.SetPartBlobs(s, recorder.Store); err != nil {
			t.Errorf("restore blob backend: %v", err)
		}
	}()
	cutoff := time.Now().Add(-time.Hour)
	counts, err := s.SweepBlobErasure(ctx, cutoff, cutoff)
	if err != nil {
		t.Fatalf("blob erasure sweep: %v", err)
	}
	if err := store.SetPartBlobs(s, recorder.Store); err != nil {
		t.Fatalf("restore blob backend: %v", err)
	}
	if counts.OrphanUnlinkAttempts != 1 || counts.OrphanUnlinkAttemptedBytes != int64(len(blobBody)) {
		t.Fatalf("orphan erase counts = %+v, want one orphan and %d bytes", counts, len(blobBody))
	}
	if counts.TempUnlinkAttempts != 1 || counts.TempUnlinkAttemptedBytes != int64(len("old temp")) {
		t.Fatalf("temp erase counts = %+v, want one abandoned temp", counts)
	}
	if counts.TempInFlight != 1 || counts.Unexplained != 1 {
		t.Fatalf("held-back counts = %+v, want one fresh temp and one unexplained path", counts)
	}
	if counts.OrphanRetained != 1 || !blobPresent(t, s, accounted.ID) {
		t.Fatalf("accounted blob was reclaimed: counts = %+v", counts)
	}
	if rowPresent(t, s, oldTemp.ID) {
		t.Errorf("temporary cleanup left the absent files row for %d", oldTemp.ID)
	}
	if blobPresent(t, s, orphan.ID) || blobErasureObjectExists(t, s, oldTempKey) {
		t.Fatal("owned reclaimable bytes survived the destructive pass")
	}
	if !blobErasureObjectExists(t, s, youngTempKey) || !blobErasureObjectExists(t, s, unexplainedKey) {
		t.Fatal("destructive pass removed held-back bytes")
	}
	if _, err := store.BlobsOf(s).ReadAt(ctx, partKey, 0, 1); err != nil {
		t.Fatalf("parts object was touched by assembled reclaim: %v", err)
	}
	for _, prefix := range recorder.prefixes {
		if prefix == blob.PartsPrefix || prefix == "" {
			t.Fatalf("destructive pass escaped assembled shard prefixes with %q", prefix)
		}
	}
	if len(recorder.prefixes) != 2*(1<<8) {
		t.Fatalf("prefix walks = %d, want two complete assembled passes", len(recorder.prefixes))
	}
}

// The id ceiling is read before the first prefix listing. A blob written after
// that snapshot, even when its row is immediately gone, is above this pass's
// authority and survives for the next one.
func TestBlobErasureSweepLeavesBlobWrittenAfterSnapshot(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559201003")

	orphan := storedFileWithBytes(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, orphan.ID); err != nil {
		t.Fatalf("erase orphan row: %v", err)
	}
	original := store.BlobsOf(s)
	var written store.File
	hook := &blobErasureWalkHook{
		Store: original,
		before: func() {
			written = storedFileWithBytes(t, s, a.ID)
			if err := store.EraseFileRow(ctx, s, written.ID); err != nil {
				t.Fatalf("erase post-snapshot row: %v", err)
			}
		},
	}
	if err := store.SetPartBlobs(s, hook); err != nil {
		t.Fatalf("install walk hook: %v", err)
	}
	defer func() {
		if err := store.SetPartBlobs(s, original); err != nil {
			t.Errorf("restore blob backend: %v", err)
		}
	}()

	cutoff := time.Now().Add(-time.Hour)
	counts, err := s.SweepBlobErasure(ctx, cutoff, cutoff)
	if err != nil {
		t.Fatalf("blob erasure sweep: %v", err)
	}
	if counts.OrphanUnlinkAttempts != 1 || counts.AboveSnapshot != 1 {
		t.Fatalf("counts = %+v, want old orphan erased and one post-snapshot blob held back", counts)
	}
	if blobPresent(t, s, orphan.ID) {
		t.Errorf("pre-snapshot orphan %d survived", orphan.ID)
	}
	if !blobPresent(t, s, written.ID) {
		t.Errorf("blob %d written during the pass was removed", written.ID)
	}
}

// A crash after MAIN-336 commits the row delete but before its unlink leaves
// exactly the no-row object this pass owns. The follow-up pass reclaims it.
func TestBlobErasureSweepReclaimsInterruptedMediaUnlink(t *testing.T) {
	t.Parallel()
	s := open(t)
	a := mustUser(t, s, "+15559201004")
	b := mustUser(t, s, "+15559201005")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)
	original := store.BlobsOf(s)
	refused := errors.New("interrupted media unlink")
	probe := &probeBlobs{
		Store: original,
		onRemove: func(key string) error {
			if key == blob.Key(f.ID) {
				return refused
			}
			return nil
		},
	}
	if err := store.SetPartBlobs(s, probe); err != nil {
		t.Fatalf("install interrupted backend: %v", err)
	}
	mediaCounts, err := s.SweepMediaErasure(context.Background(), future(), store.ErasureScanBatch)
	if err == nil || !errors.Is(err, refused) {
		t.Fatalf("media sweep error = %v, want interrupted unlink", err)
	}
	if mediaCounts.UnlinkFailed != 1 || rowPresent(t, s, f.ID) || !blobPresent(t, s, f.ID) {
		t.Fatalf("interrupted media sweep left state = counts %+v row=%v blob=%v", mediaCounts, rowPresent(t, s, f.ID), blobPresent(t, s, f.ID))
	}
	if err := store.SetPartBlobs(s, original); err != nil {
		t.Fatalf("restore blob backend: %v", err)
	}

	cutoff := time.Now().Add(-time.Hour)
	blobCounts, err := s.SweepBlobErasure(context.Background(), cutoff, cutoff)
	if err != nil {
		t.Fatalf("blob erasure sweep: %v", err)
	}
	if blobCounts.OrphanUnlinkAttempts != 1 || blobPresent(t, s, f.ID) {
		t.Fatalf("orphan follow-up counts = %+v, blob still present=%v", blobCounts, blobPresent(t, s, f.ID))
	}
}

// Database failures are not unlink failures. Cancelling after the first
// successful remove makes the next candidate fail at transaction begin; only
// the first object was handed to the blob backend.
func TestBlobErasureSweepDoesNotCountDatabaseFailureAsUnlinkFailure(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := mustUser(t, s, "+15559201006")

	first := storedFileWithBytes(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, first.ID); err != nil {
		t.Fatalf("erase first row: %v", err)
	}
	second := storedFileWithBytes(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, second.ID); err != nil {
		t.Fatalf("erase second row: %v", err)
	}

	probe := &cancelAfterFirstRemoveBlobs{Store: store.BlobsOf(s), cancel: cancel}
	if err := store.SetPartBlobs(s, probe); err != nil {
		t.Fatalf("swap blobs: %v", err)
	}
	defer func() {
		if err := store.SetPartBlobs(s, probe.Store); err != nil {
			t.Errorf("restore blob backend: %v", err)
		}
	}()

	cutoff := time.Now().Add(-time.Hour)
	counts, err := s.SweepBlobErasure(ctx, cutoff, cutoff)
	if err == nil {
		t.Fatal("sweep reported no database error after cancellation")
	}
	if counts.OrphanUnlinkFailed != 0 {
		t.Fatalf("OrphanUnlinkFailed = %d, want 0 for a database error", counts.OrphanUnlinkFailed)
	}
	if counts.OrphanUnlinkAttempts != 1 {
		t.Fatalf("OrphanUnlinkAttempts = %d, want only the first object removed", counts.OrphanUnlinkAttempts)
	}
	if blobPresent(t, s, first.ID) == blobPresent(t, s, second.ID) {
		t.Fatalf("both orphan blobs have the same post-failure state: first=%v second=%v", blobPresent(t, s, first.ID), blobPresent(t, s, second.ID))
	}
}

// Committing the no-row decision before Remove makes a cancellation in the
// backend callback harmless to the database transaction. The current order
// removes the bytes first, then loses the commit and misreports that as an
// unlink failure.
func TestBlobErasureSweepCommitsNoRowDecisionBeforeRemove(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	a := mustUser(t, s, "+15559201007")
	f := storedFileWithBytes(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, f.ID); err != nil {
		t.Fatalf("erase row: %v", err)
	}

	probe := &cancelAfterFirstRemoveBlobs{Store: store.BlobsOf(s), cancel: cancel}
	if err := store.SetPartBlobs(s, probe); err != nil {
		t.Fatalf("swap blobs: %v", err)
	}
	defer func() {
		if err := store.SetPartBlobs(s, probe.Store); err != nil {
			t.Errorf("restore blob backend: %v", err)
		}
	}()

	cutoff := time.Now().Add(-time.Hour)
	counts, err := s.SweepBlobErasure(ctx, cutoff, cutoff)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if counts.OrphanUnlinkAttempts != 1 || counts.OrphanUnlinkFailed != 0 {
		t.Fatalf("counts = %+v, want one successful unlink and no failure", counts)
	}
	if blobPresent(t, s, f.ID) {
		t.Fatalf("orphan blob %d survived the committed reclaim", f.ID)
	}
}

// Two workers may race on a rowless object because there is no row to lock.
// Remove is idempotent, so the serialized backend models the one physical
// deletion while each worker reports only its own successful Remove attempt.
func TestBlobErasureSweepConcurrentPassesRemoveEachBlobOnce(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559201008")
	f := storedFileWithBytes(t, s, a.ID)
	if err := store.EraseFileRow(ctx, s, f.ID); err != nil {
		t.Fatalf("erase row: %v", err)
	}

	probe := &serializedRemoveBlobs{Store: store.BlobsOf(s), removed: make(map[string]int)}
	if err := store.SetPartBlobs(s, probe); err != nil {
		t.Fatalf("swap blobs: %v", err)
	}
	defer func() {
		if err := store.SetPartBlobs(s, probe.Store); err != nil {
			t.Errorf("restore blob backend: %v", err)
		}
	}()

	type result struct {
		counts store.BlobEraseCounts
		err    error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Go(func() {
			cutoff := time.Now().Add(-time.Hour)
			counts, err := s.SweepBlobErasure(ctx, cutoff, cutoff)
			results <- result{counts: counts, err: err}
		})
	}
	wg.Wait()
	close(results)

	var seen, attempts int
	for got := range results {
		if got.err != nil {
			t.Fatalf("concurrent sweep: %v", got.err)
		}
		if got.counts.OrphanUnlinkAttempts > 1 {
			t.Fatalf("OrphanUnlinkAttempts = %d, want at most one per worker", got.counts.OrphanUnlinkAttempts)
		}
		attempts += got.counts.OrphanUnlinkAttempts
		seen++
	}
	if seen != 2 {
		t.Fatalf("received %d sweep results, want 2", seen)
	}
	if attempts == 0 {
		t.Fatal("neither worker reported an unlink attempt")
	}
	if probe.removed[blob.Key(f.ID)] != 1 {
		t.Fatalf("physical removes for %q = %d, want 1", blob.Key(f.ID), probe.removed[blob.Key(f.ID)])
	}
	if blobPresent(t, s, f.ID) {
		t.Fatalf("orphan blob %d survived concurrent passes", f.ID)
	}
}
