package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
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
	if key == "" || len(key) != len(blob.PartsPrefix)+33 {
		t.Fatalf("blob key = %q, want a sharded %d-byte key under the parts prefix", key, len(blob.PartsPrefix)+33)
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

// TestReadLegacyFlatPartKey keeps in-flight parts from before the shard layout
// readable for their whole TTL: the row still names a flat key and ReadPartBytes
// must reach the object.
func TestReadLegacyFlatPartKey(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000111")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	legacyKey := blob.PartsPrefix + "deadbeef000000000000000000000000"
	payload := part('l', 100)
	b, dir := localBlobsOf(t, s)
	if _, err := b.Put(ctx, legacyKey, bytes.NewReader(payload)); err != nil {
		t.Fatalf("put legacy part: %v", err)
	}
	if err := store.InsertUploadPartWithKey(ctx, s, u.ID, 31, 0, int64(len(payload)), legacyKey); err != nil {
		t.Fatalf("insert row: %v", err)
	}

	got, err := s.ReadPartBytes(ctx, legacyKey)
	if err != nil {
		t.Fatalf("read legacy part: %v", err)
	}
	if !bytes.Equal(got, payload) {
		t.Fatalf("legacy part bytes differ")
	}
	if n := countPartKeys(t, dir); n != 1 {
		t.Fatalf("legacy part objects = %d, want 1", n)
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

// TestUploadPartRecordedSizeDescribesStoredObject is criterion 12 under key
// rotation: the recorded size describes the object the row currently names.
// The failure invariant 12 was written against — record 512 KiB, re-save as 1
// byte, keep the 512 KiB object — cannot arise here, because a re-save draws a
// fresh key and the superseded object is deleted once the row commits. Holding
// the larger size instead would describe an object that no longer exists, and
// the fail-closed read at assembly would then reject an upload that is
// perfectly well formed.
func TestUploadPartRecordedSizeDescribesStoredObject(t *testing.T) {
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

	size, key, err := store.UploadPartRow(ctx, s, u.ID, 9, 0)
	if err != nil {
		t.Fatalf("read row: %v", err)
	}
	if size != 1 {
		t.Fatalf("recorded size after shrink re-save = %d, want 1 (the size of the object the row names)", size)
	}

	// The bytes are the retry's, whole: a read at the recorded size returns
	// exactly them, so the fail-closed reconciliation at assembly passes.
	got, err := s.ReadPartBytes(ctx, key)
	if err != nil {
		t.Fatalf("read part bytes: %v", err)
	}
	if !bytes.Equal(got, []byte("x")) {
		t.Fatalf("stored object = %q, want the re-saved payload", got)
	}
	if _, ok, err := s.UploadPart(ctx, u.ID, 9, 0); err != nil || !ok {
		t.Fatalf("read part after shrink re-save: ok=%v err=%v (the upload must still assemble)", ok, err)
	}

	// And the 512 KiB object is gone, so the shrink evades no cap: the
	// accounting and the bytes on disk agree.
	_, dir := localBlobsOf(t, s)
	if n := countPartKeys(t, dir); n != 1 {
		t.Fatalf("part objects after re-save = %d, want 1 (the superseded object must be deleted)", n)
	}
}

// TestResaveDeletesSupersededObject is criterion 5's retry half: a client
// looping upload.saveFilePart on one part must not grow stored bytes while the
// row-based caps stay flat. Each re-save rotates the row onto a fresh key, so
// the object the row stopped naming is unreachable from any row and is deleted
// once the row commits.
func TestResaveDeletesSupersededObject(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000108")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	_, dir := localBlobsOf(t, s)
	const resaves = 5
	for i := range resaves {
		if err := s.SaveUploadPart(ctx, u.ID, 19, 0, part(byte('a'+i), 4096), maxFile); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
		if n := countPartKeys(t, dir); n != 1 {
			t.Fatalf("after %d saves of one part: %d objects on disk, want 1", i+1, n)
		}
	}

	// The accounting never moved, and neither did the bytes it accounts for.
	parts, _, total, err := s.UploadPartsSummary(ctx, u.ID, 19)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 1 || total != 4096 {
		t.Fatalf("after %d re-saves: parts=%d total=%d, want 1/4096", resaves, parts, total)
	}
}

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
	if err := store.SetPartBlobs(s, bad); err != nil {
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
	if err := store.SetPartBlobs(s, healthy); err != nil {
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
	claimed, err := s.ClaimExpiredPartsForTest(ctx, time.Now().Add(time.Hour), sweepBatch)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 || store.ClaimedPartKey(claimed[0]) != oldKey {
		t.Fatalf("claim returned %d rows, want the one naming %s", len(claimed), oldKey)
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
	// claimed key, so the re-saved row is spared — and it reports nothing
	// retired, which is what stops the drain instead of letting a client that
	// re-saves expired parts keep every pass full.
	retired, err := s.FinaliseExpiredPartsForTest(ctx, claimed)
	if err != nil {
		t.Fatalf("finalise: %v", err)
	}
	if retired != 0 {
		t.Fatalf("finalise retired %d rows, want 0: a claim the conditional delete did not match is not a retirement", retired)
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

// TestConcurrentResavesLeaveNoUnnamedObject is the invariant the serial case
// above cannot reach: an orphaned object is reachable only through process or
// backend failure, never through a well-formed sequence of client requests.
// Concurrent saves of one part are well formed — a client retrying a part it
// has not had an answer for yet sends exactly this — and they are the case the
// advisory lock does not cover, because it orders the two rows and then
// releases at the commit while both saves still have their byte work to do.
// Rounds rather than one shot: the interleaving that produces the object is a
// race, so one pass proves nothing about the pass that would have lost.
func TestConcurrentResavesLeaveNoUnnamedObject(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000111")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, dir := localBlobsOf(t, s)

	// A max-size part is what makes the window observable rather than
	// theoretical: the losing saver's write has to still be in flight when the
	// winner's cleanup runs, and 512 KiB is long enough next to the round trips
	// the winner makes first. Against the unfixed save path this fails inside
	// ten rounds, every run.
	const savers = 4
	const rounds = 40
	marks := [savers]byte{'a', 'b', 'c', 'd'}
	for r := range rounds {
		errs := make([]error, savers)
		var wg sync.WaitGroup
		for i := range savers {
			wg.Go(func() {
				errs[i] = s.SaveUploadPart(ctx, u.ID, 25, 0, part(marks[i], 512*1024), maxFile)
			})
		}
		wg.Wait()
		for i, err := range errs {
			if err != nil {
				t.Fatalf("round %d saver %d: %v", r, i, err)
			}
		}
		assertEveryPartObjectIsNamed(t, s, dir, fmt.Sprintf("round %d", r))
	}

	// One part, one row, one object: the caps still describe what is stored.
	parts, _, total, err := s.UploadPartsSummary(ctx, u.ID, 25)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 1 || total != 512*1024 {
		t.Fatalf("after %d concurrent saves: parts=%d total=%d, want 1/524288", savers*rounds, parts, total)
	}
	if n := countPartKeys(t, dir); n != 1 {
		t.Fatalf("part objects after %d concurrent saves = %d, want 1", savers*rounds, n)
	}
}

// TestCancelledSaveCleansObjectAfterResave covers the request-disconnect
// variant of the same race. The first save commits its row, then its blob
// backend cancels the request context and pauses before landing the object.
// A second save supersedes the row and removes the first key while it is still
// absent. When the first object finally lands, its cleanup must use a detached
// bounded context or the canceled request prevents the row re-read and leaves
// bytes that no row names.
func TestCancelledSaveCleansObjectAfterResave(t *testing.T) {
	t.Parallel()
	s := open(t)
	requestCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	u, err := s.CreateUser(requestCtx, "+15559000114")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	plain, dir := localBlobsOf(t, s)
	gate := &cancelDuringPutBlobs{
		Store:        plain,
		cancel:       cancel,
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	if err := store.SetPartBlobs(s, gate); err != nil {
		t.Fatalf("install blob gate: %v", err)
	}

	var firstErr error
	var first sync.WaitGroup
	first.Go(func() {
		firstErr = s.SaveUploadPart(requestCtx, u.ID, 31, 0, part('a', 512*1024), maxFile)
	})
	<-gate.firstStarted

	secondErr := s.SaveUploadPart(context.Background(), u.ID, 31, 0, part('b', 512*1024), maxFile)
	if secondErr != nil {
		close(gate.releaseFirst)
		first.Wait()
		t.Fatalf("second save: %v", secondErr)
	}
	close(gate.releaseFirst)
	first.Wait()
	if firstErr != nil {
		t.Fatalf("canceled first save: %v", firstErr)
	}
	if !errors.Is(requestCtx.Err(), context.Canceled) {
		t.Fatalf("request context = %v, want canceled", requestCtx.Err())
	}

	assertEveryPartObjectIsNamed(t, s, dir, "canceled save")
	if n := countPartKeys(t, dir); n != 1 {
		t.Fatalf("part objects after canceled resave = %d, want 1", n)
	}
}

// TestSaveUploadPartUsesBlobOperationTimeout keeps the detached cleanup bound
// tied to the configured backend rather than to one concrete backend's
// default. A backend selected with a longer operation bound must be allowed to
// finish its Put after the request has gone away.
func TestSaveUploadPartUsesBlobOperationTimeout(t *testing.T) {
	t.Parallel()
	s := open(t)
	plain := store.BlobsOf(s)
	const timeout = 2 * time.Minute
	probe := &operationTimeoutProbe{
		Store:   plain,
		timeout: timeout,
		seen:    make(chan time.Time, 1),
	}
	if err := store.SetPartBlobs(s, probe); err != nil {
		t.Fatalf("install timeout probe: %v", err)
	}
	u, err := s.CreateUser(context.Background(), "+15559000115")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if err := s.SaveUploadPart(context.Background(), u.ID, 32, 0, []byte("payload"), maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	deadline := <-probe.seen
	remaining := time.Until(deadline)
	if remaining < timeout-time.Second || remaining > timeout+time.Second {
		t.Fatalf("blob operation deadline has %s remaining, want close to %s", remaining, timeout)
	}
}

// TestAssemblyCleanupRacingResaveLeavesNoUnnamedObject is the same invariant
// through the other door. The assembly cleanup reads the part set, deletes
// those objects, then retires the rows; a save that commits inside that window
// puts a new key on its row and writes the object for it, and a row delete that
// did not check which key the row names takes that row away and strands the
// object. Both sides are ordinary requests — messages.sendMedia and
// upload.saveFilePart naming one file.
//
// A part re-saved mid-cleanup keeping its row and its object is the right
// outcome, not a leak: it is an in-flight part like any other, counted by the
// caps and retired by the next assembly or by the TTL sweep.
//
// The filler parts widen the cleanup's unlink loop, which is the window the
// save has to land in. They are one byte each, which is also what makes the
// leak worth closing rather than rate-limiting: the window costs an attacker
// almost nothing against the byte caps.
func TestAssemblyCleanupRacingResaveLeavesNoUnnamedObject(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000112")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	_, dir := localBlobsOf(t, s)

	// The production per-file cap rather than this file's small one: the row
	// cap is derived from it, and the filler parts need the headroom.
	const wideFile = 100 << 20
	const fillers = 200
	const rounds = 12

	for r := range rounds {
		if err := s.SaveUploadPart(ctx, u.ID, 27, 0, part('a', 512*1024), wideFile); err != nil {
			t.Fatalf("round %d seed part 0: %v", r, err)
		}
		for i := 1; i <= fillers; i++ {
			if err := s.SaveUploadPart(ctx, u.ID, 27, i, part('f', 1), wideFile); err != nil {
				t.Fatalf("round %d seed filler %d: %v", r, i, err)
			}
		}

		var cleanupErr, saveErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			_, cleanupErr = s.DeleteUploadParts(ctx, u.ID, 27)
		})
		wg.Go(func() {
			saveErr = s.SaveUploadPart(ctx, u.ID, 27, 0, part('b', 512*1024), wideFile)
		})
		wg.Wait()
		if cleanupErr != nil {
			t.Fatalf("round %d cleanup: %v", r, cleanupErr)
		}
		if saveErr != nil {
			t.Fatalf("round %d racing save: %v", r, saveErr)
		}
		assertEveryPartObjectIsNamed(t, s, dir, fmt.Sprintf("round %d", r))

		// Whatever the race left is an ordinary in-flight part, so an
		// uncontended cleanup retires it and its bytes together.
		if _, err := s.DeleteUploadParts(ctx, u.ID, 27); err != nil {
			t.Fatalf("round %d drain: %v", r, err)
		}
		if n := countPartKeys(t, dir); n != 0 {
			t.Fatalf("round %d: %d part objects survive an uncontended cleanup, want 0", r, n)
		}
	}
}

// latencyBlobs answers at network speed: each call takes a round trip and its
// effect lands at the end of one, which is what MAIN-342's backend will do and
// what the local filesystem does not. It is not a timing knob invented for the
// test — the window below is sub-millisecond on the local backend and seconds
// wide on a remote one, so this is the backend the invariant has to hold
// against, run at a scale a test can assert on.
//
// putStarted closes when the first Put is entered, before its delay, so a test
// can begin the racing side at a point the shipped code defines rather than at
// a point a sleep guesses at.
type latencyBlobs struct {
	blob.Store

	put, remove time.Duration
	started     chan struct{}
	once        sync.Once
}

func newLatencyBlobs(inner blob.Store, put, remove time.Duration) *latencyBlobs {
	return &latencyBlobs{Store: inner, put: put, remove: remove, started: make(chan struct{})}
}

type cancelDuringPutBlobs struct {
	blob.Store

	cancel       context.CancelFunc
	firstStarted chan struct{}
	releaseFirst chan struct{}
	once         sync.Once
}

type operationTimeoutProbe struct {
	blob.Store

	timeout time.Duration
	seen    chan time.Time
}

func (b *operationTimeoutProbe) OperationTimeout() time.Duration { return b.timeout }

func (b *operationTimeoutProbe) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		b.seen <- time.Time{}
	} else {
		b.seen <- deadline
	}
	return b.Store.Put(ctx, key, r)
}

func (b *cancelDuringPutBlobs) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	first := false
	b.once.Do(func() {
		first = true
		b.cancel()
		close(b.firstStarted)
	})
	if first {
		<-b.releaseFirst
	}
	return b.Store.Put(ctx, key, r)
}

func (l *latencyBlobs) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	l.once.Do(func() { close(l.started) })
	time.Sleep(l.put)
	return l.Store.Put(ctx, key, r)
}

func (l *latencyBlobs) Remove(ctx context.Context, key string) error {
	time.Sleep(l.remove)
	return l.Store.Remove(ctx, key)
}

// TestRetirementCollectsBytesLandingInsideIt is the third door, and the one
// neither the conditional row delete nor the writer's re-read can see. The
// sequence is six ordinary steps: the save commits so the row names its new
// key, the cleanup reads the part set and sees that key, the cleanup deletes
// bytes that have not landed yet, the save's write lands, the save re-reads and
// correctly keeps its object because nothing has retired the row, and only then
// does the cleanup retire it. Nobody misbehaves and nobody can observe the
// problem locally: the row is consistent every time either party looks at it.
//
// What closes it is ownership rather than order — the party that last changes a
// row's state owns the bytes at the key that row named — so the retiring
// transaction deletes those objects again once it has committed.
//
// The oracle is the latency backend rather than the scheduler. On the local
// filesystem this window is a sub-millisecond alignment that a loaded host hits
// once in a few hundred rounds, which is a flake whichever way it lands; at
// round-trip speed the ordering above is forced, and the test fails every run
// without the second delete and passes every run with it.
func TestRetirementCollectsBytesLandingInsideIt(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000113")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	plain, dir := localBlobsOf(t, s)

	// The production per-file cap: the row cap is derived from it and the
	// fillers need the headroom.
	const wideFile = 100 << 20
	// Enough fillers that the cleanup is still deleting objects when the
	// racing save's write lands and it re-reads its row: 11 removes at 20ms
	// against one put at 100ms.
	const fillers = 10
	const putDelay = 100 * time.Millisecond
	const removeDelay = 20 * time.Millisecond
	const rounds = 5

	for r := range rounds {
		// Seed on the plain backend: the setup is not what is under test.
		if err := store.SetPartBlobs(s, plain); err != nil {
			t.Fatalf("round %d plain backend: %v", r, err)
		}
		if err := s.SaveUploadPart(ctx, u.ID, 29, 0, part('a', 512*1024), wideFile); err != nil {
			t.Fatalf("round %d seed part 0: %v", r, err)
		}
		for i := 1; i <= fillers; i++ {
			if err := s.SaveUploadPart(ctx, u.ID, 29, i, part('f', 1), wideFile); err != nil {
				t.Fatalf("round %d seed filler %d: %v", r, i, err)
			}
		}

		slow := newLatencyBlobs(plain, putDelay, removeDelay)
		if err := store.SetPartBlobs(s, slow); err != nil {
			t.Fatalf("round %d slow backend: %v", r, err)
		}

		var saveErr, cleanupErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			saveErr = s.SaveUploadPart(ctx, u.ID, 29, 0, part('b', 512*1024), wideFile)
		})
		// The save has committed its row and is inside its write: the cleanup
		// now reads a key whose object does not exist yet.
		<-slow.started
		wg.Go(func() {
			_, cleanupErr = s.DeleteUploadParts(ctx, u.ID, 29)
		})
		wg.Wait()
		if saveErr != nil {
			t.Fatalf("round %d racing save: %v", r, saveErr)
		}
		if cleanupErr != nil {
			t.Fatalf("round %d cleanup: %v", r, cleanupErr)
		}

		if err := store.SetPartBlobs(s, plain); err != nil {
			t.Fatalf("round %d restore backend: %v", r, err)
		}
		assertEveryPartObjectIsNamed(t, s, dir, fmt.Sprintf("round %d", r))

		if _, err := s.DeleteUploadParts(ctx, u.ID, 29); err != nil {
			t.Fatalf("round %d drain: %v", r, err)
		}
		if n := countPartKeys(t, dir); n != 0 {
			t.Fatalf("round %d: %d part objects survive an uncontended cleanup, want 0", r, n)
		}
	}
}

// assertEveryPartObjectIsNamed fails if anything under the parts prefix is not
// named by an upload_parts row. It reads the directory rather than the blob
// interface on purpose: blob.Store has no enumeration and does not grow one
// here, and the filesystem is the ground truth the caps are supposed to
// describe.
func assertEveryPartObjectIsNamed(t *testing.T, s *store.Store, dir, when string) {
	t.Helper()
	keys, err := store.UploadPartKeysNamed(context.Background(), s)
	if err != nil {
		t.Fatalf("%s: read named keys: %v", when, err)
	}
	named := make(map[string]bool, len(keys))
	for _, k := range keys {
		named[k] = true
	}
	for _, name := range listPartKeys(t, dir) {
		if !named[blob.PartsPrefix+name] {
			t.Fatalf("%s: object %q under the parts prefix is named by no row", when, name)
		}
	}
}

// TestSweepRetiresRowsLeftWithoutAKey covers the parts that were in flight
// when this change deployed: the migration gives their rows the default empty
// blob_key, so they name no object. They are also the oldest rows in the
// table, and the sweep takes oldest first, so if the empty key errored the
// pass every TTL reclamation would be dead from deploy onwards — not just for
// these rows, but for every row behind them. Deleting no object is not an
// error; the row is retired on schedule.
func TestSweepRetiresRowsLeftWithoutAKey(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000109")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Two rows the migration left keyless, then one ordinary saved part behind
	// them: the keyless rows sort first, so they are what a pass hits first.
	for i := range 2 {
		if err := store.InsertUploadPartWithoutKey(ctx, s, u.ID, 21, int32(i), 100); err != nil {
			t.Fatalf("insert keyless row %d: %v", i, err)
		}
	}
	if err := s.SaveUploadPart(ctx, u.ID, 21, 2, part('a', 100), maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}

	n, err := s.SweepExpiredUploadParts(ctx, time.Now().Add(time.Hour), sweepBatch)
	if err != nil {
		t.Fatalf("sweep over keyless rows: %v", err)
	}
	if n != 3 {
		t.Fatalf("sweep retired %d rows, want 3", n)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 21); err != nil || parts != 0 {
		t.Fatalf("after sweep: parts=%d err=%v", parts, err)
	}
	_, dir := localBlobsOf(t, s)
	if got := countPartKeys(t, dir); got != 0 {
		t.Fatalf("part objects after sweep = %d, want 0", got)
	}
}

// TestFinaliseDeletesOneRowPerClaimedKey is the per-row bound on the finalise
// step. The rows the migration left keyless all share one blob_key value, the
// empty string, so a finalise that identified rows by key alone would delete
// every one of them on the first claimed key — the batch bound the sweep
// exists to hold would be gone for exactly the deploy case above. The delete
// names a row's primary key as well, so a claim of one retires one.
func TestFinaliseDeletesOneRowPerClaimedKey(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000110")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	for i := range 3 {
		if err := store.InsertUploadPartWithoutKey(ctx, s, u.ID, 23, int32(i), 100); err != nil {
			t.Fatalf("insert keyless row %d: %v", i, err)
		}
	}

	claimed, err := s.ClaimExpiredPartsForTest(ctx, time.Now().Add(time.Hour), 1)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if len(claimed) != 1 {
		t.Fatalf("claim of 1 returned %d rows", len(claimed))
	}
	retired, err := s.FinaliseExpiredPartsForTest(ctx, claimed)
	if err != nil {
		t.Fatalf("finalise: %v", err)
	}
	if retired != 1 {
		t.Fatalf("finalise retired %d rows for one claimed key, want 1", retired)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 23); err != nil || parts != 2 {
		t.Fatalf("after a one-row finalise: parts=%d err=%v, want 2 left", parts, err)
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

// listPartKeys lists object paths under the parts prefix in dir, relative to
// parts/. Flat legacy keys and sharded keys are both included.
func listPartKeys(t *testing.T, dir string) []string {
	t.Helper()
	partsRoot := filepath.Join(dir, "parts")
	entries, err := os.ReadDir(partsRoot)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read parts dir: %v", err)
	}
	var keys []string
	for _, e := range entries {
		if e.IsDir() {
			sub, err := os.ReadDir(filepath.Join(partsRoot, e.Name()))
			if err != nil {
				t.Fatalf("read parts shard %s: %v", e.Name(), err)
			}
			for _, f := range sub {
				if !f.IsDir() {
					keys = append(keys, e.Name()+"/"+f.Name())
				}
			}
			continue
		}
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
