package store_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// blobBody is what a test writes at a file's key, so "the bytes are gone" is a
// statement about bytes that were there.
var blobBody = []byte("the bytes of one media file")

// storedFileWithBytes allocates a file, marks it stored and writes its blob at
// the key the assembly path writes to. Every erase assertion is about both
// halves — the row and the bytes — and a file with no bytes would let an
// eraser that never unlinks anything pass.
func storedFileWithBytes(t *testing.T, s *store.Store, uploaderID int64) store.File {
	t.Helper()
	f := storedFile(t, s, uploaderID)
	if _, err := store.BlobsOf(s).Put(context.Background(), blob.Key(f.ID), bytes.NewReader(blobBody)); err != nil {
		t.Fatalf("put blob for file %d: %v", f.ID, err)
	}
	return f
}

// blobPresent reports whether a file's bytes are still on the blob store.
func blobPresent(t *testing.T, s *store.Store, fileID int64) bool {
	t.Helper()
	_, err := store.BlobsOf(s).ReadAt(context.Background(), blob.Key(fileID), 0, 1)
	switch {
	case errors.Is(err, blob.ErrNotFound):
		return false
	case err != nil:
		t.Fatalf("read blob for file %d: %v", fileID, err)
	}
	return true
}

// rowPresent reports whether a files row is still there.
func rowPresent(t *testing.T, s *store.Store, fileID int64) bool {
	t.Helper()
	ok, err := store.FileRowExists(context.Background(), s, fileID)
	if err != nil {
		t.Fatalf("file row %d: %v", fileID, err)
	}
	return ok
}

// sweep runs one full erasure sweep over this test's database.
func sweep(t *testing.T, s *store.Store, olderThan time.Time) store.EraseCounts {
	t.Helper()
	c, err := s.SweepMediaErasure(context.Background(), olderThan, store.ErasureScanBatch)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	return c
}

// deletedBothSides sends fileID from a to b and soft-deletes both copies, the
// state a file has to be in before anything may erase it.
func deletedBothSides(t *testing.T, s *store.Store, a, b store.User, fileID, randomID int64) {
	t.Helper()
	ctx := context.Background()
	aLocal, bLocal := sendMedia(t, s, a, b, fileID, randomID)
	if err := store.SetMessageDeleted(ctx, s, a.ID, aLocal); err != nil {
		t.Fatalf("delete sender copy: %v", err)
	}
	if err := store.SetMessageDeleted(ctx, s, b.ID, bLocal); err != nil {
		t.Fatalf("delete recipient copy: %v", err)
	}
}

// The ticket's criterion 2: a media file whose every reference is soft-deleted
// and which is past the cutoff loses its row and its bytes, with no operator
// action, in one sweep.
func TestErasureSweepErasesUnreferencedFile(t *testing.T) {
	t.Parallel()
	s := open(t)
	a := mustUser(t, s, "+15559200001")
	b := mustUser(t, s, "+15559200002")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)

	c := sweep(t, s, future())
	if c.Erased != 1 || c.ErasedBytes != f.Size {
		t.Fatalf("counts = %+v, want 1 erased and %d bytes", c, f.Size)
	}
	if rowPresent(t, s, f.ID) {
		t.Errorf("files row %d survived the sweep", f.ID)
	}
	if blobPresent(t, s, f.ID) {
		t.Errorf("blob for file %d survived the sweep", f.ID)
	}
}

// A file with a live reference is not erased, which is the invariant with no
// recovery path. The live side here is the recipient's copy: the sender deleted
// theirs, which is the state that makes an eraser that only reads one side
// destroy the recipient's media.
func TestErasureSweepRetainsFileWithOneLiveCopy(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200011")
	b := mustUser(t, s, "+15559200012")

	f := storedFileWithBytes(t, s, a.ID)
	aLocal, _ := sendMedia(t, s, a, b, f.ID, 1)
	if err := store.SetMessageDeleted(ctx, s, a.ID, aLocal); err != nil {
		t.Fatalf("delete sender copy: %v", err)
	}

	if c := sweep(t, s, future()); c.Erased != 0 {
		t.Fatalf("counts = %+v, want nothing erased", c)
	}
	if !rowPresent(t, s, f.ID) {
		t.Errorf("files row %d was erased while a live copy referenced it", f.ID)
	}
	if !blobPresent(t, s, f.ID) {
		t.Errorf("blob for file %d was unlinked while a live copy referenced it", f.ID)
	}
}

// The ordering that is the whole safety argument: the row deletion commits
// first and the blob is unlinked only after that commit returns.
//
// Asserted from inside the unlink rather than after the sweep, because after
// the sweep both orders look identical. The blob backend under the eraser reads
// the files table at the moment Remove is called: a row still present there is
// an eraser that unlinked first, which leaves the download gate admitting a
// caller to bytes that are gone — a fourth download outcome, and permanent if
// the process dies in that window.
func TestErasureSweepUnlinksOnlyAfterTheRowCommits(t *testing.T) {
	t.Parallel()
	s := open(t)
	a := mustUser(t, s, "+15559200021")
	b := mustUser(t, s, "+15559200022")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)

	var calls int
	probe := &probeBlobs{
		Store: store.BlobsOf(s),
		onRemove: func(key string) {
			calls++
			if key != blob.Key(f.ID) {
				t.Errorf("unlinked key %q, want %q", key, blob.Key(f.ID))
			}
			if rowPresent(t, s, f.ID) {
				t.Errorf("blob for file %d unlinked while its files row was still present", f.ID)
			}
		},
	}
	if err := store.SetPartBlobs(s, probe); err != nil {
		t.Fatalf("swap blobs: %v", err)
	}

	if c := sweep(t, s, future()); c.Erased != 1 {
		t.Fatalf("counts = %+v, want one erased", c)
	}
	if calls != 1 {
		t.Fatalf("Remove called %d times, want 1 — the assertion above never ran", calls)
	}
}

// probeBlobs is a blob store that reports each unlink before performing it.
type probeBlobs struct {
	blob.Store

	onRemove func(key string)
}

func (p *probeBlobs) Remove(ctx context.Context, key string) error {
	p.onRemove(key)
	return p.Store.Remove(ctx, key)
}

// Criterion 3, first case: an erase racing a forward of the last live copy.
//
// The forward carries a file id it read in an earlier transaction — that is
// what a forward is — and it commits in the gap between the scan naming the
// file and the erase transaction taking its row. Nothing in the scan can see
// it: under READ COMMITTED an in-flight insert is invisible, and by the time it
// is visible the scan is over. The sweep must refuse the file, and the account
// that received the forward must still be able to resolve it.
//
// The other half of this race — a forward that has not yet committed when the
// eraser takes the row — is what the interlock itself covers, and is pinned by
// TestForwardSerializesAgainstFileRowRemoval.
func TestErasureSweepLosesToForwardOfLastLiveCopy(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200031")
	b := mustUser(t, s, "+15559200032")
	c := mustUser(t, s, "+15559200033")

	f := storedFileWithBytes(t, s, a.ID)
	src, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0) //nolint:dogsled // only the stored row is needed
	if err != nil {
		t.Fatalf("seed send: %v", err)
	}
	// Both copies deleted: the file is a candidate, which is the state a forward
	// of the last live copy races.
	deleteBoth(t, s, a.ID, src.LocalID, b.ID, src.PeerLocalID)

	var forwarded bool
	store.SetEraseHook(s, func(fileID int64) {
		if fileID != f.ID || forwarded {
			return
		}
		forwarded = true
		_, fwd, err := s.ForwardMessages(ctx, b.ID, store.PeerTypeUser, c.ID,
			[]store.ForwardSource{{FromID: a.ID, Date: src.Date, Text: src.Text, FileID: f.ID}},
			[]int64{2})
		if err != nil {
			t.Errorf("forward: %v", err)
			return
		}
		if len(fwd) != 1 {
			t.Errorf("forward wrote %d messages, want 1", len(fwd))
		}
	})

	counts := sweep(t, s, future())
	if !forwarded {
		t.Fatal("the hook never fired, so the race under test never happened")
	}
	if counts.Erased != 0 || counts.Retained != 1 {
		t.Fatalf("counts = %+v, want nothing erased and one retained", counts)
	}
	if !rowPresent(t, s, f.ID) {
		t.Errorf("files row %d erased out from under a forward", f.ID)
	}
	if !blobPresent(t, s, f.ID) {
		t.Errorf("blob for file %d unlinked out from under a forward", f.ID)
	}
	// The account that received the forward resolves the file, which is the
	// outcome the invariant is actually about.
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, c.ID); err != nil {
		t.Errorf("recipient of the forward cannot resolve the file: %v", err)
	}
}

// deleteBoth soft-deletes one message's two stored copies.
func deleteBoth(t *testing.T, s *store.Store, aID, aLocal, bID, bLocal int64) {
	t.Helper()
	ctx := context.Background()
	if err := store.SetMessageDeleted(ctx, s, aID, aLocal); err != nil {
		t.Fatalf("delete sender copy: %v", err)
	}
	if err := store.SetMessageDeleted(ctx, s, bID, bLocal); err != nil {
		t.Fatalf("delete recipient copy: %v", err)
	}
}

// Criterion 3, second case: an erase racing a fresh send of a file that has
// just been marked stored. Every media file on the server passes through
// "stored, zero live references" exactly once during a normal send, so this is
// not an edge case — it is what the reference predicate matches on every file
// currently being sent, and only the row lock keeps it safe.
func TestErasureSweepLosesToFreshSendOfJustStoredFile(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200041")
	b := mustUser(t, s, "+15559200042")

	// Stored, past the cutoff, never referenced: the state between assembly and
	// the send's insert.
	f := storedFileWithBytes(t, s, a.ID)

	var sent bool
	store.SetEraseHook(s, func(fileID int64) {
		if fileID != f.ID || sent {
			return
		}
		sent = true
		sendMedia(t, s, a, b, f.ID, 7)
	})

	counts := sweep(t, s, future())
	if !sent {
		t.Fatal("the hook never fired, so the race under test never happened")
	}
	if counts.Erased != 0 {
		t.Fatalf("counts = %+v, want nothing erased", counts)
	}
	if !rowPresent(t, s, f.ID) {
		t.Errorf("files row %d erased out from under a fresh send", f.ID)
	}
	if !blobPresent(t, s, f.ID) {
		t.Errorf("blob for file %d unlinked out from under a fresh send", f.ID)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, b.ID); err != nil {
		t.Errorf("recipient of the send cannot resolve the file: %v", err)
	}
}

// Criterion 4: a file gaining a live channel reference during the erase
// transaction leaves the blob alone. No handler produces channel media, so the
// row is inserted directly.
//
// Two things have to hold, and the second is the one that makes the foreign key
// still a backstop: nothing is erased, and the soft-deleted post's own file
// reference is still in place, because the whole transaction rolled back rather
// than committing the half that had already cleared it.
func TestErasureSweepAbortsOnChannelReferenceCreatedInTheWindow(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200051")
	b := mustUser(t, s, "+15559200052")

	f := storedFileWithBytes(t, s, a.ID)
	chID, postLocal, err := store.SeedChannelPost(ctx, s, a.ID, b.ID, f.ID)
	if err != nil {
		t.Fatalf("seed channel post: %v", err)
	}
	if err := store.SetChannelPostDeleted(ctx, s, chID, postLocal); err != nil {
		t.Fatalf("delete channel post: %v", err)
	}

	var posted bool
	store.SetEraseHook(s, func(fileID int64) {
		if fileID != f.ID || posted {
			return
		}
		posted = true
		if err := store.InsertLiveChannelPost(ctx, s, chID, postLocal+1000, a.ID, f.ID); err != nil {
			t.Errorf("insert live channel post: %v", err)
		}
	})

	counts := sweep(t, s, future())
	if !posted {
		t.Fatal("the hook never fired, so the race under test never happened")
	}
	if counts.Erased != 0 {
		t.Fatalf("counts = %+v, want nothing erased", counts)
	}
	if !rowPresent(t, s, f.ID) {
		t.Errorf("files row %d erased with a live channel post referencing it", f.ID)
	}
	if !blobPresent(t, s, f.ID) {
		t.Errorf("blob for file %d unlinked with a live channel post referencing it", f.ID)
	}
	ref, err := store.ChannelPostFileID(ctx, s, chID, postLocal)
	if err != nil {
		t.Fatalf("read deleted post reference: %v", err)
	}
	if ref == nil || *ref != f.ID {
		t.Errorf("the deleted post's file reference was cleared and left cleared: got %v, want %d", ref, f.ID)
	}
}

// The positive channel case, and the one that proves the reference clearing is
// load-bearing rather than decorative: a file whose only channel post is
// soft-deleted IS erasable. Without the clearing statement the foreign key
// refuses the row delete — it counts soft-deleted rows — so any file ever
// posted to a channel would be unerasable forever.
func TestErasureSweepClearsDeletedChannelReference(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200061")
	b := mustUser(t, s, "+15559200062")

	f := storedFileWithBytes(t, s, a.ID)
	chID, postLocal, err := store.SeedChannelPost(ctx, s, a.ID, b.ID, f.ID)
	if err != nil {
		t.Fatalf("seed channel post: %v", err)
	}
	if err := store.SetChannelPostDeleted(ctx, s, chID, postLocal); err != nil {
		t.Fatalf("delete channel post: %v", err)
	}

	if c := sweep(t, s, future()); c.Erased != 1 {
		t.Fatalf("counts = %+v, want one erased", c)
	}
	if rowPresent(t, s, f.ID) {
		t.Errorf("files row %d survived: the channel reference was never released", f.ID)
	}
	if blobPresent(t, s, f.ID) {
		t.Errorf("blob for file %d survived the sweep", f.ID)
	}
	ref, err := store.ChannelPostFileID(ctx, s, chID, postLocal)
	if err != nil {
		t.Fatalf("read deleted post reference: %v", err)
	}
	if ref != nil {
		t.Errorf("deleted post still names file %d after its file row went", *ref)
	}
}

// A live channel post keeps the file, with no race involved. It is the standing
// case rather than the window one, and it is the assertion that fails first if
// the channel branch is ever dropped from the predicate.
func TestErasureSweepRetainsFileWithLiveChannelPost(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200071")
	b := mustUser(t, s, "+15559200072")

	f := storedFileWithBytes(t, s, a.ID)
	if _, _, err := store.SeedChannelPost(ctx, s, a.ID, b.ID, f.ID); err != nil {
		t.Fatalf("seed channel post: %v", err)
	}

	if c := sweep(t, s, future()); c.Erased != 0 {
		t.Fatalf("counts = %+v, want nothing erased", c)
	}
	if !rowPresent(t, s, f.ID) || !blobPresent(t, s, f.ID) {
		t.Errorf("file %d erased while a live channel post referenced it", f.ID)
	}
}

// Criteria 5 and 11 together: the eraser takes the file row exclusively, and a
// row another transaction holds is skipped rather than waited on.
//
// The other holder takes the row FOR SHARE, which is the mode the reference
// interlock takes — so this asserts the eraser reaches for a lock that conflicts
// with a reference insert, which is what "serializes against the interlock
// rather than racing it" means. An eraser taking no lock, or a shared one,
// erases the file here.
//
// The bounded context is the other half: the sweep has to return without
// blocking. Left to wait, it would park until the holder released, and a test
// that merely asserted the file survived would pass on that eraser too.
func TestErasureSweepSkipsRowHeldByAReferenceWriter(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200081")
	b := mustUser(t, s, "+15559200082")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)

	release, err := store.HoldFileRowShared(ctx, s, f.ID)
	if err != nil {
		t.Fatalf("hold file row: %v", err)
	}
	defer release()

	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	counts, err := s.SweepMediaErasure(bounded, future(), store.ErasureScanBatch)
	if err != nil {
		t.Fatalf("sweep blocked on a held row: %v", err)
	}
	if counts.Erased != 0 || counts.Contended != 1 {
		t.Fatalf("counts = %+v, want nothing erased and one contended", counts)
	}
	if !rowPresent(t, s, f.ID) || !blobPresent(t, s, f.ID) {
		t.Errorf("file %d erased while another transaction held its row", f.ID)
	}

	// Released, the same file is erasable: the skip is a deferral, not a
	// permanent exclusion, and without this the assertion above would pass on an
	// eraser that simply never erases anything.
	release()
	if c := sweep(t, s, future()); c.Erased != 1 {
		t.Fatalf("counts after release = %+v, want one erased", c)
	}
}

// Criterion 6: usage falls when an account's files are erased, with no separate
// counter — the quota is summed off the files table, so removing the row is the
// whole mechanism. An account that had hit its cap can upload again afterwards.
func TestErasureSweepFreesQuota(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200091")
	b := mustUser(t, s, "+15559200092")

	const quota = 4096
	f, err := s.AllocateFile(ctx, a.ID, quota, "image/jpeg", "f.jpg", quota)
	if err != nil {
		t.Fatalf("allocate to the quota: %v", err)
	}
	if err := s.MarkFileStored(ctx, f.ID); err != nil {
		t.Fatalf("mark stored: %v", err)
	}
	if _, err := store.BlobsOf(s).Put(ctx, blob.Key(f.ID), bytes.NewReader(blobBody)); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	// The account is now exactly at its quota.
	if _, err := s.AllocateFile(ctx, a.ID, quota, "image/jpeg", "g.jpg", quota); !errors.Is(err, store.ErrStorageQuota) {
		t.Fatalf("second allocate = %v, want ErrStorageQuota", err)
	}

	deletedBothSides(t, s, a, b, f.ID, 1)
	if c := sweep(t, s, future()); c.Erased != 1 {
		t.Fatalf("counts = %+v, want one erased", c)
	}

	if _, err := s.AllocateFile(ctx, a.ID, quota, "image/jpeg", "g.jpg", quota); err != nil {
		t.Fatalf("allocate after erasure: %v, want the freed quota to admit it", err)
	}
}

// The age cutoff, re-evaluated by the delete inside the exclusive hold rather
// than trusted from the scan that named the file. Driven by moving the row's
// date after the scan, because that is the only way to reach the condition:
// with the scan and the delete agreeing, a delete that had dropped its own copy
// of the gate would pass every other test in this file.
func TestErasureSweepRefusesAFileThatStopsBeingOldEnough(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200101")
	b := mustUser(t, s, "+15559200102")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)

	var moved bool
	store.SetEraseHook(s, func(fileID int64) {
		if fileID != f.ID || moved {
			return
		}
		moved = true
		if err := store.SetFileDate(ctx, s, f.ID, time.Now().Add(2*time.Hour)); err != nil {
			t.Errorf("move file date: %v", err)
		}
	})

	counts := sweep(t, s, future())
	if !moved {
		t.Fatal("the hook never fired, so the condition under test was never reached")
	}
	if counts.Erased != 0 || counts.Retained != 1 {
		t.Fatalf("counts = %+v, want nothing erased and one retained", counts)
	}
	if !rowPresent(t, s, f.ID) || !blobPresent(t, s, f.ID) {
		t.Errorf("file %d erased despite failing the age cutoff at delete time", f.ID)
	}
}

// The stored condition, re-evaluated in the same place and for the same reason.
// A not-stored row has no bytes at this key and belongs to the crashed-upload
// reclaim, not to this pass.
func TestErasureSweepRefusesAFileThatStopsBeingStored(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200111")
	b := mustUser(t, s, "+15559200112")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)

	var cleared bool
	store.SetEraseHook(s, func(fileID int64) {
		if fileID != f.ID || cleared {
			return
		}
		cleared = true
		if err := store.SetFileUnstored(ctx, s, f.ID); err != nil {
			t.Errorf("clear stored: %v", err)
		}
	})

	counts := sweep(t, s, future())
	if !cleared {
		t.Fatal("the hook never fired, so the condition under test was never reached")
	}
	if counts.Erased != 0 || counts.Retained != 1 {
		t.Fatalf("counts = %+v, want nothing erased and one retained", counts)
	}
	if !rowPresent(t, s, f.ID) || !blobPresent(t, s, f.ID) {
		t.Errorf("file %d erased despite no longer being stored", f.ID)
	}
}

// The sweep's own age gate: a cutoff no file has reached erases nothing, which
// is the gate the operator actually sets.
func TestErasureSweepErasesNothingBeforeTheCutoff(t *testing.T) {
	t.Parallel()
	s := open(t)
	a := mustUser(t, s, "+15559200121")
	b := mustUser(t, s, "+15559200122")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)

	c := sweep(t, s, past())
	if c.Erased != 0 || c.Considered != 0 {
		t.Fatalf("counts = %+v, want nothing considered and nothing erased", c)
	}
	if !rowPresent(t, s, f.ID) || !blobPresent(t, s, f.ID) {
		t.Errorf("file %d erased before its cutoff", f.ID)
	}
}

// Criterion 12: a resend naming an erased file is refused rather than replaying
// the original, and it writes no message row and no reference.
//
// The order this depends on is the interlock sitting ahead of the per-sender
// dedup read. Moving the dedup ahead of it would make this resend return the
// original message — which is a message the sender themself deleted, pointing
// at bytes that are gone — and every other test here would still pass. This
// pins it so a later reader does not "fix" it back.
func TestResendOfAnErasedFileIsRefused(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200131")
	b := mustUser(t, s, "+15559200132")

	f := storedFileWithBytes(t, s, a.ID)
	const randomID = 4242
	deletedBothSides(t, s, a, b, f.ID, randomID)
	if c := sweep(t, s, future()); c.Erased != 1 {
		t.Fatalf("counts = %+v, want one erased", c)
	}

	before := historyLen(t, s, a.ID, store.PeerTypeUser, b.ID)
	_, _, _, dup, err := s.SendMessage(ctx, a.ID, b.ID, "here", randomID, f.ID, 0) //nolint:dogsled // the pts pair is not what this test asserts on
	if !errors.Is(err, store.ErrFileMissing) {
		t.Fatalf("resend after erasure = (dup %v, %v), want ErrFileMissing", dup, err)
	}
	if after := historyLen(t, s, a.ID, store.PeerTypeUser, b.ID); after != before {
		t.Errorf("the refused resend wrote %d message rows", after-before)
	}
}

// The forward-resend half of the same ruling: a forward carrying a random id
// whose file has been erased is refused too, and writes nothing.
func TestForwardResendOfAnErasedFileIsRefused(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200141")
	b := mustUser(t, s, "+15559200142")
	c := mustUser(t, s, "+15559200143")

	f := storedFileWithBytes(t, s, a.ID)
	src, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0) //nolint:dogsled // only the stored row is needed
	if err != nil {
		t.Fatalf("seed send: %v", err)
	}
	sources := []store.ForwardSource{{FromID: a.ID, Date: src.Date, Text: src.Text, FileID: f.ID}}
	const randomID = 99
	_, fwd, err := s.ForwardMessages(ctx, b.ID, store.PeerTypeUser, c.ID, sources, []int64{randomID})
	if err != nil {
		t.Fatalf("forward: %v", err)
	}
	if len(fwd) != 1 {
		t.Fatalf("forward wrote %d messages, want 1", len(fwd))
	}
	deleteBoth(t, s, a.ID, src.LocalID, b.ID, src.PeerLocalID)
	deleteBoth(t, s, b.ID, fwd[0].Message.LocalID, c.ID, fwd[0].Message.PeerLocalID)
	if cs := sweep(t, s, future()); cs.Erased != 1 {
		t.Fatalf("counts = %+v, want one erased", cs)
	}

	before := historyLen(t, s, c.ID, store.PeerTypeUser, b.ID)
	_, _, err = s.ForwardMessages(ctx, b.ID, store.PeerTypeUser, c.ID, sources, []int64{randomID})
	if !errors.Is(err, store.ErrFileMissing) {
		t.Fatalf("forward-resend after erasure = %v, want ErrFileMissing rather than a replay", err)
	}
	if after := historyLen(t, s, c.ID, store.PeerTypeUser, b.ID); after != before {
		t.Errorf("the refused forward-resend wrote %d message rows", after-before)
	}
}

// MAIN-80 invariant 3: an erased file and one that never existed are the same
// outcome on the download path. The gate reads the row before it evaluates
// entitlement, so an absent row and an unknown id both fail the same way — and
// the errors have to be equal, not merely both non-nil, or the difference is an
// oracle over every file on the server.
func TestErasedFileIsIndistinguishableFromOneThatNeverExisted(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559200151")
	b := mustUser(t, s, "+15559200152")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)
	if c := sweep(t, s, future()); c.Erased != 1 {
		t.Fatalf("counts = %+v, want one erased", c)
	}

	_, erasedErr := s.FileForDownload(ctx, f.ID, f.AccessHash, b.ID)
	_, unknownErr := s.FileForDownload(ctx, f.ID+1_000_000, f.AccessHash, b.ID)
	if !errors.Is(erasedErr, store.ErrFileNotFound) {
		t.Fatalf("download of an erased file = %v, want ErrFileNotFound", erasedErr)
	}
	if erasedErr.Error() != unknownErr.Error() {
		t.Errorf("erased file answers %q, a file that never existed answers %q", erasedErr, unknownErr)
	}
}

// Criterion 9's second half: nothing the sweep hands an operator carries a
// file's access hash. It is the unguessable half of a download credential, and
// a sweep summary is exactly the kind of record that is logged whole.
func TestEraseCountsNeverCarryAnAccessHash(t *testing.T) {
	t.Parallel()
	s := open(t)
	a := mustUser(t, s, "+15559200161")
	b := mustUser(t, s, "+15559200162")

	f := storedFileWithBytes(t, s, a.ID)
	deletedBothSides(t, s, a, b, f.ID, 1)

	counts := sweep(t, s, future())
	rendered := fmt.Sprintf("%+v", counts)
	if strings.Contains(rendered, strconv.FormatInt(f.AccessHash, 10)) {
		t.Errorf("erase counts rendered the access hash of file %d: %s", f.ID, rendered)
	}
	if strings.Contains(rendered, strconv.FormatInt(f.ID, 10)) && counts.Erased != 1 {
		t.Errorf("erase counts rendered a file id: %s", rendered)
	}
}

// Criterion 7: the sweep drains a backlog larger than one scan window, in
// windows of at most batch rows, and takes each file in its own transaction.
func TestErasureSweepDrainsInBoundedWindows(t *testing.T) {
	t.Parallel()
	s := open(t)
	a := mustUser(t, s, "+15559200171")
	b := mustUser(t, s, "+15559200172")

	const files = 7
	ids := make([]int64, 0, files)
	for i := range files {
		f := storedFileWithBytes(t, s, a.ID)
		deletedBothSides(t, s, a, b, f.ID, int64(i+1))
		ids = append(ids, f.ID)
	}

	// A window of two forces the walk to page, so a sweep that erased only its
	// first window fails here.
	counts, err := s.SweepMediaErasure(context.Background(), future(), 2)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if counts.Erased != files {
		t.Fatalf("counts = %+v, want %d erased", counts, files)
	}
	for _, id := range ids {
		if rowPresent(t, s, id) || blobPresent(t, s, id) {
			t.Errorf("file %d survived a paged drain", id)
		}
	}
}

// The batch bound is refused rather than read as "no bound", the rule the scan
// and the upload-part sweep both follow.
func TestErasureSweepRejectsNonPositiveBatch(t *testing.T) {
	t.Parallel()
	s := open(t)
	if _, err := s.SweepMediaErasure(context.Background(), future(), 0); err == nil {
		t.Fatal("batch 0 was accepted")
	}
}
