package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErasureScanBatch is how many files rows one scan pass reads. It bounds the
// statement, not the report: MediaErasureSummary repeats passes until the
// window is walked, in reads whose footprint does not grow with the table.
const ErasureScanBatch = 1000

// ErasureCandidate is one file nothing live references, past the age cutoff,
// with the bytes a reclaim would free.
//
// It carries no access hash and must not grow one. That value is the
// unguessable half of a download credential, and a candidate report is exactly
// the kind of record that is logged whole.
type ErasureCandidate struct {
	ID   int64
	Size int64
}

// ErasureCounts tallies what one or more scan passes saw. The five outcome
// counts partition Scanned: every row read lands in exactly one of them, so
// "nothing to reclaim" is distinguishable from "reclaim held back", and by what.
//
// A file held back by more than one reason is counted under the first that
// applies, in the order the fields are declared. A live reference outranks the
// age gate deliberately: SkippedTooNew is then exactly the set that will become
// candidates as the clock advances, which is the number worth watching, rather
// than a mixture of those and files that are simply still in use.
type ErasureCounts struct {
	// Scanned is how many files rows the pass read.
	Scanned int
	// Unreferenced and UnreferencedBytes count stored files with no live
	// reference, past the cutoff.
	Unreferenced      int
	UnreferencedBytes int64
	// Unassembled and UnassembledBytes count files still marked not-stored,
	// past the cutoff: a crashed assembly, or one running right now. They are
	// a separate class because they are reclaimed by a different mechanism —
	// there are no bytes to unlink for a file whose assembly never finished.
	Unassembled      int
	UnassembledBytes int64
	// SkippedMessageRef counts files a non-deleted messages row names.
	SkippedMessageRef int
	// SkippedChannelRef counts files a non-deleted channel_messages row names
	// and no live messages row does.
	SkippedChannelRef int
	// SkippedTooNew counts unreferenced files newer than the cutoff. The age
	// gate is defence in depth, not the safety control: every media file on the
	// server passes through "stored, zero live references" exactly once during
	// a normal send, so the reference predicate alone matches files that are
	// being sent right now. What keeps a live file safe is the transactional
	// interlock on the files row, taken by every path that writes a reference.
	SkippedTooNew int
}

func (c *ErasureCounts) add(o ErasureCounts) {
	c.Scanned += o.Scanned
	c.Unreferenced += o.Unreferenced
	c.UnreferencedBytes += o.UnreferencedBytes
	c.Unassembled += o.Unassembled
	c.UnassembledBytes += o.UnassembledBytes
	c.SkippedMessageRef += o.SkippedMessageRef
	c.SkippedChannelRef += o.SkippedChannelRef
	c.SkippedTooNew += o.SkippedTooNew
}

// ErasureScan is one bounded pass over the files table.
type ErasureScan struct {
	// Unreferenced names stored files with no live reference, past the cutoff.
	Unreferenced []ErasureCandidate
	// Unassembled names not-stored files past the cutoff.
	Unassembled []ErasureCandidate
	// Counts tallies every row this pass read, named or skipped.
	Counts ErasureCounts
	// LastID is the highest file id this pass read, and the AfterID the next
	// pass resumes from. Zero when the pass read nothing.
	LastID int64
	// Done reports that the pass reached the end of its window: it came back
	// short of the batch, so there is nothing left above LastID to read.
	Done bool
}

// MediaErasureScan reads one bounded window of the files table and classifies
// every row in it. It is read-only: it deletes nothing, unlinks nothing, and
// takes no lock, so a send, a forward or a download never waits on it.
//
// olderThan is the age cutoff — only files created strictly before it can be
// named. afterID is the exclusive lower bound of the window, throughID the
// inclusive upper bound, with a non-positive throughID meaning "no upper
// bound"; file ids are a dense BIGSERIAL starting at 1, so no real row is
// excluded by that sentinel. A caller walking the table pins throughID to a
// snapshot taken before the walk, which is what makes the walk terminate on a
// server still accepting uploads.
//
// Naming is not deciding. A candidate here is a file that had no live reference
// at the instant this statement read it; a reference can be created immediately
// afterwards, and nothing in this call prevents that. What makes an eventual
// erase safe is the interlock on the files row taken by every path that writes
// a reference, not this predicate and not the age cutoff.
func (s *Store) MediaErasureScan(ctx context.Context, olderThan time.Time, afterID, throughID int64, batch int) (ErasureScan, error) {
	// Refused rather than read as "no bound", for the reason
	// deleteExpiredUploadParts refuses it: the unbounded read is what the
	// parameter exists to prevent, and a caller that computed a zero batch has
	// a bug that must not silently restore it.
	if batch <= 0 || batch > math.MaxInt32 {
		return ErasureScan{}, fmt.Errorf("media erasure scan: batch %d out of range", batch)
	}
	if throughID <= 0 {
		throughID = math.MaxInt64
	}

	rows, err := s.q.MediaErasureScan(ctx, db.MediaErasureScanParams{
		OlderThan: pgtype.Timestamptz{Time: olderThan, Valid: true},
		AfterID:   afterID,
		ThroughID: throughID,
		Lim:       int32(batch),
	})
	if err != nil {
		return ErasureScan{}, fmt.Errorf("media erasure scan: %w", err)
	}

	scan := ErasureScan{Done: len(rows) < batch}
	for _, r := range rows {
		scan.Counts.Scanned++
		scan.LastID = r.ID
		c := ErasureCandidate{ID: r.ID, Size: r.Size}
		switch {
		case r.MessageRef:
			scan.Counts.SkippedMessageRef++
		case r.ChannelRef:
			scan.Counts.SkippedChannelRef++
		case !r.Aged:
			scan.Counts.SkippedTooNew++
		case r.Stored:
			scan.Unreferenced = append(scan.Unreferenced, c)
			scan.Counts.Unreferenced++
			scan.Counts.UnreferencedBytes += c.Size
		default:
			scan.Unassembled = append(scan.Unassembled, c)
			scan.Counts.Unassembled++
			scan.Counts.UnassembledBytes += c.Size
		}
	}
	return scan, nil
}

// MediaErasureSummary walks the whole files table in passes of at most batch
// rows and returns the tallies, without retaining a single file id. It is the
// operator-facing shape: how much is reclaimable, and how much is held back by
// what. A caller that needs the ids themselves pages MediaErasureScan.
//
// The upper bound is snapshotted before the walk starts. files.id is BIGSERIAL
// and never reused, so a file created while the walk runs is above the snapshot
// and is left for the next one — which also makes the walk terminate rather
// than chase an id space a busy server keeps extending.
func (s *Store) MediaErasureSummary(ctx context.Context, olderThan time.Time, batch int) (ErasureCounts, error) {
	if batch <= 0 || batch > math.MaxInt32 {
		return ErasureCounts{}, fmt.Errorf("media erasure summary: batch %d out of range", batch)
	}
	through, err := s.q.MaxFileID(ctx)
	if err != nil {
		return ErasureCounts{}, fmt.Errorf("media erasure summary: max file id: %w", err)
	}

	var counts ErasureCounts
	for after := int64(0); after < through; {
		scan, err := s.MediaErasureScan(ctx, olderThan, after, through, batch)
		if err != nil {
			return counts, err
		}
		counts.add(scan.Counts)
		if scan.Done {
			break
		}
		after = scan.LastID
	}
	return counts, nil
}

// EraseCounts is what one erasure sweep did and what it declined to do.
//
// Erased, Contended and Retained partition Considered: every candidate the
// sweep took up lands in exactly one of those three, so "nothing was reclaimed"
// is distinguishable from "reclaim was refused", and by what. UnlinkFailed is
// not a fourth class — it refines Erased, counting the subset whose row went
// but whose bytes did not. Summing all four over-counts what the sweep took up,
// which is why the distinction is spelled out here rather than left to be
// inferred from the field order.
//
// It carries no file id and no access hash. An operator needs the size of what
// moved, not an enumeration of whose media it was, and a sweep summary is
// exactly the kind of record that is logged whole.
type EraseCounts struct {
	// Considered is how many candidates the sweep's scans named.
	Considered int
	// Erased and ErasedBytes count files whose row was removed and whose bytes
	// were unlinked. The bytes are the row's recorded size, which is also what
	// the account's quota was carrying for it.
	Erased      int
	ErasedBytes int64
	// Contended counts candidates whose files row another transaction held. A
	// reference is being written to that file right now, or another replica's
	// sweep reached it first; either way it is not this pass's, and the next
	// sweep reads it again.
	Contended int
	// Retained counts candidates the erase transaction refused: the file gained
	// a reference, or stopped meeting the age or stored condition, between the
	// scan that named it and the exclusive lock. The transaction rolled back,
	// so the channel references it had cleared are still in place.
	Retained int
	// UnlinkFailed counts files whose row is committed gone but whose bytes the
	// blob store would not remove. They are counted in Erased as well — the
	// erase happened, the row is gone and the quota is freed — and this is how
	// many of those left their bytes behind. The bytes are reclaimable and
	// nothing else names them; the disk reclamation pass is what collects them,
	// which is the same state a crash between the commit and the unlink leaves.
	UnlinkFailed int

	// The unassembled fields are the same outcomes for rows whose stored flag
	// was false at scan time. They are separate from the ordinary fields so an
	// operator can distinguish completed media reclaimed from crashed upload
	// rows reclaimed, even though both remove a files row and its exact key.
	UnassembledConsidered   int
	UnassembledErased       int
	UnassembledErasedBytes  int64
	UnassembledContended    int
	UnassembledRetained     int
	UnassembledUnlinkFailed int
}

// eraseOutcome is which of EraseCounts' classes one candidate landed in.
type eraseOutcome int

const (
	erased eraseOutcome = iota
	contended
	retained
)

// SweepMediaErasure removes the files nothing live references and unlinks their
// bytes, walking the files table in windows of at most batch rows.
//
// This is the destructive pass. Everything above it in this file names
// candidates and decides nothing; this is where a name becomes a delete, and
// the blob volume it unlinks from has no backup and no restore path.
//
// olderThan is the age cutoff, fixed for the whole sweep rather than recomputed
// per window, so what one sweep reclaims is decided once at its start — the
// same rule the upload-part sweep follows.
//
// The upper bound is snapshotted before the walk, for the reason
// MediaErasureSummary snapshots it: files.id is BIGSERIAL and never reused, so
// a file uploaded while the sweep runs is above the snapshot and is left to the
// next one, which is also what makes the walk terminate on a server still
// accepting uploads.
//
// Every replica may run this concurrently. Nothing is partitioned between them
// and nothing here assumes it is: a row a second sweep already holds is skipped
// rather than waited on, and an unlink of an already-unlinked key is a no-op.
//
// A candidate whose bytes could not be unlinked does not stop the sweep. Its
// row is already committed gone, so stopping would leave the same reclaimable
// bytes behind while also abandoning every candidate after it; the count says
// how many, and the first error is returned once the walk is finished.
func (s *Store) SweepMediaErasure(ctx context.Context, olderThan time.Time, batch int) (EraseCounts, error) {
	var counts EraseCounts
	if batch <= 0 || batch > math.MaxInt32 {
		return counts, fmt.Errorf("media erasure sweep: batch %d out of range", batch)
	}
	through, err := s.q.MaxFileID(ctx)
	if err != nil {
		return counts, fmt.Errorf("media erasure sweep: max file id: %w", err)
	}

	var unlinkErr error
	for after := int64(0); after < through; {
		scan, err := s.MediaErasureScan(ctx, olderThan, after, through, batch)
		if err != nil {
			return counts, err
		}
		// MediaErasureScan orders by id, so the candidates arrive ascending and
		// are taken in that order. It is the ordering the reference interlock
		// established for this lock class: a caller holding one files row and
		// reaching for another can only ever reach upward, so two transactions
		// naming the same pair cannot hold them the opposite way round.
		for _, c := range scan.Unreferenced {
			counts.Considered++
			out, err := s.eraseCandidate(ctx, c, olderThan, true)
			if err != nil {
				return counts, err
			}
			switch out {
			case contended:
				counts.Contended++
			case retained:
				counts.Retained++
			case erased:
				counts.Erased++
				counts.ErasedBytes += c.Size
				// Only now, with the row's deletion committed. The reverse
				// order is the one thing this pass must never do: it leaves a
				// row the download gate still admits over bytes that are gone,
				// which is a fourth download outcome, and a crash in that
				// window makes it permanent.
				if err := s.blobs.Remove(ctx, blob.Key(c.ID)); err != nil {
					counts.UnlinkFailed++
					if unlinkErr == nil {
						unlinkErr = fmt.Errorf("media erasure sweep: unlink file %d: %w", c.ID, err)
					}
				}
			}
		}
		// Not-stored rows are a separate reclaim class, but they use the same
		// row interlock and the same row-first ordering. A live assembly holds
		// the row FOR SHARE from before Put through MarkFileStored's commit, so
		// LockFileForErase skips it rather than queueing behind it. If the
		// assembly's connection dies, the lock dies with it and the row becomes
		// reclaimable. DeleteUnassembledFile still decides again under an
		// exclusive hold, so an assembly that completes or gains a live
		// reference is retained. Age is an additional gate, not the safety
		// control; MAIN-338 remains the follow-up for assembly duration policy.
		for _, c := range scan.Unassembled {
			counts.UnassembledConsidered++
			out, err := s.eraseCandidate(ctx, c, olderThan, false)
			if err != nil {
				return counts, err
			}
			switch out {
			case contended:
				counts.UnassembledContended++
			case retained:
				counts.UnassembledRetained++
			case erased:
				counts.UnassembledErased++
				counts.UnassembledErasedBytes += c.Size
				if err := s.blobs.Remove(ctx, blob.Key(c.ID)); err != nil {
					counts.UnassembledUnlinkFailed++
					if unlinkErr == nil {
						unlinkErr = fmt.Errorf("media erasure sweep: unlink unassembled file %d: %w", c.ID, err)
					}
				}
			}
		}
		if scan.Done {
			break
		}
		after = scan.LastID
	}
	return counts, unlinkErr
}

// eraseCandidate runs one file's erase transaction: take the row, release the
// references held by already-deleted channel posts, delete the row. One file
// per transaction, and the transaction does nothing else.
//
// The hold is a chat-wide stall while it lasts. A forward reaches the file
// interlock holding the destination chat's row lock and up to 200 per-owner
// advisory locks, so a forward parked on this row parks every other send and
// membership mutation in that chat behind it. That blocking is the control
// rather than a defect — it is how a reference insert loses to an erase instead
// of racing it — but its duration is the whole cost, so nothing that can be
// done outside the hold is done inside it: the candidate scan, the byte unlink
// and every filesystem operation are all on the other side of the commit.
//
// The one thing that cannot move outside is the reference re-check, and the
// reason is the ordering, not the cost. See LockFileForErase.
func (s *Store) eraseCandidate(ctx context.Context, c ErasureCandidate, olderThan time.Time, stored bool) (eraseOutcome, error) {
	if s.eraseHook != nil {
		s.eraseHook(c.ID)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return retained, fmt.Errorf("erase file %d: begin: %w", c.ID, err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)

	// No row means the row is contended or already gone. Both are somebody
	// else's business and neither is an error: a held row is a reference being
	// written or another replica's sweep, and an absent one is a sweep that
	// already finished the job.
	if _, err = qtx.LockFileForErase(ctx, c.ID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contended, nil
		}
		return retained, fmt.Errorf("erase file %d: lock: %w", c.ID, err)
	}
	if _, err = qtx.ClearDeletedChannelFileRefs(ctx, &c.ID); err != nil {
		return retained, fmt.Errorf("erase file %d: clear channel refs: %w", c.ID, err)
	}
	var n int64
	if stored {
		n, err = qtx.DeleteUnreferencedFile(ctx, db.DeleteUnreferencedFileParams{
			ID:        c.ID,
			OlderThan: pgtype.Timestamptz{Time: olderThan, Valid: true},
		})
	} else {
		n, err = qtx.DeleteUnassembledFile(ctx, db.DeleteUnassembledFileParams{
			ID:        c.ID,
			OlderThan: pgtype.Timestamptz{Time: olderThan, Valid: true},
		})
	}
	if err != nil {
		return retained, fmt.Errorf("erase file %d: delete: %w", c.ID, err)
	}
	if n == 0 {
		// Rolled back rather than committed, so the channel references cleared
		// above go back too. A file that survives keeps every record of which
		// posts named it; committing that half would quietly detach a retained
		// file from the deleted posts that are the only remaining evidence of
		// where it went.
		return retained, nil
	}
	if err = tx.Commit(ctx); err != nil {
		return retained, fmt.Errorf("erase file %d: commit: %w", c.ID, err)
	}
	return erased, nil
}
