package store

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgtype"

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
