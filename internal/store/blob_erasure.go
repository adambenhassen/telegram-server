package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
)

// BlobErasureGateBatch bounds the ids held between a prefix walk and the
// database existence check. The walk continues after every batch, so a large
// blob tree cannot turn one reclaim pass into an unbounded allocation.
const BlobErasureGateBatch = 500

const blobErasureShardCount = 1 << 8

const blobErasureMaxSample = 64

// BlobErasureReport is the read-only classification used when destructive
// media erasure is disabled. It carries aggregates and a bounded sample of
// paths that the layout cannot explain, never an access hash.
type BlobErasureReport struct {
	Through int64

	Orphans     int
	OrphanBytes int64

	AbandonedTemps     int
	AbandonedTempBytes int64

	Accounted        int
	AboveSnapshot    int
	TempsInFlight    int
	Unexplained      int
	UnexplainedBytes int64
	UnexplainedPaths []string
}

func (r *BlobErasureReport) addUnexplained(e blob.Entry) {
	r.Unexplained++
	r.UnexplainedBytes += e.Size
	if len(r.UnexplainedPaths) < blobErasureMaxSample {
		r.UnexplainedPaths = append(r.UnexplainedPaths, e.Key)
	}
}

// BlobEraseCounts is what one destructive assembled-blob pass did. Temporary
// files and id-based orphans are separate because the former are reclaimed by
// mtime alone while the latter require the files-row absence transaction.
// Unexplained paths and fresh temporary files are retained and reported.
type BlobEraseCounts struct {
	Through int64

	OrphanConsidered   int
	OrphanErased       int
	OrphanErasedBytes  int64
	OrphanRetained     int
	OrphanUnlinkFailed int

	TempConsidered   int
	TempErased       int
	TempErasedBytes  int64
	TempInFlight     int
	TempUnlinkFailed int

	AboveSnapshot    int
	Unexplained      int
	UnexplainedBytes int64
	UnexplainedPaths []string
}

func (c *BlobEraseCounts) addUnexplained(e blob.Entry) {
	c.Unexplained++
	c.UnexplainedBytes += e.Size
	if len(c.UnexplainedPaths) < blobErasureMaxSample {
		c.UnexplainedPaths = append(c.UnexplainedPaths, e.Key)
	}
}

type blobErasureCandidate struct {
	key  string
	id   int64
	size int64
}

func walkAssembledBlobPrefixes(ctx context.Context, blobs blob.Store, fn func(blob.Entry) error) error {
	for shard := range blobErasureShardCount {
		if err := ctx.Err(); err != nil {
			return err
		}
		prefix := fmt.Sprintf("%02x/", shard)
		if err := blobs.WalkPrefix(ctx, prefix, fn); err != nil {
			return fmt.Errorf("walk assembled blob prefix %q: %w", prefix, err)
		}
	}
	return nil
}

// walkBlobTemps is deliberately a pass of its own. A temporary path is an
// in-progress write and is judged by mtime alone; it must never be folded into
// the id-based row-absence pass.
func walkBlobTemps(ctx context.Context, blobs blob.Store, through int64, olderThan time.Time, onCandidate func(blobErasureCandidate) error, addUnexplained func(blob.Entry), addAbove func(), addInFlight func()) error {
	return walkAssembledBlobPrefixes(ctx, blobs, func(e blob.Entry) error {
		if !e.Regular || !strings.HasSuffix(e.Key, blob.TempSuffix) {
			return nil
		}
		key := strings.TrimSuffix(e.Key, blob.TempSuffix)
		id, ok := blob.ParseKey(key)
		if !ok {
			addUnexplained(e)
			return nil
		}
		if id > through {
			addAbove()
			return nil
		}
		if !e.ModTime.Before(olderThan) {
			addInFlight()
			return nil
		}
		return onCandidate(blobErasureCandidate{key: e.Key, id: id, size: e.Size})
	})
}

// walkBlobOrphans is the id-based pass. Everything that is not a regular
// assembled key is unexplained, while temporary files are delegated to the
// separate mtime pass above. Only ids at or below the pre-walk ceiling can
// reach the row-absence gate.
func walkBlobOrphans(ctx context.Context, blobs blob.Store, through int64, onCandidate func(blobErasureCandidate) error, addUnexplained func(blob.Entry), addAbove func()) error {
	return walkAssembledBlobPrefixes(ctx, blobs, func(e blob.Entry) error {
		if !e.Regular {
			addUnexplained(e)
			return nil
		}
		if strings.HasSuffix(e.Key, blob.TempSuffix) {
			return nil
		}
		id, ok := blob.ParseKey(e.Key)
		if !ok {
			addUnexplained(e)
			return nil
		}
		if id > through {
			addAbove()
			return nil
		}
		return onCandidate(blobErasureCandidate{key: e.Key, id: id, size: e.Size})
	})
}

// BlobErasureSummary classifies the assembled keyspace without deleting or
// locking anything. The allocated sequence ceiling is read before either
// prefix pass, so a blob written while the tree is being listed is above this
// pass's authority and remains for the next one.
func (s *Store) BlobErasureSummary(ctx context.Context, tempOlderThan time.Time) (BlobErasureReport, error) {
	through, err := s.AllocatedFileIDCeiling(ctx)
	if err != nil {
		return BlobErasureReport{}, fmt.Errorf("blob erasure summary: ceiling: %w", err)
	}
	report := BlobErasureReport{Through: through}
	if err := walkBlobTemps(ctx, s.blobs, through, tempOlderThan,
		func(c blobErasureCandidate) error {
			report.AbandonedTemps++
			report.AbandonedTempBytes += c.size
			return nil
		}, report.addUnexplained,
		func() { report.AboveSnapshot++ },
		func() { report.TempsInFlight++ }); err != nil {
		return report, fmt.Errorf("blob erasure summary: temps: %w", err)
	}

	pending := make([]blobErasureCandidate, 0, BlobErasureGateBatch)
	resolve := func() error {
		if len(pending) == 0 {
			return nil
		}
		ids := make([]int64, len(pending))
		for i, c := range pending {
			ids[i] = c.id
		}
		existing, err := s.ExistingFileIDs(ctx, ids)
		if err != nil {
			return err
		}
		for _, c := range pending {
			if _, ok := existing[c.id]; ok {
				report.Accounted++
				continue
			}
			report.Orphans++
			report.OrphanBytes += c.size
		}
		pending = pending[:0]
		return nil
	}
	if err := walkBlobOrphans(ctx, s.blobs, through, func(c blobErasureCandidate) error {
		pending = append(pending, c)
		if len(pending) < BlobErasureGateBatch {
			return nil
		}
		return resolve()
	}, report.addUnexplained, func() { report.AboveSnapshot++ }); err != nil {
		return report, fmt.Errorf("blob erasure summary: orphans: %w", err)
	}
	if err := resolve(); err != nil {
		return report, fmt.Errorf("blob erasure summary: existing rows: %w", err)
	}
	return report, nil
}

// SweepBlobErasure reclaims assembled blobs whose ids are at or below a
// sequence snapshot and have no files row, plus temporary files old enough to
// be abandoned. It never enumerates the parts prefix.
func (s *Store) SweepBlobErasure(ctx context.Context, tempOlderThan time.Time) (BlobEraseCounts, error) {
	through, err := s.AllocatedFileIDCeiling(ctx)
	if err != nil {
		return BlobEraseCounts{}, fmt.Errorf("blob erasure sweep: ceiling: %w", err)
	}
	counts := BlobEraseCounts{Through: through}
	var firstErr error
	if err := walkBlobTemps(ctx, s.blobs, through, tempOlderThan,
		func(c blobErasureCandidate) error {
			counts.TempConsidered++
			if err := s.blobs.Remove(ctx, c.key); err != nil {
				counts.TempUnlinkFailed++
				if firstErr == nil {
					firstErr = fmt.Errorf("blob erasure sweep: unlink temporary %q: %w", c.key, err)
				}
				return nil
			}
			counts.TempErased++
			counts.TempErasedBytes += c.size
			return nil
		}, counts.addUnexplained,
		func() { counts.AboveSnapshot++ },
		func() { counts.TempInFlight++ }); err != nil {
		return counts, fmt.Errorf("blob erasure sweep: temps: %w", err)
	}

	pending := make([]blobErasureCandidate, 0, BlobErasureGateBatch)
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		ids := make([]int64, len(pending))
		for i, c := range pending {
			ids[i] = c.id
		}
		existing, err := s.ExistingFileIDs(ctx, ids)
		if err != nil {
			return fmt.Errorf("blob erasure: existing rows: %w", err)
		}
		for _, c := range pending {
			counts.OrphanConsidered++
			if _, ok := existing[c.id]; ok {
				counts.OrphanRetained++
				continue
			}
			removed, err := s.reclaimOrphanBlob(ctx, c)
			if err != nil {
				counts.OrphanUnlinkFailed++
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			if !removed {
				counts.OrphanRetained++
				continue
			}
			counts.OrphanErased++
			counts.OrphanErasedBytes += c.size
		}
		pending = pending[:0]
		return nil
	}
	if err := walkBlobOrphans(ctx, s.blobs, through, func(c blobErasureCandidate) error {
		pending = append(pending, c)
		if len(pending) < BlobErasureGateBatch {
			return nil
		}
		return flush()
	}, counts.addUnexplained, func() { counts.AboveSnapshot++ }); err != nil {
		return counts, fmt.Errorf("blob erasure sweep: orphans: %w", err)
	}
	if err := flush(); err != nil {
		return counts, err
	}
	return counts, firstErr
}

// reclaimOrphanBlob performs the final no-row check in a transaction and only
// then removes the object. The transaction carries no row lock and the storage
// call is the only operation in it after the check: uploads, sends and
// downloads do not wait on a files-row interlock for this class. File ids are
// never reused, so a committed absence remains the same decision after the
// short transaction completes.
func (s *Store) reclaimOrphanBlob(ctx context.Context, c blobErasureCandidate) (bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("blob erasure: check %q: begin: %w", c.key, err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	exists, err := s.q.WithTx(tx).FileExistsForBlob(ctx, c.id)
	if err != nil {
		return false, fmt.Errorf("blob erasure: check %q: %w", c.key, err)
	}
	if exists {
		return false, nil
	}
	if err := s.blobs.Remove(ctx, c.key); err != nil {
		return false, fmt.Errorf("blob erasure: unlink %q: %w", c.key, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("blob erasure: check %q: commit: %w", c.key, err)
	}
	return true, nil
}
