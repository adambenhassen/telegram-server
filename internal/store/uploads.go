package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// ErrPartTooLarge is returned when one upload part exceeds MaxPartBytes, is
// empty, or names a part index outside the int32 column.
var ErrPartTooLarge = errors.New("upload part too large")

// ErrFileTooLarge is returned when an upload's parts would exceed the per-file
// byte cap.
var ErrFileTooLarge = errors.New("upload file too large")

// ErrUploadQuota is returned when a user's outstanding unassembled bytes would
// exceed their cap.
var ErrUploadQuota = errors.New("upload quota exceeded")

// ErrTooManyParts is returned when a user's outstanding part count would exceed
// the row cap.
var ErrTooManyParts = errors.New("too many upload parts")

// MaxPartBytes is the protocol maximum for one upload.saveFilePart part.
const MaxPartBytes = 512 * 1024

// MinPartBytesForRowCap is the smallest part size real clients choose. The
// per-user row cap is derived from it so that a client using ordinary part
// sizes is never rejected by the row cap before the byte cap.
const MinPartBytesForRowCap = 32 * 1024

// partIndexOf narrows a part index to the int32 the column stores. Truncating
// instead would alias a client-chosen index onto a different part's row.
func partIndexOf(i int) (int32, bool) {
	if i < 0 || i > math.MaxInt32 {
		return 0, false
	}
	return int32(i), true
}

// SaveUploadPart writes one part of an in-flight upload, enforcing the per-file
// and per-user byte caps. maxFileBytes is the per-file cap; the per-user
// outstanding cap is twice that, which is two concurrent max-size uploads. The
// same outstanding set is capped by row count, because a row costs more than
// its payload in index and WAL and a 1-byte part pays almost nothing for one.
//
// The bytes go to the blob store under a random key the row records; Postgres
// keeps the accounting only. The order is the contract, and it is the reverse
// of the assembled-blob rule:
//
//  1. The caps are evaluated and the row is committed BEFORE any byte leaves
//     the process. The sums are read back inside the same transaction as the
//     upsert, so what they measure is the state the write would produce — the
//     same property the payload column used to buy: a client re-saving part 0
//     in a loop is not billed twice, because there is no counter to
//     increment, only a SUM over rows the upsert has already deduplicated.
//     Over a cap, the transaction rolls back and the request is refused
//     having stored nothing.
//  2. The bytes are written after the commit. A crash between the two leaves a
//     row with no bytes, which is the recoverable direction: the cap
//     over-counts, assembly fails closed, and the sweep retires the row on
//     schedule. An object with no row is invisible to the row-driven sweep
//     and permanent, so the reverse order is not an option.
//
// The recorded size is measured from the payload this call received, never
// from anything the client declared, and it describes the object the row names
// and no other: every save draws a fresh key, so size and key move together.
//
// A re-save therefore leaves the object it replaced named by nothing, and this
// deletes it once the row commits. Without that, a client retrying one part in
// a loop grows stored bytes without bound while the row-based caps — which
// count rows, not objects — stay flat.
//
// Two deletes, not one, and together they are what makes that hold under
// concurrency as well as serially: this save's own object is dropped too if the
// row has moved off it by the time the bytes are down. The advisory lock orders
// the rows of two concurrent saves of one part but not their byte work, so
// without the second delete the later saver's cleanup can run before the
// earlier saver's write and leave an object no row names — reachable on demand,
// not by any failure. With it, an orphan needs this process or the backend to
// fail between the write and the cleanup.
//
// Lock: one advisory lock on user_id, taken first, so one account's concurrent
// saves serialise and two of them cannot both read a sum that is under the cap
// and both commit. It is the same per-owner advisory namespace lockOwners uses
// in messages.go. The lock is held only for the transaction's life: the blob
// write happens after the commit, so a hanging storage backend cannot pin it.
func (s *Store) SaveUploadPart(ctx context.Context, userID, fileID int64, partIndex int, payload []byte, maxFileBytes int64) error {
	if len(payload) == 0 || len(payload) > MaxPartBytes {
		return ErrPartTooLarge
	}
	idx, ok := partIndexOf(partIndex)
	if !ok {
		return ErrPartTooLarge
	}
	key, err := blob.NewPartKey()
	if err != nil {
		return err
	}
	size := int64(len(payload))

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if err = lockOwners(ctx, tx, userID); err != nil {
		return err
	}
	qtx := s.q.WithTx(tx)

	superseded, err := qtx.UpsertUploadPart(ctx, db.UpsertUploadPartParams{
		UserID:    userID,
		FileID:    fileID,
		PartIndex: idx,
		Size:      size,
		BlobKey:   key,
	})
	if err != nil {
		return fmt.Errorf("upsert upload part: %w", err)
	}

	fileBytes, err := qtx.FileOutstandingBytes(ctx, db.FileOutstandingBytesParams{UserID: userID, FileID: fileID})
	if err != nil {
		return fmt.Errorf("file outstanding bytes: %w", err)
	}
	if fileBytes > maxFileBytes {
		return ErrFileTooLarge
	}

	user, err := qtx.UserOutstanding(ctx, userID)
	if err != nil {
		return fmt.Errorf("user outstanding: %w", err)
	}
	if user.TotalBytes > 2*maxFileBytes {
		return ErrUploadQuota
	}
	maxParts := 2 * ((maxFileBytes + MinPartBytesForRowCap - 1) / MinPartBytesForRowCap)
	if user.Parts > maxParts {
		return ErrTooManyParts
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// The row is committed, so the cap holds whatever happens next. The bytes
	// go in after: a failure here leaves a row with no bytes, which the
	// assembly's fail-closed check and the sweep both handle.
	_, putErr := s.blobs.Put(ctx, key, bytes.NewReader(payload))

	// The commit moved the row onto the new key, so whatever sits at the old
	// one is already named by no row. It runs whether or not the write above
	// succeeded, because the commit orphaned it either way, and a failure is
	// logged rather than returned: the part itself is in the state its row
	// describes.
	if superseded != "" && superseded != key {
		if err = s.removePartBytes(ctx, superseded); err != nil {
			s.log.Error("remove superseded upload part bytes",
				"user_id", userID, "file_id", fileID, "part", partIndex, "err", err)
		}
	}

	// And the object this save just wrote, if the row has already moved off it.
	// That happens without any failure: two concurrent saves of one part
	// serialise their rows on the advisory lock but not their byte work, so the
	// later one's delete of this key can run before this write did, and the
	// write then lands under a key no row names. The check is AFTER the write
	// and not before it for exactly that reason — a check before would only
	// narrow the window, while a writer looking at the row it already committed
	// against can see the whole outcome. The ordering that makes it total: if
	// this read still sees our key, then it ran before any superseding commit,
	// so that save's delete of our key runs after this write and collects it;
	// if it does not, we collect it here.
	if putErr == nil {
		if err = s.dropUnnamedPartBytes(ctx, userID, fileID, partIndex, key); err != nil {
			s.log.Error("drop unnamed upload part bytes",
				"user_id", userID, "file_id", fileID, "part", partIndex, "err", err)
		}
	}

	if putErr != nil {
		s.log.Error("save upload part bytes", "user_id", userID, "file_id", fileID, "part", partIndex, "err", putErr)
		return fmt.Errorf("write part bytes: %w", putErr)
	}
	return nil
}

// dropUnnamedPartBytes deletes the object at key when the part's row no longer
// names it — because a concurrent save superseded it, or because a sweep or an
// assembly cleanup retired the row while the bytes were landing. An absent row
// is the same answer as a different key: nothing names these bytes.
//
// A read that fails leaves the object alone. Not knowing which key the row
// names is not a reason to delete bytes it may still name, and a failing
// database is the failure class the residual bound already covers.
func (s *Store) dropUnnamedPartBytes(ctx context.Context, userID, fileID int64, partIndex int, key string) error {
	current, _, found, err := s.UploadPartKey(ctx, userID, fileID, partIndex)
	if err != nil {
		return fmt.Errorf("re-read part key: %w", err)
	}
	if found && current == key {
		return nil
	}
	return s.removePartBytes(ctx, key)
}

// removePartBytes deletes one part's object. An empty key is the migration
// default, carried by every part that was in flight when the payload column
// went: the row names no object, so there is nothing to delete and that is not
// a failure. Passing it through would be — the local backend rejects an empty
// path rather than reporting it absent, and those rows are the oldest in the
// table, so an error on them would stall the sweep behind them for good.
func (s *Store) removePartBytes(ctx context.Context, key string) error {
	if key == "" {
		return nil
	}
	return s.blobs.Remove(ctx, key)
}

// UploadPartsSummary reports the part count, the highest part index and the
// total byte size of an in-flight upload. Assembly uses all three.
func (s *Store) UploadPartsSummary(ctx context.Context, userID, fileID int64) (parts int64, maxIndex int, totalBytes int64, err error) {
	r, err := s.q.UploadPartsSummary(ctx, db.UploadPartsSummaryParams{UserID: userID, FileID: fileID})
	if err != nil {
		return 0, 0, 0, fmt.Errorf("upload parts summary: %w", err)
	}
	return r.Parts, int(r.MaxIndex), r.TotalBytes, nil
}

// UploadPartKey reports one part's recorded blob key and size. ok=false when
// the part is absent.
func (s *Store) UploadPartKey(ctx context.Context, userID, fileID int64, partIndex int) (key string, size int64, ok bool, err error) {
	idx, ok := partIndexOf(partIndex)
	if !ok {
		return "", 0, false, nil
	}
	r, err := s.q.UploadPartKey(ctx, db.UploadPartKeyParams{
		UserID:    userID,
		FileID:    fileID,
		PartIndex: idx,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return "", 0, false, nil
	case err != nil:
		return "", 0, false, fmt.Errorf("upload part key: %w", err)
	}
	return r.BlobKey, r.Size, true, nil
}

// UploadPartRef names one in-flight part: its index, the key its bytes are
// read back from and the server-measured size that read is reconciled against.
type UploadPartRef struct {
	Index int
	Key   string
	Size  int64
}

// UploadPartRefs returns every part of one in-flight upload in index order,
// in one statement. Assembly takes both what it validates and what it reads
// from this, rather than a key lookup per part on each pass.
func (s *Store) UploadPartRefs(ctx context.Context, userID, fileID int64) ([]UploadPartRef, error) {
	rows, err := s.q.UploadPartRefs(ctx, db.UploadPartRefsParams{UserID: userID, FileID: fileID})
	if err != nil {
		return nil, fmt.Errorf("upload part refs: %w", err)
	}
	refs := make([]UploadPartRef, len(rows))
	for i, r := range rows {
		refs[i] = UploadPartRef{Index: int(r.PartIndex), Key: r.BlobKey, Size: r.Size}
	}
	return refs, nil
}

// ReadUploadPart returns ref's bytes. It is the one place recorded accounting
// and stored bytes are compared, and it is fail closed: a row whose bytes are
// gone or short — the crash window between the row commit and the byte write —
// is an error, not a short part. The caller's only sane response is to refuse
// the assembly, and the sweep retires the row on schedule.
func (s *Store) ReadUploadPart(ctx context.Context, ref UploadPartRef) ([]byte, error) {
	if ref.Key == "" {
		// The migration default: a part that was in flight when the payload
		// column went names no object and can never be assembled.
		return nil, fmt.Errorf("part %d: row names no stored bytes", ref.Index)
	}
	b, err := s.blobs.ReadAt(ctx, ref.Key, 0, ref.Size)
	if err != nil {
		return nil, fmt.Errorf("read part bytes: %w", err)
	}
	if int64(len(b)) != ref.Size {
		return nil, fmt.Errorf("part %d: read %d bytes, row records %d", ref.Index, len(b), ref.Size)
	}
	return b, nil
}

// UploadPart returns one part's payload. ok=false when the part is absent;
// every other failure is ReadUploadPart's, fail closed.
func (s *Store) UploadPart(ctx context.Context, userID, fileID int64, partIndex int) (payload []byte, ok bool, err error) {
	key, size, found, err := s.UploadPartKey(ctx, userID, fileID, partIndex)
	if err != nil || !found {
		return nil, false, err
	}
	b, err := s.ReadUploadPart(ctx, UploadPartRef{Index: partIndex, Key: key, Size: size})
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

// DeleteUploadParts drops every part of one in-flight upload, returning how
// many rows it retired. Called once an upload has been assembled.
//
// The bytes go before the rows and again after them, and both halves are
// load-bearing. Before: a failure between them leaves rows that name objects,
// which the sweep retires on schedule, rather than objects no row names, which
// nothing reclaims. After, in retirePartRows: a write that was still in flight
// during the first delete lands under a key this had already passed, and only
// the transaction that retired the row can collect it.
//
// The row delete is retirePartRows', one row per object actually deleted and
// conditional on the row still naming it, for the reason stated there. A blind
// delete over the whole upload would drop rows whose bytes it never touched: a
// save committing between the read above and the delete below has put a new key
// on its row and written the object for it, and taking that row away strands
// the object. The row it spares instead is an ordinary in-flight part, counted
// by the caps, retired by the next assembly or by the TTL sweep.
func (s *Store) DeleteUploadParts(ctx context.Context, userID, fileID int64) (int64, error) {
	refs, err := s.UploadPartRefs(ctx, userID, fileID)
	if err != nil {
		return 0, err
	}
	deleted := make([]db.DeleteUploadPartByKeyParams, 0, len(refs))
	for _, ref := range refs {
		idx, ok := partIndexOf(ref.Index)
		if !ok {
			// Unreachable: the index came out of the int32 column.
			return 0, fmt.Errorf("delete upload parts: part index %d out of range", ref.Index)
		}
		if err = s.removePartBytes(ctx, ref.Key); err != nil {
			s.log.Error("delete upload part bytes", "user_id", userID, "file_id", fileID, "err", err)
			return 0, fmt.Errorf("delete part bytes: %w", err)
		}
		deleted = append(deleted, db.DeleteUploadPartByKeyParams{
			UserID: userID, FileID: fileID, PartIndex: idx, BlobKey: ref.Key,
		})
	}
	n, err := s.retirePartRows(ctx, deleted)
	if err != nil {
		return 0, fmt.Errorf("delete upload parts: %w", err)
	}
	return n, nil
}

// PartBlobs returns the blob backend the store uses for in-flight upload part
// bytes. It is the same backend the parts are read back from, so a caller that
// needs to inspect or remove a part object uses this rather than opening its
// own store.
func (s *Store) PartBlobs() blob.Store { return s.blobs }

// ExpiredPartSweepBatch is how many expired parts one sweep pass removes. It
// bounds the statement, not the retention: SweepExpiredUploadParts repeats
// passes until one comes back short, so a backlog still drains in one sweep, in
// transactions whose lock and WAL footprint do not grow with it.
const ExpiredPartSweepBatch = 1000

// SweepExpiredUploadParts deletes every part stored before cutoff in passes of
// at most batch rows, returning how many it removed. The bytes go with the
// rows: each pass claims its batch, deletes the claimed objects, and finalises
// the row delete.
//
// It terminates on rows actually retired, never on rows claimed. The two
// differ: a part re-saved between a pass's claim and its finalise has moved
// onto a new key, the conditional delete spares it, and the pass retires fewer
// rows than it took. Counting the claim as progress would let an account that
// re-saves its expired parts on a timer keep every pass full and this loop
// running for as long as it cared to, since the re-save deliberately does not
// move the expiry date the claim selects on. A pass that retires fewer than
// batch rows ends the drain and leaves the remainder to the next sweep.
//
// cutoff is fixed for the whole drain rather than recomputed per pass, so what
// a sweep retires is decided once, at its start, and a part that expires
// mid-drain waits for the next one.
//
// A canceled context surfaces as an error from the pass it interrupts, with the
// count of what earlier passes already retired: each pass commits on its own,
// so an interrupted drain leaves less to do rather than nothing done.
func (s *Store) SweepExpiredUploadParts(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	var total int64
	for {
		n, err := s.deleteExpiredUploadParts(ctx, cutoff, batch)
		total += n
		if err != nil {
			return total, err
		}
		if n < int64(batch) {
			return total, nil
		}
	}
}

// deleteExpiredUploadParts retires up to batch parts stored before cutoff,
// oldest first, returning how many rows it actually removed — which is at most
// how many it claimed, and less when a re-save spared one.
//
// cutoff is compared against when a part was first stored, not when it was last
// written — re-saving a part does not extend its life, or an account could hold
// an outstanding set forever by touching it.
//
// The pass is three steps, and the order is the contract: claim, commit, then
// the storage work, then finalise.
//
//  1. Claim: the SQL takes its bounded batch and returns each row's primary key
//     and the blob key it names. It commits on its own, so no row lock is held
//     across a storage call: a hanging storage backend cannot pin one. It also
//     partitions nothing between replicas, and nothing here assumes it does —
//     both remaining steps are idempotent under a concurrent duplicate pass.
//  2. The bytes: each claimed object is deleted. Deleting an absent object is
//     a no-op, so a crash that left a row without bytes costs nothing here, and
//     a row the migration left with no key at all names no object to delete. A
//     failure stops the pass with the rows still in place, so the next pass
//     retries them rather than the bytes becoming unreachable.
//  3. Finalise: one delete per claimed row, naming both its primary key and the
//     blob key the pass deleted, and then the objects of the rows that actually
//     went, deleted a second time after that transaction commits. A re-save
//     that committed in the window between the claim and the byte delete has
//     renamed the row: it is not deleted and it is not counted. A save whose
//     bytes were still in flight when step 2 passed their key is what the
//     second delete collects — the claim can read a key whose object has not
//     landed yet exactly as the assembly cleanup can.
func (s *Store) deleteExpiredUploadParts(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	// Refused rather than read as "no bound": the unbounded DELETE is exactly
	// what this parameter exists to prevent, and a caller that computed a zero
	// batch has a bug that must not silently restore it.
	if batch <= 0 || batch > math.MaxInt32 {
		return 0, fmt.Errorf("delete expired upload parts: batch %d out of range", batch)
	}
	claimed, err := s.q.ClaimExpiredUploadParts(ctx, db.ClaimExpiredUploadPartsParams{
		Date: pgtype.Timestamptz{Time: cutoff, Valid: true},
		Lim:  int32(batch),
	})
	if err != nil {
		return 0, fmt.Errorf("claim expired upload parts: %w", err)
	}
	if len(claimed) == 0 {
		return 0, nil
	}
	for _, c := range claimed {
		if err = s.removePartBytes(ctx, c.BlobKey); err != nil {
			// The rows still name their objects, so the next pass retries them;
			// stopping here rather than dropping the rows is what keeps the
			// bytes reachable.
			return 0, fmt.Errorf("delete expired part bytes: %w", err)
		}
	}
	retired, err := s.finaliseExpiredUploadParts(ctx, claimed)
	if err != nil {
		return 0, fmt.Errorf("finalise expired upload parts: %w", err)
	}
	return retired, nil
}

// finaliseExpiredUploadParts drops the rows a sweep pass claimed and reports
// how many went.
func (s *Store) finaliseExpiredUploadParts(ctx context.Context, claimed []db.ClaimExpiredUploadPartsRow) (int64, error) {
	rows := make([]db.DeleteUploadPartByKeyParams, len(claimed))
	for i, c := range claimed {
		rows[i] = db.DeleteUploadPartByKeyParams(c)
	}
	return s.retirePartRows(ctx, rows)
}

// retirePartRows drops the rows whose objects a caller has just deleted, and
// reports how many went. It is the one place a parts row is deleted alongside
// its bytes, and it is one function rather than a rule each caller reimplements
// because both callers that had to follow it did not: the assembly cleanup
// dropped an upload's rows blind until this was pulled out of the sweep.
//
// Each delete names one row's primary key and requires it to still carry the
// blob key whose bytes went. The primary key is what makes it one row — the
// rows a deploy leaves behind all share the empty blob key, and a delete keyed
// on that alone would take every one of them at once, losing the sweep's
// per-batch bound exactly where it is needed. The blob key is what makes it
// conditional: a save that committed after the caller read the key has moved
// its row onto a new one and written the object for it, and that row must not
// be deleted, because deleting it would strand the object. Sparing it leaves an
// ordinary in-flight part, which the caps count and the next assembly or TTL
// sweep retires.
//
// One transaction, so a caller that reads its own count back sees all of the
// deletes or none.
//
// Then it deletes the objects of the rows it actually retired, a second time
// and after the commit, and that is the half that makes the whole thing hold.
// The caller's delete before this ran against the keys as they were when it
// read them, and a write for one of those keys can still be in flight at that
// moment: the row is not wrong — it names the key the write is landing under —
// so neither the conditional delete nor the writer's own re-read has anything
// to notice, and the object outlives the row. The rule that closes it is about
// ownership rather than order: whoever last changes a row's state owns the
// bytes at the key that row named. This transaction is that party for the rows
// it retired, so it collects them once it has committed. A write landing before
// this second delete is collected by it; a write landing after it belongs to a
// writer whose re-read now finds no row and collects its own. There is no third
// case.
//
// A failure here is logged, not returned: the rows are already gone, so the
// caller has nothing to retry, and an object no row names is the residue this
// design accepted from the start.
func (s *Store) retirePartRows(ctx context.Context, rows []db.DeleteUploadPartByKeyParams) (int64, error) {
	if len(rows) == 0 {
		return 0, nil
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	retired := make([]string, 0, len(rows))
	for _, r := range rows {
		n, err := qtx.DeleteUploadPartByKey(ctx, r)
		if err != nil {
			return 0, fmt.Errorf("delete part rows: %w", err)
		}
		if n > 0 {
			retired = append(retired, r.BlobKey)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("commit: %w", err)
	}

	// Safe to delete unconditionally: a part key is drawn from crypto/rand for
	// one save, so once the row naming it is gone no row can ever name it
	// again. Deleting an object that is already gone is a no-op, which is what
	// this is for every key whose bytes the caller's first delete did reach.
	for _, key := range retired {
		if err = s.removePartBytes(ctx, key); err != nil {
			s.log.Error("remove retired upload part bytes", "key", key, "err", err)
		}
	}
	return int64(len(retired)), nil
}

// PartOrphanResult reports what one orphan pass reclaimed.
type PartOrphanResult struct {
	// Objects is how many part objects the pass deleted.
	Objects int
	// Bytes is the total size of the objects it deleted.
	Bytes int64
}

// PartOrphanGateBatch is how many candidate keys one live-key lookup carries,
// and with it the whole memory footprint of a run: the enumeration streams, so
// the pass holds a batch rather than a listing, whatever the prefix contains.
// It bounds the Postgres round trips the same way — one lookup per batch of
// candidates, not one per object and not one per prefix.
const PartOrphanGateBatch = 500

// PartOrphanMargin is the extra age on top of the part TTL before an
// unaccounted object is eligible for reclamation. It covers the row sweep's
// own lag (one tick at ttl/4, plus the drain that tick performs) and one
// hour of clock skew between replicas and between the local clock and the
// object store's clock on a remote backend. The live-key gate carries
// correctness alone; this margin is defence in depth, not the safety
// control.
func PartOrphanMargin(ttl time.Duration) time.Duration {
	return ttl/4 + time.Hour
}

// ReclaimOrphanedPartBytes streams the parts prefix, reclaims part objects
// older than the cutoff whose key no row still names, and returns what it
// removed.
//
// The pass is safe in a way the row-driven sweep is not: it holds no lock a
// request path waits on, makes no storage call inside a transaction, and its
// only interaction with Postgres is a batched live-key lookup. Two replicas
// running it at once delete the same object twice, and the second delete is a
// no-op.
//
// cutoff is the earliest an object may be removed. The caller passes
// now-(TTL+margin); the pass clamps it to that floor so a misconfigured small
// cutoff cannot reach a live part. The margin (PartOrphanMargin) covers the
// row sweep's own lag plus one hour of clock skew; the live-key gate carries
// correctness alone, and this margin is defence in depth.
//
// What it acts on is narrower than what it sees, and each exclusion is a
// separate reason. A directory, a symlink or anything else not a regular file
// never came from the writer. The writer's temporary file is a write in
// progress, and unlinking one is destroying bytes a caller is still handing
// over — no assumption about how long a part write can take belongs in a
// delete path. What is left has to be a key NewPartKey could have produced:
// this pass pages candidates by walking paths rather than by reading
// upload_parts rows, which makes that shape check load-bearing for deletion
// rather than for a counter, so anything else parked under the prefix is
// unexplained and is left where it is for [blobscan] to report.
//
// One run streams the entire prefix, so every orphan is examined on every run
// regardless of where its key sorts and how many ineligible objects precede
// it. The enumeration is one directory read per run, linear in the number of
// part objects and not quadratic, and the pass holds a batch of candidates
// rather than a listing. The bound is therefore independent of process uptime
// and of the store's contents: an unaccounted byte is reclaimed on the first
// run after it crosses the cutoff, and the residual bound is TTL + margin +
// one sweep interval.
//
// There is no delete-time re-check, and the streaming order is what carries
// that. A row is committed before its bytes are written (MAIN-341's ordering
// rule), so any row naming an object the walk observed was committed before
// that object existed, and the gate query that follows the observation sees
// it. A row committed after the walk passed a directory draws a fresh key from
// crypto/rand, so it names no object this run ever saw.
func (s *Store) ReclaimOrphanedPartBytes(ctx context.Context, cutoff time.Time, partTTL time.Duration) (PartOrphanResult, error) {
	// Floor: the cutoff can never be younger than TTL+margin. A misconfigured
	// small cutoff is clamped rather than trusted.
	margin := PartOrphanMargin(partTTL)
	floor := time.Now().Add(-(partTTL + margin))
	if cutoff.After(floor) {
		cutoff = floor
	}

	var res PartOrphanResult
	batch := make([]blob.Entry, 0, PartOrphanGateBatch)
	// gate resolves one batch of candidates against the rows and removes what
	// no row names. It is where the only Postgres round trip of the pass is.
	gate := func() error {
		if len(batch) == 0 {
			return nil
		}
		keys := make([]string, len(batch))
		for i, e := range batch {
			keys[i] = e.Key
		}
		liveKeys, err := s.q.LivePartKeys(ctx, keys)
		if err != nil {
			return fmt.Errorf("live part keys: %w", err)
		}
		live := make(map[string]bool, len(liveKeys))
		for _, k := range liveKeys {
			live[k] = true
		}
		for _, e := range batch {
			if live[e.Key] {
				continue
			}
			if err := s.blobs.Remove(ctx, e.Key); err != nil {
				// Log what was already reclaimed before surfacing the error:
				// the partial result is the operator-visible record of this run.
				s.log.Info("reclaim orphaned part bytes: partial",
					"objects", res.Objects, "bytes", res.Bytes, "err", err)
				return fmt.Errorf("remove orphaned part %s: %w", e.Key, err)
			}
			res.Objects++
			res.Bytes += e.Size
		}
		batch = batch[:0]
		return nil
	}

	var gateErr error
	err := s.blobs.WalkPrefix(ctx, blob.PartsPrefix, func(e blob.Entry) error {
		if !e.Regular {
			return nil
		}
		key, temp := strings.CutSuffix(e.Key, blob.TempSuffix)
		if temp || !blob.ParsePartKey(key) {
			return nil
		}
		// Age gate: an object at or younger than the cutoff is never removed.
		if !e.ModTime.Before(cutoff) {
			return nil
		}
		batch = append(batch, e)
		if len(batch) < PartOrphanGateBatch {
			return nil
		}
		gateErr = gate()
		return gateErr
	})
	if gateErr != nil {
		return res, gateErr
	}
	if err != nil {
		return res, fmt.Errorf("walk part prefix: %w", err)
	}
	if err := gate(); err != nil {
		return res, err
	}
	return res, nil
}
