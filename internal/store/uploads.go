package store

import (
	"bytes"
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
// from anything the client declared, and the upsert never lowers it below the
// size of the object that may still exist at the part's key.
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

	if err = qtx.UpsertUploadPart(ctx, db.UpsertUploadPartParams{
		UserID:    userID,
		FileID:    fileID,
		PartIndex: idx,
		Size:      size,
		BlobKey:   key,
	}); err != nil {
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
	if _, err = s.blobs.Put(ctx, key, bytes.NewReader(payload)); err != nil {
		s.log.Error("save upload part bytes", "user_id", userID, "file_id", fileID, "part", partIndex, "err", err)
		return fmt.Errorf("write part bytes: %w", err)
	}
	return nil
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

// UploadPart returns one part's payload, read from the blob store at the key
// the row records. ok=false when the part is absent. A row whose bytes are
// gone — the crash window between the row commit and the byte write — is an
// error, not a short part: the caller's only sane response is to fail closed,
// and the sweep retires the row on schedule.
func (s *Store) UploadPart(ctx context.Context, userID, fileID int64, partIndex int) (payload []byte, ok bool, err error) {
	key, size, found, err := s.UploadPartKey(ctx, userID, fileID, partIndex)
	if err != nil || !found {
		return nil, false, err
	}
	b, err := s.blobs.ReadAt(ctx, key, 0, size)
	if err != nil {
		return nil, false, fmt.Errorf("read part bytes: %w", err)
	}
	if int64(len(b)) != size {
		return nil, false, fmt.Errorf("part %d: read %d bytes, row records %d", partIndex, len(b), size)
	}
	return b, true, nil
}

// DeleteUploadParts drops every part of one in-flight upload, returning the row
// count. Called once an upload has been assembled. The bytes go first: a
// failure deleting them leaves rows that name objects, which the sweep retires
// on schedule, rather than objects no row names, which nothing reclaims.
func (s *Store) DeleteUploadParts(ctx context.Context, userID, fileID int64) (int64, error) {
	keys, err := s.partKeys(ctx, userID, fileID)
	if err != nil {
		return 0, err
	}
	for _, key := range keys {
		if err = s.blobs.Remove(ctx, key); err != nil {
			s.log.Error("delete upload part bytes", "user_id", userID, "file_id", fileID, "err", err)
			return 0, fmt.Errorf("delete part bytes: %w", err)
		}
	}
	n, err := s.q.DeleteUploadParts(ctx, db.DeleteUploadPartsParams{UserID: userID, FileID: fileID})
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

// partKeys lists the blob keys an upload's rows currently name.
func (s *Store) partKeys(ctx context.Context, userID, fileID int64) ([]string, error) {
	rows, err := s.q.UploadPartKeys(ctx, db.UploadPartKeysParams{UserID: userID, FileID: fileID})
	if err != nil {
		return nil, fmt.Errorf("upload part keys: %w", err)
	}
	return rows, nil
}

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
// It terminates because each pass either comes back short — nothing more to
// take under this cutoff — or removes exactly batch rows from a finite set the
// sweep itself never adds to. cutoff is fixed for the whole drain rather than
// recomputed per pass, so what a sweep retires is decided once, at its start,
// and a part that expires mid-drain waits for the next one.
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
// oldest first, returning the row count. A pass returning batch means there is
// more to take.
//
// cutoff is compared against when a part was first stored, not when it was last
// written — re-saving a part does not extend its life, or an account could hold
// an outstanding set forever by touching it.
//
// The pass is three steps, and the order is the contract: claim, commit, then
// the storage work, then finalise.
//
//  1. Claim: the SQL takes its FOR UPDATE SKIP LOCKED batch and returns the
//     keys the batch names. The claim commits on its own, so the row locks are
//     released before any storage call: a hanging storage backend cannot pin
//     them.
//  2. The bytes: each claimed object is deleted. Deleting an absent object is
//     a no-op, so a crash that left a row without bytes costs nothing here. A
//     failure here stops the pass with the rows still in place, so the next
//     pass retries them rather than the bytes becoming unreachable.
//  3. Finalise: the row delete is conditional on the row still naming the key
//     the pass deleted, never blind on the primary key, so a re-save that
//     committed in the window between the claim and the byte delete keeps its
//     row and its new object.
func (s *Store) deleteExpiredUploadParts(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	// Refused rather than read as "no bound": the unbounded DELETE is exactly
	// what this parameter exists to prevent, and a caller that computed a zero
	// batch has a bug that must not silently restore it.
	if batch <= 0 || batch > math.MaxInt32 {
		return 0, fmt.Errorf("delete expired upload parts: batch %d out of range", batch)
	}
	keys, err := s.q.ClaimExpiredUploadParts(ctx, db.ClaimExpiredUploadPartsParams{
		Date: pgtype.Timestamptz{Time: cutoff, Valid: true},
		Lim:  int32(batch),
	})
	if err != nil {
		return 0, fmt.Errorf("claim expired upload parts: %w", err)
	}
	if len(keys) == 0 {
		return 0, nil
	}
	for _, key := range keys {
		if err = s.blobs.Remove(ctx, key); err != nil {
			// The rows still name their objects, so the next pass retries them;
			// stopping here rather than dropping the rows is what keeps the
			// bytes reachable.
			return 0, fmt.Errorf("delete expired part bytes: %w", err)
		}
	}
	if err = s.finaliseExpiredUploadParts(ctx, keys); err != nil {
		return 0, fmt.Errorf("finalise expired upload parts: %w", err)
	}
	return int64(len(keys)), nil
}

// finaliseExpiredUploadParts drops the rows a sweep pass claimed, one
// conditional delete per key: the row goes only if it still names the key its
// bytes were deleted under. A re-save that committed in the window between the
// claim and the byte delete has renamed the row, and its row and its new
// object both survive.
func (s *Store) finaliseExpiredUploadParts(ctx context.Context, keys []string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit
	qtx := s.q.WithTx(tx)
	for _, key := range keys {
		if err = qtx.DeleteUploadPartsByKey(ctx, key); err != nil {
			return fmt.Errorf("delete expired part rows: %w", err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}
