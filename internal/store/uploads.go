package store

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

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
// Ordering, and why the caps are checked AFTER the write: the sums are read
// back inside the same transaction as the upsert, so what they measure is the
// state the write produced. That removes the whole class of accounting bug a
// separate counter has — a client re-saving part 0 in a loop is not billed
// twice, because there is no counter to increment, only a SUM over rows the
// upsert has already deduplicated. Over a cap, the transaction rolls back and
// the part is gone.
//
// Lock: one advisory lock on user_id, taken first, so one account's concurrent
// saves serialise and two of them cannot both read a sum that is under the cap
// and both commit. It is the same per-owner advisory namespace lockOwners uses
// in messages.go. This transaction takes that one lock and no other lock of any
// kind, so it cannot be part of a cycle and cannot deadlock against the
// send/fan-out paths, which take advisory locks under the chats row lock.
func (s *Store) SaveUploadPart(ctx context.Context, userID, fileID int64, partIndex int, payload []byte, maxFileBytes int64) error {
	if len(payload) == 0 || len(payload) > MaxPartBytes {
		return ErrPartTooLarge
	}
	idx, ok := partIndexOf(partIndex)
	if !ok {
		return ErrPartTooLarge
	}
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
		Payload:   payload,
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

// UploadPart returns one part's payload. ok=false when the part is absent.
func (s *Store) UploadPart(ctx context.Context, userID, fileID int64, partIndex int) (payload []byte, ok bool, err error) {
	idx, ok := partIndexOf(partIndex)
	if !ok {
		return nil, false, nil
	}
	p, err := s.q.UploadPartPayload(ctx, db.UploadPartPayloadParams{
		UserID:    userID,
		FileID:    fileID,
		PartIndex: idx,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, false, nil
	case err != nil:
		return nil, false, fmt.Errorf("upload part payload: %w", err)
	}
	return p, true, nil
}

// DeleteUploadParts drops every part of one in-flight upload, returning the row
// count. Called once an upload has been assembled.
func (s *Store) DeleteUploadParts(ctx context.Context, userID, fileID int64) (int64, error) {
	n, err := s.q.DeleteUploadParts(ctx, db.DeleteUploadPartsParams{UserID: userID, FileID: fileID})
	if err != nil {
		return 0, fmt.Errorf("delete upload parts: %w", err)
	}
	return n, nil
}

// ExpiredPartSweepBatch is how many expired parts one sweep pass removes. It
// bounds the statement, not the retention: SweepExpiredUploadParts repeats
// passes until one comes back short, so a backlog still drains in one sweep, in
// transactions whose lock and WAL footprint do not grow with it.
const ExpiredPartSweepBatch = 1000

// SweepExpiredUploadParts deletes every part stored before cutoff in passes of
// at most batch rows, returning how many it removed.
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

// deleteExpiredUploadParts drops up to batch parts stored before cutoff, oldest
// first, returning the row count. A pass returning batch means there is more to
// take.
//
// cutoff is compared against when a part was first stored, not when it was last
// written — re-saving a part does not extend its life, or an account could hold
// an outstanding set forever by touching it.
//
// This is the only delete in M5 that removes stored bytes, and it can only ever
// remove parts no message references: a part row is deleted at assembly, so
// anything left is an upload that was never sent.
func (s *Store) deleteExpiredUploadParts(ctx context.Context, cutoff time.Time, batch int) (int64, error) {
	// Refused rather than read as "no bound": the unbounded DELETE is exactly
	// what this parameter exists to prevent, and a caller that computed a zero
	// batch has a bug that must not silently restore it.
	if batch <= 0 || batch > math.MaxInt32 {
		return 0, fmt.Errorf("delete expired upload parts: batch %d out of range", batch)
	}
	n, err := s.q.DeleteExpiredUploadParts(ctx, db.DeleteExpiredUploadPartsParams{
		Date: pgtype.Timestamptz{Time: cutoff, Valid: true},
		Lim:  int32(batch),
	})
	if err != nil {
		return 0, fmt.Errorf("delete expired upload parts: %w", err)
	}
	return n, nil
}
