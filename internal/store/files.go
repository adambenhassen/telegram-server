package store

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/store/db"
)

// File is a stored uploaded file. AccessHash is 64 random bits drawn per row;
// it is deliberately not the peer access_hash placeholder (access_hash ==
// user_id), which is satisfiable by construction.
type File struct {
	ID         int64
	UploaderID int64
	AccessHash int64
	Size       int64
	MimeType   string
	FileName   string
	Stored     bool
	Date       time.Time
}

// ErrStorageQuota is returned when a new file would take an account past its
// total stored-bytes cap.
var ErrStorageQuota = errors.New("storage quota exceeded")

// ErrFileNotFound is returned by FileForDownload for every rejection: an id
// that does not exist, a wrong access hash, a file whose bytes were never
// stored, and a caller who owns no live message referencing it. They are one
// error on purpose — see the query's comment.
var ErrFileNotFound = errors.New("file not found")

func fileFromRow(r db.File) File {
	return File{
		ID:         r.ID,
		UploaderID: r.UploaderID,
		AccessHash: r.AccessHash,
		Size:       r.Size,
		MimeType:   r.MimeType,
		FileName:   r.FileName,
		Stored:     r.Stored,
		Date:       r.Date.Time,
	}
}

// newAccessHash draws a file's 64-bit access hash. It fails closed: a
// crypto/rand error is returned, never swallowed and never replaced with a
// fallback draw, and 0 is redrawn because it is the "no access hash" sentinel
// on the wire. The redraw is bounded so a broken RNG is an error rather than a
// spin — at 64 random bits, eight consecutive zero draws is not chance.
func newAccessHash() (int64, error) {
	var buf [8]byte
	for range 8 {
		if _, err := rand.Read(buf[:]); err != nil {
			return 0, fmt.Errorf("access hash: %w", err)
		}
		if h := int64(binary.BigEndian.Uint64(buf[:])); h != 0 { //nolint:gosec // G115: opaque 64-bit reinterpretation, sign irrelevant
			return h, nil
		}
	}
	return 0, errors.New("access hash: rand returned zero repeatedly")
}

// AllocateFile reserves a files row for an upload the caller is about to store,
// enforcing maxUserBytes over that account's existing files. The row is created
// with stored = false: the bytes are written to the blob store afterwards and
// MarkFileStored flips it, so a crashed assembly leaves an unreachable row
// rather than a file id that serves whatever is at its key.
//
// Lock: one advisory lock on uploaderID and no other lock of any kind, so two
// concurrent allocations for one account cannot both read a sum under the cap
// and both commit, and this transaction cannot be part of a lock cycle. Same
// reasoning as SaveUploadPart.
func (s *Store) AllocateFile(ctx context.Context, uploaderID, size int64, mimeType, fileName string, maxUserBytes int64) (File, error) {
	if size <= 0 {
		return File{}, fmt.Errorf("allocate file: size %d is not positive", size)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return File{}, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if err = lockOwners(ctx, tx, uploaderID); err != nil {
		return File{}, err
	}
	qtx := s.q.WithTx(tx)

	used, err := qtx.UserStoredBytes(ctx, uploaderID)
	if err != nil {
		return File{}, fmt.Errorf("user stored bytes: %w", err)
	}
	if used+size > maxUserBytes {
		return File{}, ErrStorageQuota
	}

	hash, err := newAccessHash()
	if err != nil {
		return File{}, err
	}
	row, err := qtx.InsertFile(ctx, db.InsertFileParams{
		UploaderID: uploaderID,
		AccessHash: hash,
		Size:       size,
		MimeType:   mimeType,
		FileName:   fileName,
	})
	if err != nil {
		return File{}, fmt.Errorf("insert file: %w", err)
	}
	if err = tx.Commit(ctx); err != nil {
		return File{}, fmt.Errorf("commit: %w", err)
	}
	return fileFromRow(row), nil
}

// MarkFileStored records that a file's bytes are in the blob store, making it
// downloadable. Returns ErrFileNotFound if no row moved, which means the id is
// absent or it was already marked.
func (s *Store) MarkFileStored(ctx context.Context, fileID int64) error {
	n, err := s.q.MarkFileStored(ctx, fileID)
	if err != nil {
		return fmt.Errorf("mark file stored: %w", err)
	}
	if n == 0 {
		return ErrFileNotFound
	}
	return nil
}

// FileForDownload resolves a download request to a file the caller is entitled
// to read. Every rejection is ErrFileNotFound.
func (s *Store) FileForDownload(ctx context.Context, fileID, accessHash, callerID int64) (File, error) {
	row, err := s.q.FileForDownload(ctx, db.FileForDownloadParams{
		ID:         fileID,
		AccessHash: accessHash,
		OwnerID:    callerID,
	})
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return File{}, ErrFileNotFound
	case err != nil:
		return File{}, fmt.Errorf("file for download: %w", err)
	}
	return fileFromRow(row), nil
}

// FilesByIDs loads stored files by id, keyed by id, for hydrating media onto
// message rows. Absent and unstored ids are simply missing from the map.
func (s *Store) FilesByIDs(ctx context.Context, ids []int64) (map[int64]File, error) {
	out := make(map[int64]File, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.q.FilesByIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("files by ids: %w", err)
	}
	for _, r := range rows {
		out[r.ID] = fileFromRow(r)
	}
	return out, nil
}
