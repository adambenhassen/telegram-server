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

// ErrFileMissing is returned when a message would reference a files row that is
// not there to lock. It is separate from ErrFileNotFound because it is not a
// download rejection and answers nothing about a file the caller was not
// already holding: both paths that reach it arrive carrying a file id read off
// the caller's own upload or off a message the caller owns.
var ErrFileMissing = errors.New("referenced file is missing")

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
	// Written as a subtraction rather than used+size so a size near MaxInt64
	// cannot wrap the sum negative and be admitted. size > 0 is rejected above
	// and maxUserBytes is config-side and non-negative, so this is exact.
	if size > maxUserBytes || used > maxUserBytes-size {
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

// lockFileRefs takes the shared row lock on every file a message is about to
// reference, and fails closed with ErrFileMissing when one of them is not there
// to lock. Zero is the "no media" sentinel and is dropped, so a text message
// takes no lock and issues no query at all.
//
// Lock order. This is the last lock class a reference-creating transaction
// takes: the chats row lock and the per-owner advisory locks are both already
// held by the time any caller gets here, and none of them is ever taken after.
// A transaction holding a files row therefore waits for nothing else in this
// server, which is what makes a cycle through it impossible. Within the class
// the ids are taken ascending, so a caller naming several files cannot hold one
// and wait for another a second caller holds the other way round — the same
// rule lockOwners follows, and the one an eraser has to follow too.
func lockFileRefs(ctx context.Context, qtx *db.Queries, ids ...int64) error {
	live := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id != 0 {
			live = append(live, id)
		}
	}
	for _, id := range ascendingUnique(live) {
		_, err := qtx.LockFileForReference(ctx, id)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return ErrFileMissing
		case err != nil:
			return fmt.Errorf("lock file %d: %w", id, err)
		}
	}
	return nil
}

// MaxFileID returns the highest id among the files rows that exist, or zero
// on an empty table. It is a ceiling on the rows, not on the id space: once a
// row is deleted it falls below the erased ids, which is what a row-paging
// walk like MediaErasureSummary wants and what a disk classification does not.
func (s *Store) MaxFileID(ctx context.Context) (int64, error) {
	id, err := s.q.MaxFileID(ctx)
	if err != nil {
		return 0, fmt.Errorf("max file id: %w", err)
	}
	return id, nil
}

// AllocatedFileIDCeiling returns the highest file id ever allocated, or zero
// on an empty table. files.id is BIGSERIAL and the sequence never reuses an id
// and never falls back, so the value is a snapshot of the id space that stays
// meaningful after it is read: an id at or below it was allocated before the
// read, every id allocated afterwards is above it, and it never drops below an
// id whose row was once committed.
//
// That is what makes a pass over the blob store's disk sound. Reading this
// first and then listing the tree means a blob written during the listing is
// above the snapshot and therefore out of scope by construction, rather than
// classified against a table read that predates it.
func (s *Store) AllocatedFileIDCeiling(ctx context.Context) (int64, error) {
	id, err := s.q.AllocatedFileIDCeiling(ctx)
	if err != nil {
		return 0, fmt.Errorf("allocated file id ceiling: %w", err)
	}
	return id, nil
}

// ExistingFileIDs returns which of ids the files table still has a row for,
// stored or not. Ids with no row are simply absent from the set.
func (s *Store) ExistingFileIDs(ctx context.Context, ids []int64) (map[int64]struct{}, error) {
	out := make(map[int64]struct{}, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.q.ExistingFileIDs(ctx, ids)
	if err != nil {
		return nil, fmt.Errorf("existing file ids: %w", err)
	}
	for _, id := range rows {
		out[id] = struct{}{}
	}
	return out, nil
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
