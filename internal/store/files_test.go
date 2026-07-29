package store_test

import (
	"context"
	"errors"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/store"
)

const bigQuota = int64(1) << 31

func allocate(t *testing.T, s *store.Store, uploaderID, size int64) store.File {
	t.Helper()
	f, err := s.AllocateFile(context.Background(), uploaderID, size, "text/plain", "hello.txt", bigQuota)
	if err != nil {
		t.Fatalf("allocate file: %v", err)
	}
	return f
}

func TestAllocateFileReservesUnstoredRow(t *testing.T) {
	t.Parallel()
	s := open(t)
	u := mustUser(t, s, "+15559100001")

	f := allocate(t, s, u.ID, 11)
	if f.ID == 0 {
		t.Fatal("file id is zero")
	}
	if f.AccessHash == 0 {
		t.Fatal("access hash is the zero sentinel")
	}
	if f.Stored {
		t.Fatal("fresh row is already stored")
	}
	if f.Size != 11 || f.MimeType != "text/plain" || f.FileName != "hello.txt" || f.UploaderID != u.ID {
		t.Fatalf("row wrong: %+v", f)
	}

	other := allocate(t, s, u.ID, 11)
	if other.AccessHash == f.AccessHash {
		t.Fatalf("two allocations share access hash %d", f.AccessHash)
	}
	if other.ID == f.ID {
		t.Fatalf("two allocations share id %d", f.ID)
	}
}

func TestAllocateFileEnforcesStorageQuota(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15559100011")

	const quota = int64(100)
	if _, err := s.AllocateFile(ctx, u.ID, 60, "text/plain", "a.txt", quota); err != nil {
		t.Fatalf("first allocate: %v", err)
	}
	if _, err := s.AllocateFile(ctx, u.ID, 41, "text/plain", "b.txt", quota); !errors.Is(err, store.ErrStorageQuota) {
		t.Fatalf("over quota: want ErrStorageQuota, got %v", err)
	}
	// The rejected allocation inserted nothing.
	if _, err := s.AllocateFile(ctx, u.ID, 40, "text/plain", "c.txt", quota); err != nil {
		t.Fatalf("at quota after rejection: %v", err)
	}
}

func TestMarkFileStoredIsOnce(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15559100021")

	f := allocate(t, s, u.ID, 11)
	if err := s.MarkFileStored(ctx, f.ID); err != nil {
		t.Fatalf("mark stored: %v", err)
	}
	if err := s.MarkFileStored(ctx, f.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("second mark: want ErrFileNotFound, got %v", err)
	}
	if err := s.MarkFileStored(ctx, f.ID+(1<<40)); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("absent id: want ErrFileNotFound, got %v", err)
	}
}

// The gate: every rejection is the identical ErrFileNotFound, so a caller
// cannot tell an absent id from a wrong hash from a file that is not theirs.
func TestFileForDownloadGate(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559100031")
	b := mustUser(t, s, "+15559100032")
	stranger := mustUser(t, s, "+15559100033")

	f := allocate(t, s, a.ID, 11)
	if err := s.MarkFileStored(ctx, f.ID); err != nil {
		t.Fatalf("mark stored: %v", err)
	}
	sender, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID) //nolint:dogsled // only the stored message is needed here
	if err != nil {
		t.Fatalf("send: %v", err)
	}

	got, err := s.FileForDownload(ctx, f.ID, f.AccessHash, a.ID)
	if err != nil {
		t.Fatalf("owner download: %v", err)
	}
	if got.ID != f.ID || got.AccessHash != f.AccessHash || !got.Stored {
		t.Fatalf("downloaded row wrong: %+v", got)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, b.ID); err != nil {
		t.Fatalf("recipient download: %v", err)
	}

	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash+1, a.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("wrong hash: want ErrFileNotFound, got %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID+(1<<40), f.AccessHash, a.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("absent id: want ErrFileNotFound, got %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, stranger.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("stranger: want ErrFileNotFound, got %v", err)
	}

	// Soft-deleting the message revokes retrieval on both sides.
	if _, err := s.DeleteMessages(ctx, a.ID, []int64{sender.LocalID}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, a.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("after delete, sender: want ErrFileNotFound, got %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, b.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("after delete, recipient: want ErrFileNotFound, got %v", err)
	}
}

func TestFileForDownloadRejectsUnstoredFile(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a := mustUser(t, s, "+15559100041")
	b := mustUser(t, s, "+15559100042")

	f := allocate(t, s, a.ID, 11)
	if _, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID); err != nil {
		t.Fatalf("send: %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, a.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("unstored file: want ErrFileNotFound, got %v", err)
	}
}

func TestFilesByIDsReturnsOnlyStored(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15559100051")

	stored := allocate(t, s, u.ID, 11)
	unstored := allocate(t, s, u.ID, 12)
	if err := s.MarkFileStored(ctx, stored.ID); err != nil {
		t.Fatalf("mark stored: %v", err)
	}

	got, err := s.FilesByIDs(ctx, []int64{stored.ID, unstored.ID, stored.ID + (1 << 40)})
	if err != nil {
		t.Fatalf("files by ids: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("got %d files, want only the stored one: %+v", len(got), got)
	}
	if got[stored.ID].Size != 11 || !got[stored.ID].Stored {
		t.Fatalf("stored file wrong: %+v", got[stored.ID])
	}

	empty, err := s.FilesByIDs(ctx, nil)
	if err != nil {
		t.Fatalf("files by nil ids: %v", err)
	}
	if empty == nil || len(empty) != 0 {
		t.Fatalf("nil ids = %+v, want an empty non-nil map", empty)
	}
}
