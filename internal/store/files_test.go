package store_test

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
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

// TestFilesRejectsNonPositiveID pins the invariant three readers already depend
// on and none of them enforce: the download gate's m.file_id = f.id, which with
// a row at id 0 would read every text message as an entitlement to that file;
// MediaErasureSummary's walk, which starts at after_id = 0 and so could never
// classify such a row; and the m.file_id <> 0 conjunct in MediaErasureScan,
// whose no-op argument holds only while no files row carries the sentinel.
// BIGSERIAL supplies a default and constrains nothing, so an explicit insert
// naming id is the way in. AllocateFile never names id, which is why this goes
// through raw SQL rather than the store API.
func TestFilesRejectsNonPositiveID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	conn, err := pgx.Connect(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	var uploaderID int64
	if err := conn.QueryRow(ctx, `INSERT INTO users DEFAULT VALUES RETURNING id`).Scan(&uploaderID); err != nil {
		t.Fatalf("create uploader: %v", err)
	}

	for _, id := range []int64{0, -1} {
		_, err := conn.Exec(ctx,
			`INSERT INTO files (id, uploader_id, access_hash, size, mime_type, file_name)
			 VALUES ($1, $2, 1, 0, 'text/plain', 'x.txt')`, id, uploaderID)
		if err == nil {
			t.Fatalf("insert at files.id = %d must be refused", id)
		}
		if !strings.Contains(err.Error(), "files_id_positive") {
			t.Fatalf("files.id = %d: want a files_id_positive violation, got %v", id, err)
		}
	}
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
	// A size that would wrap the signed sum negative is still over quota, not
	// under it.
	if _, err := s.AllocateFile(ctx, u.ID, math.MaxInt64, "text/plain", "huge.txt", quota); !errors.Is(err, store.ErrStorageQuota) {
		t.Fatalf("overflowing size: want ErrStorageQuota, got %v", err)
	}
	// Neither rejection inserted a row: the exact remaining 40 bytes still fit,
	// which they would not if 41 or MaxInt64 had been recorded.
	if _, err := s.AllocateFile(ctx, u.ID, 40, "text/plain", "c.txt", quota); err != nil {
		t.Fatalf("at quota after rejection: %v", err)
	}
	if _, err := s.AllocateFile(ctx, u.ID, 1, "text/plain", "d.txt", quota); !errors.Is(err, store.ErrStorageQuota) {
		t.Fatalf("full quota: want ErrStorageQuota, got %v", err)
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

// The allocation claim exists before the row is visible. If the eraser gets
// the files row first, its nonblocking claim check commits a no-op and retains
// the row; assembly then completes normally. This drives the real blob sweep,
// including its commit path, rather than a test-only row lock that rolls back.
func TestAllocateAndCompleteFileSurvivesEraserRowLockFirst(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u := mustUser(t, s, "+15559100025")

	const body = "assembled"
	claimed := make(chan int64)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAssembly := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAssembly()
	store.SetAssemblyClaimHook(s, func(fileID int64) {
		claimed <- fileID
		<-release
	})
	defer store.SetAssemblyClaimHook(s, nil)

	type assemblyResult struct {
		file store.File
		err  error
	}
	done := make(chan assemblyResult, 1)
	go func() {
		file, err := s.AllocateAndCompleteFile(ctx, u.ID, int64(len(body)), "text/plain", "hello.txt", bigQuota, func(file store.File) error {
			_, err := store.BlobsOf(s).Put(ctx, blob.Key(file.ID), bytes.NewReader([]byte(body)))
			return err
		})
		done <- assemblyResult{file: file, err: err}
	}()

	fileID := <-claimed
	tempKey := blob.Key(fileID) + blob.TempSuffix
	if _, err := store.BlobsOf(s).Put(ctx, tempKey, bytes.NewReader([]byte("pending"))); err != nil {
		t.Fatalf("plant temporary: %v", err)
	}

	counts, err := s.SweepBlobErasure(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("blob sweep: %v", err)
	}
	if counts.TempConsidered != 1 || counts.TempContended != 1 || counts.TempUnlinkAttempts != 0 {
		t.Fatalf("blob sweep counts = %+v, want one claim-contended candidate and no unlink", counts)
	}
	if _, err := store.BlobsOf(s).ReadAt(ctx, tempKey, 0, 1); err != nil {
		t.Fatalf("claim-contended temporary was removed: %v", err)
	}
	if err := store.BlobsOf(s).Remove(ctx, tempKey); err != nil {
		t.Fatalf("remove planted temporary: %v", err)
	}

	releaseAssembly()
	result := <-done
	if result.err != nil {
		t.Fatalf("complete assembly after eraser committed its no-op: %v", result.err)
	}
	if result.file.ID != fileID || !result.file.Stored {
		t.Fatalf("completed file = %+v, want id %d stored", result.file, fileID)
	}
	got, err := store.BlobsOf(s).ReadAt(ctx, blob.Key(fileID), 0, int64(len(body)))
	if err != nil {
		t.Fatalf("read assembled bytes: %v", err)
	}
	if string(got) != body {
		t.Fatalf("assembled bytes = %q, want %q", got, body)
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
	sender, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0) //nolint:dogsled // only the stored message is needed here
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
	if _, err := s.DeleteMessages(ctx, a.ID, []int64{sender.LocalID}, true); err != nil {
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
	if _, _, _, _, err := s.SendMessage(ctx, a.ID, b.ID, "here", 1, f.ID, 0); err != nil {
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

// The channel branch of the gate: membership entitles, and a ban, a
// non-membership and a soft-deleted post all revoke — as the same
// ErrFileNotFound the 1:1 branch returns.
func TestFileForDownloadChannelMember(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15559100061")
	member := mustUser(t, s, "+15559100062")
	stranger := mustUser(t, s, "+15559100063")

	f := allocate(t, s, creator.ID, 11)
	if err := s.MarkFileStored(ctx, f.ID); err != nil {
		t.Fatalf("mark stored: %v", err)
	}
	channelID, localID, err := store.SeedChannelPost(ctx, s, creator.ID, member.ID, f.ID)
	if err != nil {
		t.Fatalf("seed channel post: %v", err)
	}

	got, err := s.FileForDownload(ctx, f.ID, f.AccessHash, member.ID)
	if err != nil {
		t.Fatalf("member download: %v", err)
	}
	if got.ID != f.ID || !got.Stored {
		t.Fatalf("downloaded row wrong: %+v", got)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, stranger.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("non-member: want ErrFileNotFound, got %v", err)
	}

	// Membership of SOME channel is not membership of the posting one. Without
	// cp.channel_id = cm.channel_id in the join, outsider — a member of a
	// different channel and of nothing else — reads this channel's file.
	outsider := mustUser(t, s, "+15559100064")
	other := allocate(t, s, creator.ID, 11)
	if _, _, err := store.SeedChannelPost(ctx, s, creator.ID, outsider.ID, other.ID); err != nil {
		t.Fatalf("seed second channel: %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, outsider.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("member of another channel: want ErrFileNotFound, got %v", err)
	}

	// Membership entitles to the files a post in that channel carries, not to
	// every stored file: without cm.file_id = f.id, orphan — stored and posted
	// nowhere — is readable by any member of any channel.
	orphan := allocate(t, s, creator.ID, 11)
	if err := s.MarkFileStored(ctx, orphan.ID); err != nil {
		t.Fatalf("mark orphan stored: %v", err)
	}
	if _, err := s.FileForDownload(ctx, orphan.ID, orphan.AccessHash, member.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("file with no channel post: want ErrFileNotFound, got %v", err)
	}

	// A live ban revokes, and lifting it restores.
	future := time.Now().Add(time.Hour)
	if err := store.SetChannelBan(ctx, s, channelID, member.ID, &future); err != nil {
		t.Fatalf("ban: %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, member.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("banned member: want ErrFileNotFound, got %v", err)
	}
	if err := store.SetChannelBan(ctx, s, channelID, member.ID, nil); err != nil {
		t.Fatalf("unban: %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, member.ID); err != nil {
		t.Fatalf("after unban: %v", err)
	}

	// A lapsed ban is not a ban.
	past := time.Now().Add(-time.Hour)
	if err := store.SetChannelBan(ctx, s, channelID, member.ID, &past); err != nil {
		t.Fatalf("lapsed ban: %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, member.ID); err != nil {
		t.Fatalf("lapsed ban: %v", err)
	}

	// Soft-deleting the post revokes retrieval, as it does on the 1:1 branch.
	if err := store.SetChannelPostDeleted(ctx, s, channelID, localID); err != nil {
		t.Fatalf("delete post: %v", err)
	}
	if _, err := s.FileForDownload(ctx, f.ID, f.AccessHash, member.ID); !errors.Is(err, store.ErrFileNotFound) {
		t.Fatalf("deleted post: want ErrFileNotFound, got %v", err)
	}
}
