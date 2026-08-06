package store_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

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
