package store_test

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/store"
)

const maxFile = 600 * 1024

func part(b byte, n int) []byte { return bytes.Repeat([]byte{b}, n) }

func TestSaveAndReadUploadPart(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	want := part('a', 1024)
	if err := s.SaveUploadPart(ctx, u.ID, 777, 0, want, maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := s.UploadPart(ctx, u.ID, 777, 0)
	if err != nil || !ok {
		t.Fatalf("read part: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload mismatch: got %d bytes", len(got))
	}

	if _, ok, err = s.UploadPart(ctx, u.ID, 777, 5); err != nil || ok {
		t.Fatalf("missing part: ok=%v err=%v", ok, err)
	}
}

func TestSaveUploadPartRetryReplacesPayload(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000002")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := s.SaveUploadPart(ctx, u.ID, 1, 0, part('a', 2048), maxFile); err != nil {
		t.Fatalf("save first: %v", err)
	}
	want := part('b', 4096)
	if err := s.SaveUploadPart(ctx, u.ID, 1, 0, want, maxFile); err != nil {
		t.Fatalf("save retry: %v", err)
	}

	got, ok, err := s.UploadPart(ctx, u.ID, 1, 0)
	if err != nil || !ok {
		t.Fatalf("read part: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatal("retry did not replace payload")
	}
	parts, maxIndex, total, err := s.UploadPartsSummary(ctx, u.ID, 1)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 1 || maxIndex != 0 || total != 4096 {
		t.Fatalf("summary after retry: parts=%d maxIndex=%d total=%d", parts, maxIndex, total)
	}
}

func TestSaveUploadPartRejectsBadSizes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000003")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	err = s.SaveUploadPart(ctx, u.ID, 1, 0, part('a', store.MaxPartBytes+1), maxFile)
	if !errors.Is(err, store.ErrPartTooLarge) {
		t.Fatalf("oversized part: %v", err)
	}
	if err = s.SaveUploadPart(ctx, u.ID, 1, 0, nil, maxFile); !errors.Is(err, store.ErrPartTooLarge) {
		t.Fatalf("empty part: %v", err)
	}

	parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 1)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 0 {
		t.Fatalf("rejected parts left %d rows", parts)
	}
}

func TestSaveUploadPartFileCap(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000004")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := s.SaveUploadPart(ctx, u.ID, 777, 0, part('a', store.MaxPartBytes), maxFile); err != nil {
		t.Fatalf("save first: %v", err)
	}
	err = s.SaveUploadPart(ctx, u.ID, 777, 1, part('b', store.MaxPartBytes), maxFile)
	if !errors.Is(err, store.ErrFileTooLarge) {
		t.Fatalf("over file cap: %v", err)
	}

	parts, maxIndex, total, err := s.UploadPartsSummary(ctx, u.ID, 777)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 1 || maxIndex != 0 || total != store.MaxPartBytes {
		t.Fatalf("rollback failed: parts=%d maxIndex=%d total=%d", parts, maxIndex, total)
	}
}

func TestSaveUploadPartUserQuota(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000005")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	// Each file stays under the per-file cap; together they pass 2*maxFile.
	for i, fileID := range []int64{10, 11} {
		if err := s.SaveUploadPart(ctx, u.ID, fileID, 0, part('a', store.MaxPartBytes), maxFile); err != nil {
			t.Fatalf("save file %d: %v", i, err)
		}
	}
	err = s.SaveUploadPart(ctx, u.ID, 12, 0, part('a', store.MaxPartBytes), maxFile)
	if !errors.Is(err, store.ErrUploadQuota) {
		t.Fatalf("over user quota: %v", err)
	}

	parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 12)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 0 {
		t.Fatalf("quota rollback failed: parts=%d", parts)
	}
}

// Two accounts naming the same client-chosen file_id must not see each other's
// bytes: user_id is part of the primary key and of every lookup.
func TestUploadPartsIsolatedPerUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a, err := s.CreateUser(ctx, "+15559000006")
	if err != nil {
		t.Fatalf("create user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15559000007")
	if err != nil {
		t.Fatalf("create user b: %v", err)
	}

	aWant, bWant := part('a', 1024), part('b', 2048)
	if err := s.SaveUploadPart(ctx, a.ID, 777, 0, aWant, maxFile); err != nil {
		t.Fatalf("save a: %v", err)
	}
	if err := s.SaveUploadPart(ctx, b.ID, 777, 0, bWant, maxFile); err != nil {
		t.Fatalf("save b: %v", err)
	}

	for _, tc := range []struct {
		name string
		id   int64
		want []byte
	}{{"a", a.ID, aWant}, {"b", b.ID, bWant}} {
		got, ok, err := s.UploadPart(ctx, tc.id, 777, 0)
		if err != nil || !ok {
			t.Fatalf("read %s: ok=%v err=%v", tc.name, ok, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Fatalf("user %s read the wrong payload", tc.name)
		}
		parts, _, total, err := s.UploadPartsSummary(ctx, tc.id, 777)
		if err != nil {
			t.Fatalf("summary %s: %v", tc.name, err)
		}
		if parts != 1 || total != int64(len(tc.want)) {
			t.Fatalf("summary %s: parts=%d total=%d", tc.name, parts, total)
		}
	}
}

func TestUploadPartsSummary(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000008")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := range 3 {
		if err := s.SaveUploadPart(ctx, u.ID, 5, i, part('a', 1000), maxFile); err != nil {
			t.Fatalf("save part %d: %v", i, err)
		}
	}
	parts, maxIndex, total, err := s.UploadPartsSummary(ctx, u.ID, 5)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 3 || maxIndex != 2 || total != 3000 {
		t.Fatalf("summary: parts=%d maxIndex=%d total=%d", parts, maxIndex, total)
	}

	parts, maxIndex, total, err = s.UploadPartsSummary(ctx, u.ID, 6)
	if err != nil {
		t.Fatalf("empty summary: %v", err)
	}
	if parts != 0 || maxIndex != -1 || total != 0 {
		t.Fatalf("empty summary: parts=%d maxIndex=%d total=%d", parts, maxIndex, total)
	}
}

func TestDeleteUploadParts(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000009")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := range 2 {
		if err := s.SaveUploadPart(ctx, u.ID, 1, i, part('a', 100), maxFile); err != nil {
			t.Fatalf("save: %v", err)
		}
	}
	if err := s.SaveUploadPart(ctx, u.ID, 2, 0, part('b', 100), maxFile); err != nil {
		t.Fatalf("save other file: %v", err)
	}

	n, err := s.DeleteUploadParts(ctx, u.ID, 1)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted %d rows, want 2", n)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 1); err != nil || parts != 0 {
		t.Fatalf("file 1 after delete: parts=%d err=%v", parts, err)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 2); err != nil || parts != 1 {
		t.Fatalf("file 2 after delete: parts=%d err=%v", parts, err)
	}
}

func TestDeleteExpiredUploadParts(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000010")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := s.SaveUploadPart(ctx, u.ID, 1, 0, part('a', 100), maxFile); err != nil {
		t.Fatalf("save: %v", err)
	}

	n, err := s.DeleteExpiredUploadParts(ctx, time.Now().Add(-time.Hour))
	if err != nil {
		t.Fatalf("sweep past cutoff: %v", err)
	}
	if n != 0 {
		t.Fatalf("past cutoff deleted %d rows", n)
	}

	n, err = s.DeleteExpiredUploadParts(ctx, time.Now().Add(time.Hour))
	if err != nil {
		t.Fatalf("sweep future cutoff: %v", err)
	}
	if n != 1 {
		t.Fatalf("future cutoff deleted %d rows, want 1", n)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 1); err != nil || parts != 0 {
		t.Fatalf("after sweep: parts=%d err=%v", parts, err)
	}
}
