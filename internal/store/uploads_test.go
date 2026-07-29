package store_test

import (
	"bytes"
	"context"
	"errors"
	"math"
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

// A row costs far more than a 1-byte payload, so the byte cap alone lets a
// client hold ~2*maxFileBytes rows. rowCapFile is small enough that the row cap
// binds first: at 1 byte a part, 2*maxFileBytes+1 parts would be needed to trip
// the byte cap and only rowCapParts+1 to trip the row cap.
const (
	rowCapFile  = 3 * store.MinPartBytesForRowCap
	rowCapParts = 2 * (rowCapFile / store.MinPartBytesForRowCap)
)

func TestSaveUploadPartRowCap(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000011")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := range rowCapParts {
		if err := s.SaveUploadPart(ctx, u.ID, 1, i, part('a', 1), rowCapFile); err != nil {
			t.Fatalf("save part %d: %v", i, err)
		}
	}
	err = s.SaveUploadPart(ctx, u.ID, 1, rowCapParts, part('a', 1), rowCapFile)
	if !errors.Is(err, store.ErrTooManyParts) {
		t.Fatalf("over row cap: %v", err)
	}

	parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, 1)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != rowCapParts {
		t.Fatalf("row cap rollback failed: parts=%d, want %d", parts, rowCapParts)
	}
}

// The point of MinPartBytesForRowCap: a client using ordinary part sizes must
// hit the byte cap, never the row cap.
//
// The margin has to be built deliberately. When maxFileBytes is a multiple of
// MinPartBytesForRowCap, exact 32 KiB parts trip both caps on the same call and
// the byte error only wins because its check is written first — that asserts
// statement order, not a margin. marginFile is 2.5 parts, so the row cap rounds
// up to 6 while the byte cap allows only 5: part 6 is over bytes with the row
// count still strictly under its cap, whichever order the two checks run in.
const (
	marginFile    = 5 * store.MinPartBytesForRowCap / 2
	marginByteCap = 2 * marginFile / store.MinPartBytesForRowCap // 5 parts
	marginPerFile = 2                                            // 2*32 KiB <= marginFile, so the per-file cap never fires
	marginRowCap  = 6                                            // 2*ceil(marginFile/32 KiB)
)

func TestSaveUploadPartRowCapNoFalsePositive(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000012")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	for i := range marginByteCap {
		if err := s.SaveUploadPart(ctx, u.ID, int64(i/marginPerFile), i%marginPerFile, part('a', store.MinPartBytesForRowCap), marginFile); err != nil {
			t.Fatalf("save part %d: %v", i, err)
		}
	}
	// marginByteCap+1 parts is over the byte cap and still under the row cap.
	if marginByteCap+1 > marginRowCap {
		t.Fatalf("test setup has no margin: byte cap trips at %d parts, row cap at %d", marginByteCap+1, marginRowCap+1)
	}
	err = s.SaveUploadPart(ctx, u.ID, marginByteCap/marginPerFile, marginByteCap%marginPerFile, part('a', store.MinPartBytesForRowCap), marginFile)
	if !errors.Is(err, store.ErrUploadQuota) {
		t.Fatalf("32 KiB parts over the byte cap: got %v, want ErrUploadQuota", err)
	}
}

func TestUploadRowCapIsPerUser(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	a, err := s.CreateUser(ctx, "+15559000013")
	if err != nil {
		t.Fatalf("create user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15559000014")
	if err != nil {
		t.Fatalf("create user b: %v", err)
	}

	for i := range rowCapParts {
		if err := s.SaveUploadPart(ctx, a.ID, 1, i, part('a', 1), rowCapFile); err != nil {
			t.Fatalf("save a part %d: %v", i, err)
		}
	}
	if err := s.SaveUploadPart(ctx, a.ID, 1, rowCapParts, part('a', 1), rowCapFile); !errors.Is(err, store.ErrTooManyParts) {
		t.Fatalf("a over row cap: %v", err)
	}
	if err := s.SaveUploadPart(ctx, b.ID, 1, 0, part('b', 1), rowCapFile); err != nil {
		t.Fatalf("b blocked by a: %v", err)
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

// TestSaveUploadPartRejectsOutOfRangeIndex covers both branches of the index
// narrowing. An index past MaxInt32 is the one that matters: truncating it
// instead of rejecting it would alias onto part 0 and silently overwrite it, so
// the assertion is on part 0's bytes, not only on the error.
func TestSaveUploadPartRejectsOutOfRangeIndex(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000010")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	want := part('a', 64)
	if err = s.SaveUploadPart(ctx, u.ID, 42, 0, want, maxFile); err != nil {
		t.Fatalf("save part 0: %v", err)
	}
	for _, idx := range []int{-1, math.MaxInt32 + 1} {
		if err = s.SaveUploadPart(ctx, u.ID, 42, idx, part('b', 64), maxFile); !errors.Is(err, store.ErrPartTooLarge) {
			t.Fatalf("index %d: %v, want ErrPartTooLarge", idx, err)
		}
	}

	got, ok, err := s.UploadPart(ctx, u.ID, 42, 0)
	if err != nil || !ok {
		t.Fatalf("read part 0: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("part 0 was overwritten: got %q", got)
	}
}
