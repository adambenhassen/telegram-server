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

	n, err := s.SweepExpiredUploadParts(ctx, time.Now().Add(-time.Hour), sweepBatch)
	if err != nil {
		t.Fatalf("sweep past cutoff: %v", err)
	}
	if n != 0 {
		t.Fatalf("past cutoff deleted %d rows", n)
	}

	n, err = s.SweepExpiredUploadParts(ctx, time.Now().Add(time.Hour), sweepBatch)
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

// sweepBatch is a bound wide enough that the tests not about batching drain in
// one pass.
const sweepBatch = 1000

// TestUploadPartExpiresFromFirstStore is the retention invariant: expiry is
// measured from when a part was first stored, so a client re-saving the same
// part cannot hold an outstanding set past its TTL by touching it forever.
//
// The assertion is on the row's own stored date rather than on a wall clock:
// the sweep compares against that column, and reading it back is the only way
// to tell "the re-save did not move it" from "the test was fast enough".
func TestUploadPartExpiresFromFirstStore(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	u, err := s.CreateUser(ctx, "+15559000030")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	if err := s.SaveUploadPart(ctx, u.ID, 1, 0, part('a', 100), maxFile); err != nil {
		t.Fatalf("first save: %v", err)
	}
	stored, err := store.UploadPartDate(ctx, s, u.ID, 1, 0)
	if err != nil {
		t.Fatalf("read date: %v", err)
	}

	// The retry a real client makes: same part, new bytes, later.
	if err := s.SaveUploadPart(ctx, u.ID, 1, 0, part('b', 100), maxFile); err != nil {
		t.Fatalf("re-save: %v", err)
	}
	resaved, err := store.UploadPartDate(ctx, s, u.ID, 1, 0)
	if err != nil {
		t.Fatalf("read date after re-save: %v", err)
	}
	if !resaved.Equal(stored) {
		t.Fatalf("re-save moved the expiry clock: %s -> %s", stored, resaved)
	}
	// The re-save still wins on content — dedupe semantics are unchanged.
	got, ok, err := s.UploadPart(ctx, u.ID, 1, 0)
	if err != nil || !ok {
		t.Fatalf("read part: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, part('b', 100)) {
		t.Fatalf("re-saved payload not stored")
	}

	// A sweep at first-store time + TTL takes it, however recent the re-save.
	n, err := s.SweepExpiredUploadParts(ctx, stored.Add(time.Millisecond), sweepBatch)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 1 {
		t.Fatalf("sweep deleted %d rows, want 1", n)
	}
}

// seedExpiredParts stores n parts of one file for a fresh user and returns a
// cutoff every one of them is expired under.
func seedExpiredParts(t *testing.T, s *store.Store, phone string, n int) (userID int64, cutoff time.Time) {
	t.Helper()
	ctx := context.Background()
	u, err := s.CreateUser(ctx, phone)
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	for i := range n {
		if err := s.SaveUploadPart(ctx, u.ID, 1, i, part('a', 100), maxFile); err != nil {
			t.Fatalf("save %d: %v", i, err)
		}
	}
	return u.ID, time.Now().Add(time.Hour)
}

// TestDeleteExpiredUploadPartsPassBounded pins the per-statement bound: one pass
// removes at most the batch, whatever is expired. Unbounded, the sweep is one
// DELETE across every account's expired rows, holding locks and writing WAL in
// proportion to whatever accumulated.
func TestDeleteExpiredUploadPartsPassBounded(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	userID, cutoff := seedExpiredParts(t, s, "+15559000031", 5)

	const batch = 2
	n, err := store.DeleteExpiredUploadPartsPass(ctx, s, cutoff, batch)
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if n != batch {
		t.Fatalf("pass deleted %d rows, want the bound of %d", n, batch)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, userID, 1); err != nil || parts != 3 {
		t.Fatalf("after one pass: parts=%d err=%v", parts, err)
	}
}

// TestSweepExpiredUploadPartsDrains is the shipped sweep: with more expired rows
// than the bound, one call drains all of them through repeated bounded passes.
// This is the termination condition the server runs on a ticker — if it stopped
// after the first pass, retention would silently become one batch per tick.
func TestSweepExpiredUploadPartsDrains(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const rows = 5
	userID, cutoff := seedExpiredParts(t, s, "+15559000032", rows)

	n, err := s.SweepExpiredUploadParts(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != rows {
		t.Fatalf("sweep drained %d rows, want %d", n, rows)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, userID, 1); err != nil || parts != 0 {
		t.Fatalf("after drain: parts=%d err=%v", parts, err)
	}
}

// TestSweepExpiredUploadPartsExactMultiple is the loop's boundary: when the last
// row count equals the batch exactly, the drain must take the empty pass that
// proves there is nothing left rather than stopping on a full one.
func TestSweepExpiredUploadPartsExactMultiple(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	const rows = 4
	userID, cutoff := seedExpiredParts(t, s, "+15559000033", rows)

	n, err := s.SweepExpiredUploadParts(ctx, cutoff, 2)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != rows {
		t.Fatalf("sweep drained %d rows, want %d", n, rows)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, userID, 1); err != nil || parts != 0 {
		t.Fatalf("after drain: parts=%d err=%v", parts, err)
	}
}

// TestSweepExpiredUploadPartsSparesUnexpired pins what a drain must not take: a
// part stored after the cutoff survives however many passes run around it.
func TestSweepExpiredUploadPartsSparesUnexpired(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	userID, _ := seedExpiredParts(t, s, "+15559000034", 3)

	// Cutoff between the seeded parts and one stored after it.
	cutoff := time.Now()
	if err := s.SaveUploadPart(ctx, userID, 2, 0, part('b', 100), maxFile); err != nil {
		t.Fatalf("save live part: %v", err)
	}

	if _, err := s.SweepExpiredUploadParts(ctx, cutoff, 2); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if parts, _, _, err := s.UploadPartsSummary(ctx, userID, 2); err != nil || parts != 1 {
		t.Fatalf("live part: parts=%d err=%v", parts, err)
	}
}

// TestSweepExpiredUploadPartsCanceledContext proves a canceled sweep reports the
// failure rather than looping or claiming a clean drain. Shutdown cancels the
// sweep context, and a drain that swallowed it would report a retention pass
// that never ran.
func TestSweepExpiredUploadPartsCanceledContext(t *testing.T) {
	t.Parallel()
	s := open(t)
	userID, cutoff := seedExpiredParts(t, s, "+15559000035", 3)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SweepExpiredUploadParts(ctx, cutoff, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled sweep: err = %v, want context.Canceled", err)
	}
	if parts, _, _, err := s.UploadPartsSummary(context.Background(), userID, 1); err != nil || parts != 3 {
		t.Fatalf("after canceled sweep: parts=%d err=%v", parts, err)
	}
}

// TestSweepExpiredUploadPartsRejectsUnboundedBatch pins the guard on the bound
// itself: a non-positive batch is a caller bug, and reading it as "no limit"
// would silently restore the unbounded DELETE this bound exists to prevent. It
// must also not become an infinite drain of zero-row passes.
func TestSweepExpiredUploadPartsRejectsUnboundedBatch(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()

	if _, err := s.SweepExpiredUploadParts(ctx, time.Now(), 0); err == nil {
		t.Fatal("batch 0: expected an error")
	}
	if _, err := s.SweepExpiredUploadParts(ctx, time.Now(), -1); err == nil {
		t.Fatal("batch -1: expected an error")
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
