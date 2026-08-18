package blobscan_test

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/blobscan"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestMain(m *testing.M) {
	if err := pgtest.Prewarm(); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest prewarm: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

// past is a cutoff behind everything a test plants, so the age gate is
// exercised by the argument rather than by the wall clock. A test that waited
// out a real cutoff would need a short one, and a short one closes under host
// load before the assertion runs.
func past() time.Time { return time.Now().Add(-time.Hour) }

const payload = "some bytes"

// fixture is a store and a blob tree that start out agreeing with each other,
// so every test says in one place how it makes them disagree.
type fixture struct {
	t     *testing.T
	store *store.Store
	blobs *blob.Local
	dir   string
	dsn   string
	user  store.User
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	dir := filepath.Join(t.TempDir(), "blobs")
	l, err := blob.NewLocal(dir)
	if err != nil {
		t.Fatalf("new local: %v", err)
	}
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(l))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	u, err := s.CreateUser(ctx, "+15559170001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	return &fixture{t: t, store: s, blobs: l, dir: dir, dsn: dsn, user: u}
}

// allocate reserves a files row without storing any bytes: the state a crashed
// or in-flight assembly leaves behind.
func (f *fixture) allocate(size int64) store.File {
	f.t.Helper()
	file, err := f.store.AllocateFile(context.Background(), f.user.ID, size, "image/png", "x.png", 1<<30)
	if err != nil {
		f.t.Fatalf("allocate file: %v", err)
	}
	return file
}

// stored allocates a file, writes its bytes and marks the row stored, which is
// what a completed upload looks like from both sides.
func (f *fixture) stored(body string) store.File {
	f.t.Helper()
	file := f.allocate(int64(len(body)))
	f.putBlob(file.ID, body)
	if err := f.store.MarkFileStored(context.Background(), file.ID); err != nil {
		f.t.Fatalf("mark stored: %v", err)
	}
	return file
}

// putBlob writes bytes at a file id's key through the writer under test, so the
// on-disk layout a scan reads back is the one the server actually produces.
func (f *fixture) putBlob(id int64, body string) {
	f.t.Helper()
	if _, err := f.blobs.Put(context.Background(), blob.Key(id), strings.NewReader(body)); err != nil {
		f.t.Fatalf("put blob %d: %v", id, err)
	}
}

// plant writes a path into the blob tree directly, bypassing the writer. It is
// how a test produces something the writer never would.
func (f *fixture) plant(rel, body string) {
	f.t.Helper()
	name := filepath.Join(f.dir, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		f.t.Fatalf("mkdir for %s: %v", rel, err)
	}
	if err := os.WriteFile(name, []byte(body), 0o600); err != nil {
		f.t.Fatalf("plant %s: %v", rel, err)
	}
}

// age backdates a path's modification time, which is what the temporary-file
// cutoff reads.
func (f *fixture) age(rel string, d time.Duration) {
	f.t.Helper()
	name := filepath.Join(f.dir, filepath.FromSlash(rel))
	when := time.Now().Add(-d)
	if err := os.Chtimes(name, when, when); err != nil {
		f.t.Fatalf("chtimes %s: %v", rel, err)
	}
}

// dropRow removes a files row behind the store's back. Nothing in this build
// deletes one, and that is the point: the orphan class exists for the state an
// eraser leaves when it commits the row deletion and then fails to unlink, and
// for whatever else has ever put bytes under this tree.
func (f *fixture) dropRow(id int64) {
	f.t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		f.t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close in a test
	if _, err := conn.Exec(ctx, "DELETE FROM files WHERE id = $1", id); err != nil {
		f.t.Fatalf("drop files row %d: %v", id, err)
	}
}

func (f *fixture) scan(tempOlderThan time.Time) blobscan.Report {
	f.t.Helper()
	rep, err := blobscan.Scan(context.Background(), f.blobs, f.store, tempOlderThan)
	if err != nil {
		f.t.Fatalf("scan: %v", err)
	}
	return rep
}

// has reports whether a class named key.
func has(c blobscan.Class, key string) bool {
	for _, p := range c.Paths {
		if p.Key == key {
			return true
		}
	}
	return false
}

// The ticket's concrete case: a blob whose row is gone, id below the snapshot.
// It is named an orphan candidate and it carries the bytes a reclaim would free.
func TestScanNamesOrphanBelowSnapshot(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	gone := f.stored(payload)
	f.dropRow(gone.ID)
	// A second file keeps the snapshot above the orphan and gives the pass
	// something it must not name.
	live := f.stored(payload)

	rep := f.scan(past())
	key := blob.Key(gone.ID)
	if !has(rep.Orphans, key) {
		t.Fatalf("blob %s not named an orphan candidate; report = %+v", key, rep)
	}
	if rep.Orphans.Count != 1 || rep.Orphans.Bytes != int64(len(payload)) {
		t.Fatalf("orphans = %d files / %d bytes, want 1 / %d", rep.Orphans.Count, rep.Orphans.Bytes, len(payload))
	}
	if has(rep.Temps, key) || has(rep.Unexplained, key) {
		t.Fatalf("an orphan candidate was reported in another class too: %+v", rep)
	}
	if rep.Accounted != 1 || rep.Through != live.ID {
		t.Fatalf("accounted = %d, through = %d; want 1 and %d", rep.Accounted, rep.Through, live.ID)
	}
}

// A blob whose row exists is never reported, whether the row says its bytes are
// stored or not. An unstored row is an assembly that is running right now: its
// bytes are on their way to that exact key, and naming it would name a live
// upload.
func TestScanSkipsBlobWhoseRowExists(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	done := f.stored(payload)
	// An assembly that has written its bytes but not yet flipped the row.
	assembling := f.allocate(int64(len(payload)))
	f.putBlob(assembling.ID, payload)

	rep := f.scan(past())
	if rep.Orphans.Count != 0 {
		t.Fatalf("named %d orphans over blobs whose rows exist: %+v", rep.Orphans.Count, rep.Orphans.Paths)
	}
	if rep.Accounted != 2 {
		t.Fatalf("accounted = %d, want 2 (%d stored, %d unstored)", rep.Accounted, done.ID, assembling.ID)
	}
}

// hookTree runs before just as the walk begins, which is after the id snapshot
// has been taken if it was taken in the required order and not otherwise.
type hookTree struct {
	inner  blobscan.Tree
	before func()
}

func (h hookTree) Walk(ctx context.Context, fn func(blob.Entry) error) error {
	h.before()
	return h.inner.Walk(ctx, fn)
}

// The ticket's other concrete case: a blob written to disk during the pass, id
// above the snapshot, is not reported in any class. It has no row, so the
// reference side alone would call it an orphan and a reclaimer would destroy a
// file that is being uploaded right now. What excludes it is that the snapshot
// was read before the listing began: an id above it cannot be judged by a table
// read that predates it, so it is out of scope by construction.
//
// The hook allocates a real files row as well as writing the blob, which is
// what makes the test able to fail: the allocation advances the id ceiling, so
// a scan that read the snapshot after the listing began would see the arriving
// id under Through and name it an orphan. A test that only wrote the blob
// pins the id bound but not the order, because nothing moves the ceiling during
// the walk.
func TestScanExcludesBlobWrittenAfterTheSnapshot(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	settled := f.stored(payload)
	tree := hookTree{inner: f.blobs, before: func() {
		f.allocate(int64(len(payload)))
		f.putBlob(settled.ID+1, payload)
	}}

	rep, err := blobscan.Scan(context.Background(), tree, f.store, past())
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	key := blob.Key(settled.ID + 1)
	if has(rep.Orphans, key) || has(rep.Temps, key) || has(rep.Unexplained, key) {
		t.Fatalf("blob %s written after the snapshot was reported: %+v", key, rep)
	}
	if rep.Orphans.Count != 0 || rep.Unexplained.Count != 0 || rep.Temps.Count != 0 {
		t.Fatalf("report is not empty of candidates: %+v", rep)
	}
	if rep.AboveSnapshot != 1 {
		t.Fatalf("AboveSnapshot = %d, want 1; the blob has to be counted somewhere", rep.AboveSnapshot)
	}
	if rep.Through != settled.ID {
		t.Fatalf("Through = %d, want the snapshot %d taken before the walk", rep.Through, settled.ID)
	}
}

// The ceiling is on the ids allocated, not on the rows that exist. A blob
// whose row was committed and then deleted, with nothing allocated after it,
// sits at the top of the id space: a bound read from the rows has shrunk below
// it and would park the blob in AboveSnapshot, where nothing names it until a
// later upload allocates past it. That is the exact state a committed row
// delete and a lost unlink leave behind, and the class this pass exists for.
func TestScanNamesOrphanAtTheTopOfTheIDSpace(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	// Two files so the deleted one is not the only row: the row-based bound
	// would then be the lower id, strictly below the orphan.
	f.stored(payload)
	top := f.stored(payload)
	f.dropRow(top.ID)

	rep := f.scan(past())
	key := blob.Key(top.ID)
	if !has(rep.Orphans, key) {
		t.Fatalf("blob %s at the top of the id space not named an orphan candidate; report = %+v", key, rep)
	}
	if rep.Orphans.Count != 1 || rep.Orphans.Bytes != int64(len(payload)) {
		t.Fatalf("orphans = %d / %d bytes, want 1 / %d; report = %+v", rep.Orphans.Count, rep.Orphans.Bytes, len(payload), rep)
	}
	if rep.AboveSnapshot != 0 {
		t.Fatalf("AboveSnapshot = %d, want 0; the ceiling must not fall below a committed id: %+v", rep.AboveSnapshot, rep)
	}
	if rep.Accounted != 1 || rep.Through != top.ID {
		t.Fatalf("accounted = %d, through = %d; want 1 and %d", rep.Accounted, rep.Through, top.ID)
	}
}

// A path under the parts prefix is the layout's other keyspace: it is never
// unexplained, never an orphan candidate, and it is counted as its own class.
// The reclaim is the upload-part sweep's, not this pass's, so the class
// carries the bytes an operator wants to see without claiming anything about
// them.
func TestScanCountsUploadPartsAsTheirOwnClass(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	f.stored(payload)
	partKey, err := blob.NewPartKey()
	if err != nil {
		t.Fatalf("part key: %v", err)
	}
	f.plant(partKey, "part bytes")

	rep := f.scan(past())
	if !has(rep.Parts, partKey) {
		t.Fatalf("%s not counted as an upload part; report = %+v", partKey, rep)
	}
	if rep.Parts.Count != 1 || rep.Parts.Bytes != int64(len("part bytes")) {
		t.Fatalf("parts = %d / %d bytes, want 1 / %d; report = %+v", rep.Parts.Count, rep.Parts.Bytes, len("part bytes"), rep)
	}
	if has(rep.Unexplained, partKey) || has(rep.Orphans, partKey) || has(rep.Temps, partKey) {
		t.Fatalf("an upload part was reported in another class: %+v", rep)
	}
	if rep.Unexplained.Count != 0 || rep.Orphans.Count != 0 {
		t.Fatalf("a tree holding live upload parts claims the layout cannot explain them: %+v", rep)
	}
}

// The parts prefix's own directory is the layout, not an unexplained path: a
// server doing ordinary upload traffic must not warn that it does not explain
// a path its own writer created.
func TestScanDoesNotNameThePartsDirectory(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	f.stored(payload)
	partKey, err := blob.NewPartKey()
	if err != nil {
		t.Fatalf("part key: %v", err)
	}
	f.plant(partKey, "part bytes")

	rep := f.scan(past())
	if has(rep.Unexplained, "parts") {
		t.Fatalf("the parts directory was reported as unexplained: %+v", rep.Unexplained.Paths)
	}
	if rep.Unexplained.Count != 0 {
		t.Fatalf("unexplained = %d, want 0; report = %+v", rep.Unexplained.Count, rep)
	}
}

// Anything under the tree that does not parse back to the layout the writer
// produces is unexplained, reported on its own, and never an orphan candidate.
// A pass that guessed at an unexpected entry is how something nobody meant to
// touch gets destroyed by whatever acts on the result.
func TestScanReportsUnexplainedPaths(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	f.stored(payload) // one real blob, so the tree is not only strange
	for _, rel := range []string{
		"92/notanid",     // right shard, not a key
		"92/04242",       // a padded id is not one Key writes
		"zz/1",           // not a shard at all
		"92/deep/1",      // an extra path element
		"stray",          // a file at the root
		"junk/thing.dat", // and something else's directory
	} {
		f.plant(rel, "x")
	}

	rep := f.scan(past())
	for _, want := range []string{
		"92/notanid", "92/04242", "zz", "zz/1", "92/deep", "92/deep/1", "stray", "junk", "junk/thing.dat",
	} {
		if !has(rep.Unexplained, want) {
			t.Errorf("%q not reported as unexplained; report = %+v", want, rep.Unexplained.Paths)
		}
		if has(rep.Orphans, want) || has(rep.Temps, want) {
			t.Errorf("%q was reported as a reclaim candidate", want)
		}
	}
	if rep.Orphans.Count != 0 {
		t.Errorf("named %d orphans in a tree whose only blob has a row: %+v", rep.Orphans.Count, rep.Orphans.Paths)
	}
}

// A temporary file is a write in progress, not garbage, so it is its own class
// and it is only ever considered past the age cutoff. The fresh one is the case
// that matters: reclaiming it would delete the bytes of an upload that is
// running.
func TestScanHoldsBackFreshTemporaryFiles(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	// Below the snapshot, so it is the age gate holding this back and not the
	// id bound: the two exclusions have to be told apart.
	live := f.stored(payload)
	inFlight := blob.Key(live.ID) + blob.TempSuffix
	f.plant(inFlight, "half a b")

	rep := f.scan(past())
	if has(rep.Temps, inFlight) || has(rep.Orphans, inFlight) || has(rep.Unexplained, inFlight) {
		t.Fatalf("a temporary file written seconds ago was reported: %+v", rep)
	}
	if rep.TempsInFlight != 1 || rep.AboveSnapshot != 0 {
		t.Fatalf("TempsInFlight = %d, AboveSnapshot = %d; want the age gate to be what held it back",
			rep.TempsInFlight, rep.AboveSnapshot)
	}
}

// Past the cutoff it is reported, in the temporary class and no other: it is
// reclaimed by unlinking a path the writer abandoned, not by the id-based pass.
func TestScanNamesAgedTemporaryFiles(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	gone := f.stored(payload)
	f.dropRow(gone.ID)
	f.stored(payload) // keeps the snapshot above the dropped id
	abandoned := blob.Key(gone.ID) + blob.TempSuffix
	f.plant(abandoned, "half a b")
	f.age(abandoned, 30*time.Hour)

	rep := f.scan(time.Now().Add(-24 * time.Hour))
	if !has(rep.Temps, abandoned) {
		t.Fatalf("%s not named past the cutoff; report = %+v", abandoned, rep)
	}
	if rep.Temps.Count != 1 || rep.Temps.Bytes != 8 {
		t.Fatalf("temps = %d files / %d bytes, want 1 / 8", rep.Temps.Count, rep.Temps.Bytes)
	}
	// The stored blob at the same id is an orphan; the two classes are counted
	// apart because they are reclaimed by different mechanisms.
	if !has(rep.Orphans, blob.Key(gone.ID)) || rep.Orphans.Count != 1 {
		t.Fatalf("orphan class did not carry the blob itself: %+v", rep.Orphans)
	}
	if rep.TempsInFlight != 0 {
		t.Fatalf("TempsInFlight = %d, want 0", rep.TempsInFlight)
	}
}

// What an operator reads before anything is allowed to act: how much there is
// per class, in files and in bytes, and what is being held back and by what.
func TestScanTotalsPartitionTheTree(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	a, b := f.stored("aaaa"), f.stored("bbbbbbb") // two orphans, 4 + 7 bytes
	f.dropRow(a.ID)
	f.dropRow(b.ID)
	keep := f.stored(payload) // accounted for, and the snapshot
	tmp := blob.Key(a.ID) + blob.TempSuffix
	f.plant(tmp, "half")
	f.age(tmp, 30*time.Hour)
	f.plant("92/notanid", "junk!")
	f.putBlob(keep.ID+2, payload) // above the snapshot
	partKey, err := blob.NewPartKey()
	if err != nil {
		t.Fatalf("part key: %v", err)
	}
	f.plant(partKey, "part bytes") // the layout's other keyspace

	rep := f.scan(time.Now().Add(-24 * time.Hour))
	if rep.Through != keep.ID {
		t.Fatalf("Through = %d, want %d", rep.Through, keep.ID)
	}
	if rep.Orphans.Count != 2 || rep.Orphans.Bytes != 11 {
		t.Errorf("orphans = %d / %d bytes, want 2 / 11", rep.Orphans.Count, rep.Orphans.Bytes)
	}
	if rep.Temps.Count != 1 || rep.Temps.Bytes != 4 {
		t.Errorf("temps = %d / %d bytes, want 1 / 4", rep.Temps.Count, rep.Temps.Bytes)
	}
	if rep.Parts.Count != 1 || rep.Parts.Bytes != int64(len("part bytes")) {
		t.Errorf("parts = %d / %d bytes, want 1 / %d", rep.Parts.Count, rep.Parts.Bytes, len("part bytes"))
	}
	if rep.Unexplained.Count != 1 || rep.Unexplained.Bytes != 5 {
		t.Errorf("unexplained = %d / %d bytes, want 1 / 5", rep.Unexplained.Count, rep.Unexplained.Bytes)
	}
	if rep.Accounted != 1 || rep.AboveSnapshot != 1 {
		t.Errorf("accounted = %d, above snapshot = %d; want 1 and 1", rep.Accounted, rep.AboveSnapshot)
	}
	// Every path the walk saw lands in exactly one bucket, so a total that does
	// not add up is visible rather than quiet. Shard directories and the parts
	// directory are the remainder: they are the layout itself and are reported
	// as nothing.
	shards := map[string]bool{"92": true}
	for _, id := range []int64{a.ID, b.ID, keep.ID, keep.ID + 2} {
		shards[path.Dir(blob.Key(id))] = true
	}
	got := rep.Orphans.Count + rep.Temps.Count + rep.Parts.Count + rep.Unexplained.Count +
		rep.Accounted + rep.AboveSnapshot + rep.TempsInFlight
	if rep.Walked != got+len(shards)+1 { // +1 for the parts directory
		t.Errorf("walked %d paths, classified %d plus %d layout directories", rep.Walked, got, len(shards))
	}
}

// The report is a log line's worth of data about every file it names, so it must
// not carry the one value that is a download credential. Nothing has to remember
// not to print it: it is not in the report at all.
func TestScanReportCarriesNoAccessHash(t *testing.T) {
	t.Parallel()
	f := newFixture(t)

	orphan := f.stored(payload)
	f.dropRow(orphan.ID)
	f.stored(payload) // keeps the snapshot above the orphan, so it is named

	rep := f.scan(past())
	if rep.Orphans.Count != 1 {
		t.Fatalf("orphans = %d, want the one this test is about", rep.Orphans.Count)
	}
	rendered := fmt.Sprintf("%+v", rep)
	for _, hash := range []string{
		strconv.FormatInt(orphan.AccessHash, 10),
		strconv.FormatUint(uint64(orphan.AccessHash), 16), //nolint:gosec // G115: printing the same 64 bits the wire carries
	} {
		if strings.Contains(rendered, hash) {
			t.Fatalf("report contains the file's access hash %s: %s", hash, rendered)
		}
	}
}

// The pass takes no lock a request path could be holding, so it neither waits
// on a send nor makes one wait on it. Held here is the exclusive row lock every
// media send and forward takes on the files row; a scan that took any
// conflicting lock would sit behind this transaction until the deadline.
func TestScanWaitsOnNoFileRowLock(t *testing.T) {
	t.Parallel()
	f := newFixture(t)
	ctx := context.Background()

	held := f.stored(payload)
	conn, err := pgx.Connect(ctx, f.dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close in a test
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after the test
	if _, err := tx.Exec(ctx, "SELECT id FROM files WHERE id = $1 FOR UPDATE", held.ID); err != nil {
		t.Fatalf("lock files row: %v", err)
	}

	deadline, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := blobscan.Scan(deadline, f.blobs, f.store, past()); err != nil {
		t.Fatalf("scan blocked behind a held files row lock: %v", err)
	}
}
