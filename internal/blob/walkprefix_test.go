package blob_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
)

// collectPrefix walks a prefix and returns what it yielded, keyed by key.
func collectPrefix(t *testing.T, l *blob.Local, prefix string) map[string]blob.Entry {
	t.Helper()
	got := map[string]blob.Entry{}
	if err := l.WalkPrefix(context.Background(), prefix, func(e blob.Entry) error {
		got[e.Key] = e
		return nil
	}); err != nil {
		t.Fatalf("walk %q: %v", prefix, err)
	}
	return got
}

// TestWalkPrefix yields every entry under the prefix with its size and age
// inputs, and nothing outside it.
func TestWalkPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)
	// A file named exactly like the prefix's directory ("parts", no slash) is
	// not under "parts/": the walk must not yield it or reach into it. A
	// directory and a file cannot share a name, so the in-prefix objects live
	// under "parts2/" for this test and the assembled key sits outside both.
	if err := os.WriteFile(filepath.Join(dir, "parts"), []byte("a file named like the prefix itself"), 0o600); err != nil {
		t.Fatalf("write sibling: %v", err)
	}

	prefix := "parts2/"
	wantIn := map[string]int64{
		prefix + "aaa": 5,
		prefix + "bbb": 2,
	}
	for k, v := range map[string]string{
		prefix + "aaa": "12345",
		prefix + "bbb": "67",
		blob.Key(4242): "assembled",
	} {
		if _, err := l.Put(ctx, k, strings.NewReader(v)); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// Age one of the in-prefix objects so the pass can tell them apart.
	old := prefix + "aaa"
	past := time.Now().Add(-time.Hour)
	if err := os.Chtimes(filepath.Join(l.RootDir(), filepath.FromSlash(old)), past, past); err != nil {
		t.Fatalf("chtimes: %v", err)
	}

	got := collectPrefix(t, l, prefix)
	if len(got) != len(wantIn) {
		t.Fatalf("walked %d entries, want %d: %v", len(got), len(wantIn), got)
	}
	for _, e := range got {
		if e.Size != wantIn[e.Key] {
			t.Fatalf("%s size = %d, want %d", e.Key, e.Size, wantIn[e.Key])
		}
		if !e.Regular || e.Dir {
			t.Fatalf("%s reported dir=%v regular=%v, want a regular file", e.Key, e.Dir, e.Regular)
		}
		if e.ModTime.IsZero() {
			t.Fatalf("%s has no modification time", e.Key)
		}
	}
	if age := time.Since(got[old].ModTime); age < 50*time.Minute {
		t.Fatalf("old object reports age %v, want about an hour", age)
	}
	if age := time.Since(got[prefix+"bbb"].ModTime); age > time.Minute {
		t.Fatalf("new object reports age %v, want fresh", age)
	}
}

// TestWalkPrefixYieldsEveryEntryAndHonoursCallerStop asserts what the callback
// shape buys the caller: every entry under the prefix arrives, and a caller
// that stops mid-walk stops the walk and gets its own error back.
//
// It does not prove the traversal avoids buffering — twenty entries against a
// 512-entry chunk cannot reach that, and no test here can observe the read
// size. What holds that property is the chunked read in walk, exercised across
// its boundary by TestWalkPrefixCrossesChunkBoundary and measured as a flat
// heap in the PR.
func TestWalkPrefixYieldsEveryEntryAndHonoursCallerStop(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, _ := newLocal(t)
	const n = 20
	for i := range n {
		key := blob.PartsPrefix + string(rune('a'+i))
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// The whole prefix, in one streamed pass.
	var seen []string
	if err := l.WalkPrefix(ctx, blob.PartsPrefix, func(e blob.Entry) error {
		seen = append(seen, e.Key)
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	sort.Strings(seen)
	want := make([]string, n)
	for i := range n {
		want[i] = blob.PartsPrefix + string(rune('a'+i))
	}
	if strings.Join(seen, ",") != strings.Join(want, ",") {
		t.Fatalf("walk saw %v, want %v", seen, want)
	}

	// Stopping after the third entry stops the walk there: the caller decides
	// what it holds, and the error it returns is the error it gets back.
	stop := errors.New("enough")
	count := 0
	err := l.WalkPrefix(ctx, blob.PartsPrefix, func(blob.Entry) error {
		count++
		if count == 3 {
			return stop
		}
		return nil
	})
	if !errors.Is(err, stop) {
		t.Fatalf("walk error = %v, want the caller's own error", err)
	}
	if count != 3 {
		t.Fatalf("callback ran %d times after stopping at 3", count)
	}
}

// TestWalkPrefixCrossesChunkBoundary walks directories sized around the read
// chunk. The traversal reads a fixed number of entries at a time and loops, so
// the counts either side of that boundary are where a walk stops early or
// repeats: an off-by-one in the loop's exit condition is invisible at any other
// size, and the pass built on it would then reclaim part of the prefix and
// report a clean run.
func TestWalkPrefixCrossesChunkBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	for _, n := range []int{511, 512, 513, 1024, 1025} {
		l, _ := newLocal(t)
		for i := range n {
			key := fmt.Sprintf("%s%032x", blob.PartsPrefix, i)
			if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
				t.Fatalf("put %s: %v", key, err)
			}
		}
		seen := map[string]bool{}
		if err := l.WalkPrefix(ctx, blob.PartsPrefix, func(e blob.Entry) error {
			if seen[e.Key] {
				t.Fatalf("n=%d: %q yielded twice", n, e.Key)
			}
			seen[e.Key] = true
			return nil
		}); err != nil {
			t.Fatalf("n=%d: walk: %v", n, err)
		}
		if len(seen) != n {
			t.Fatalf("n=%d: walked %d entries", n, len(seen))
		}
	}
}

// TestWalkPrefixEmptyPrefix is a no-op: the assembled keyspace has no single
// prefix, so nothing enumerates it through this.
func TestWalkPrefixEmptyPrefix(t *testing.T) {
	t.Parallel()
	l, _ := newLocal(t)
	if _, err := l.Put(context.Background(), blob.Key(4242), strings.NewReader("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	if got := collectPrefix(t, l, ""); len(got) != 0 {
		t.Fatalf("empty prefix walked %d entries, want none", len(got))
	}
}

// TestWalkPrefixSeparatorFreeFailsClosed asserts that a separator-free prefix
// (a top-level shard such as "92") cannot yield a key outside its containment
// scope. The prefix names a directory; if that directory does not exist the
// walk yields nothing, and if it does exist only keys under it are yielded. A
// file at the scope path is not a directory, so the call fails closed.
func TestWalkPrefixSeparatorFreeFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)

	// Plant objects under two assembled shards and the parts prefix.
	for _, k := range []string{blob.Key(4242), blob.Key(999), blob.PartsPrefix + "aaaa"} {
		if _, err := l.Put(ctx, k, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// A separator-free prefix naming a non-existent shard directory yields
	// nothing: it does not widen to the store root.
	if got := collectPrefix(t, l, "ff"); len(got) != 0 {
		t.Fatalf("non-existent shard walked %d entries, want 0: %v", len(got), got)
	}

	// A separator-free prefix naming an existing shard directory yields only
	// keys under that shard. The result must be non-empty: the object under
	// "92/" was planted above, so an empty result means the walk failed to
	// find it, and the containment assertion below would pass vacuously.
	got := collectPrefix(t, l, "92")
	if len(got) == 0 {
		t.Fatal("shard 92 walked no entries, want at least the planted one")
	}
	for _, e := range got {
		if !strings.HasPrefix(e.Key, "92/") {
			t.Fatalf("shard 92 yielded key outside scope: %q", e.Key)
		}
	}

	// A separator-free prefix naming a file (not a directory) fails closed.
	if err := os.WriteFile(filepath.Join(dir, "ab"), []byte("a file"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	err := l.WalkPrefix(ctx, "ab", func(blob.Entry) error { return nil })
	if err == nil {
		t.Fatal("prefix naming a file succeeded, want a fail-closed error")
	}
}

// TestWalkPrefixReportsNestedShape asserts the caller can tell what a path is
// without inferring it from the name: a directory under the prefix is a
// directory, a file under it is a regular file, and the writer's temporary
// file is visible as one so a pass acting on the prefix can refuse it.
func TestWalkPrefixReportsNestedShape(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)
	if _, err := l.Put(ctx, blob.PartsPrefix+"aaaa", strings.NewReader("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	// A writer's in-flight temporary file, and a directory nobody's writer
	// created, both parked under the prefix.
	tmp := blob.PartsPrefix + "bbbb" + blob.TempSuffix
	if err := os.WriteFile(filepath.Join(dir, filepath.FromSlash(tmp)), []byte("half"), 0o600); err != nil {
		t.Fatalf("write temp: %v", err)
	}
	if err := os.Mkdir(filepath.Join(dir, "parts", "nested"), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}

	got := collectPrefix(t, l, blob.PartsPrefix)
	if e, ok := got[tmp]; !ok || !e.Regular || e.Size != 4 {
		t.Fatalf("temporary file reported as %+v (present=%v), want a 4-byte regular file", e, ok)
	}
	if e, ok := got["parts/nested"]; !ok || !e.Dir || e.Regular {
		t.Fatalf("nested directory reported as %+v (present=%v), want a directory", e, ok)
	}
}

// TestWalkPrefixStopsOnCancelledContext asserts the walk honours cancellation
// the same way the whole-tree walk does.
func TestWalkPrefixStopsOnCancelledContext(t *testing.T) {
	t.Parallel()
	l, _ := newLocal(t)
	if _, err := l.Put(context.Background(), blob.PartsPrefix+"aaaa", strings.NewReader("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := l.WalkPrefix(ctx, blob.PartsPrefix, func(blob.Entry) error { return nil })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("walk error = %v, want context.Canceled", err)
	}
}
