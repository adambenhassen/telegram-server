package blob_test

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/blob"
)

// TestListPrefix yields every object under the prefix with its size and age
// inputs, and nothing outside it.
func TestListPrefix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)
	// A file named exactly like the prefix's directory ("parts", no slash) is
	// not under "parts/": the walk must not list it or reach into it. A
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

	got, err := l.ListPrefix(ctx, prefix, "", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != len(wantIn) {
		t.Fatalf("listed %d objects, want %d: %v", len(got), len(wantIn), got)
	}
	for _, o := range got {
		if o.Size != wantIn[o.Key] {
			t.Fatalf("%s size = %d, want %d", o.Key, o.Size, wantIn[o.Key])
		}
		if o.Modified.IsZero() {
			t.Fatalf("%s has no modification time", o.Key)
		}
	}
	byKey := map[string]blob.Object{}
	for _, o := range got {
		byKey[o.Key] = o
	}
	if age := time.Since(byKey[old].Modified); age < 50*time.Minute {
		t.Fatalf("old object reports age %v, want about an hour", age)
	}
	if age := time.Since(byKey[prefix+"bbb"].Modified); age > time.Minute {
		t.Fatalf("new object reports age %v, want fresh", age)
	}
}

// TestListPrefixBounded returns at most limit objects per call and resumes
// where the previous call stopped, so repeated calls walk the whole prefix.
func TestListPrefixBounded(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, _ := newLocal(t)
	const n = 5
	for i := range n {
		key := blob.PartsPrefix + string(rune('a'+i))
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// The caller resumes past the page's last key; a page that returns fewer
	// than limit objects is the last one.
	after := ""
	var seen []string
	for {
		got, err := l.ListPrefix(ctx, blob.PartsPrefix, after, 2)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for _, o := range got {
			seen = append(seen, o.Key)
		}
		if len(got) < 2 {
			break
		}
		after = got[len(got)-1].Key
	}
	sort.Strings(seen)
	want := make([]string, n)
	for i := range n {
		want[i] = blob.PartsPrefix + string(rune('a'+i))
	}
	if strings.Join(seen, "") != strings.Join(want, "") {
		t.Fatalf("paged walk saw %v, want %v", seen, want)
	}
}

// TestListPrefixEmptyPrefix is a no-op: the assembled keyspace has no single
// prefix, so nothing enumerates it through this.
func TestListPrefixEmptyPrefix(t *testing.T) {
	t.Parallel()
	l, _ := newLocal(t)
	if _, err := l.Put(context.Background(), blob.Key(4242), strings.NewReader("x")); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, err := l.ListPrefix(context.Background(), "", "", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty prefix listed %d objects, want none", len(got))
	}
}

// TestListPrefixSeparatorFreeFailsClosed asserts that a separator-free prefix
// (a top-level shard such as "92") cannot return a key outside its containment
// scope. The prefix names a directory; if that directory does not exist the
// call returns nothing, and if it does exist only keys under it are returned.
// A file at the scope path is not a directory, so the call fails closed.
func TestListPrefixSeparatorFreeFailsClosed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)

	// Plant objects under two assembled shards and the parts prefix.
	for _, k := range []string{blob.Key(4242), blob.Key(999), blob.PartsPrefix + "aaaa"} {
		if _, err := l.Put(ctx, k, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", k, err)
		}
	}

	// A separator-free prefix naming a non-existent shard directory returns
	// nothing: it does not widen to the store root.
	got, err := l.ListPrefix(ctx, "ff", "", 100)
	if err != nil {
		t.Fatalf("list non-existent shard: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("non-existent shard listed %d objects, want 0: %v", len(got), got)
	}

	// A separator-free prefix naming an existing shard directory returns only
	// keys under that shard. The result must be non-empty: the object under
	// "92/" was planted above, so an empty result means the walk failed to
	// find it, and the containment assertion below would pass vacuously.
	got, err = l.ListPrefix(ctx, "92", "", 100)
	if err != nil {
		t.Fatalf("list shard 92: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("shard 92 listed no objects, want at least the planted one")
	}
	for _, o := range got {
		if !strings.HasPrefix(o.Key, "92/") {
			t.Fatalf("shard 92 returned key outside scope: %q", o.Key)
		}
	}

	// A separator-free prefix naming a file (not a directory) fails closed.
	if err := os.WriteFile(filepath.Join(dir, "ab"), []byte("a file"), 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}
	_, err = l.ListPrefix(ctx, "ab", "", 100)
	if err == nil {
		t.Fatal("prefix naming a file succeeded, want a fail-closed error")
	}
}

// TestListPrefixAfterResumes verifies the after parameter: keys at or below
// after are skipped, and the page is the limit smallest keys above after
// within the prefix.
func TestListPrefixAfterResumes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, _ := newLocal(t)
	for i := range 5 {
		key := blob.PartsPrefix + string(rune('a'+i))
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	// Resume after "parts/c": should get d and e.
	got, err := l.ListPrefix(ctx, blob.PartsPrefix, blob.PartsPrefix+"c", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d objects, want 2: %v", len(got), got)
	}
	if got[0].Key != blob.PartsPrefix+"d" || got[1].Key != blob.PartsPrefix+"e" {
		t.Fatalf("got %v, want [parts/d parts/e]", got)
	}
}

// TestListPrefixAllKeysReturnsAll verifies that the AllKeys sentinel returns
// every key under the prefix in a single call, for a caller that walks the
// whole prefix once and chunks its own work.
func TestListPrefixAllKeysReturnsAll(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, _ := newLocal(t)
	const n = 10
	for i := range n {
		key := blob.PartsPrefix + string(rune('a'+i))
		if _, err := l.Put(ctx, key, strings.NewReader("x")); err != nil {
			t.Fatalf("put %s: %v", key, err)
		}
	}

	got, err := l.ListPrefix(ctx, blob.PartsPrefix, "", blob.AllKeys)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != n {
		t.Fatalf("got %d objects, want %d", len(got), n)
	}
	// Globally ordered.
	for i := 1; i < len(got); i++ {
		if got[i-1].Key >= got[i].Key {
			t.Fatalf("not in order at %d: %q >= %q", i, got[i-1].Key, got[i].Key)
		}
	}

	// Zero is an error: a caller that computed a zero limit has a bug that
	// must not silently become an unbounded operation.
	_, err = l.ListPrefix(ctx, blob.PartsPrefix, "", 0)
	if err == nil {
		t.Fatal("zero limit succeeded, want an error")
	}
	// A negative limit other than AllKeys is an error.
	_, err = l.ListPrefix(ctx, blob.PartsPrefix, "", -2)
	if err == nil {
		t.Fatal("negative limit other than AllKeys succeeded, want an error")
	}
}
