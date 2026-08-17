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

	got, err := l.ListPrefix(ctx, prefix, 10)
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
	next := blob.PartsPrefix
	var seen []string
	for {
		got, err := l.ListPrefix(ctx, next, 2)
		if err != nil {
			t.Fatalf("list: %v", err)
		}
		for i, o := range got {
			seen = append(seen, o.Key)
			if i == len(got)-1 && len(got) == 2 {
				// Any byte above a legal key byte sorts after every real key,
				// so this prefix continues right after the page.
				next = o.Key + "\x00"
			}
		}
		if len(got) < 2 {
			break
		}
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
	got, err := l.ListPrefix(context.Background(), "", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("empty prefix listed %d objects, want none", len(got))
	}
}
