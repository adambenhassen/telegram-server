package blob_test

import (
	"context"
	"errors"
	"math"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/blob"
)

const payload = "hello world, this is a blob"

// newLocal returns a store rooted in a fresh temp dir, plus that dir.
func newLocal(t *testing.T) (*blob.Local, string) {
	t.Helper()
	dir := t.TempDir()
	l, err := blob.NewLocal(filepath.Join(dir, "blobs"))
	if err != nil {
		t.Fatalf("new local: %v", err)
	}
	return l, filepath.Join(dir, "blobs")
}

func TestPutReadAt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)
	key := blob.Key(4242)

	n, err := l.Put(ctx, key, strings.NewReader(payload))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if n != int64(len(payload)) {
		t.Fatalf("put returned %d bytes, want %d", n, len(payload))
	}

	for path, want := range map[string]os.FileMode{
		filepath.Join(dir, "92"):      0o700,
		filepath.Join(dir, "92/4242"): 0o600,
	} {
		fi, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if fi.Mode().Perm() != want {
			t.Fatalf("%s mode %o, want %o", path, fi.Mode().Perm(), want)
		}
	}

	t.Run("full range", func(t *testing.T) {
		t.Parallel()
		got, err := l.ReadAt(ctx, key, 0, int64(len(payload)))
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != payload {
			t.Fatalf("read %q, want %q", got, payload)
		}
	})

	t.Run("ranged", func(t *testing.T) {
		t.Parallel()
		got, err := l.ReadAt(ctx, key, 6, 5)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != "world" {
			t.Fatalf("read %q, want %q", got, "world")
		}
	})

	t.Run("short read at eof", func(t *testing.T) {
		t.Parallel()
		got, err := l.ReadAt(ctx, key, int64(len(payload)-3), 1024)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != payload[len(payload)-3:] {
			t.Fatalf("read %q, want %q", got, payload[len(payload)-3:])
		}
	})

	t.Run("offset at eof", func(t *testing.T) {
		t.Parallel()
		got, err := l.ReadAt(ctx, key, int64(len(payload)), 1024)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("read %q, want empty", got)
		}
	})

	t.Run("no temp file left behind", func(t *testing.T) {
		t.Parallel()
		leftover, err := filepath.Glob(filepath.Join(dir, "*", "*.tmp"))
		if err != nil {
			t.Fatalf("glob: %v", err)
		}
		if len(leftover) != 0 {
			t.Fatalf("temp files left behind: %v", leftover)
		}
	})
}

func TestReadAtHostileWindow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, _ := newLocal(t)
	key := blob.Key(4242)
	if _, err := l.Put(ctx, key, strings.NewReader(payload)); err != nil {
		t.Fatalf("put: %v", err)
	}

	for name, w := range map[string]struct{ offset, limit int64 }{
		"negative limit":  {0, -1},
		"negative offset": {-1, 5},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := l.ReadAt(ctx, key, w.offset, w.limit); err == nil {
				t.Fatal("accepted an out-of-range window")
			}
		})
	}

	t.Run("limit past end of blob", func(t *testing.T) {
		t.Parallel()
		got, err := l.ReadAt(ctx, key, 0, 1<<31)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if string(got) != payload {
			t.Fatalf("read %q, want %q", got, payload)
		}
		if cap(got) > len(payload) {
			t.Fatalf("allocated %d bytes for a %d-byte blob", cap(got), len(payload))
		}
	})
}

func TestReadAtMissingKey(t *testing.T) {
	t.Parallel()
	l, _ := newLocal(t)

	_, err := l.ReadAt(context.Background(), blob.Key(7), 0, 10)
	if !errors.Is(err, blob.ErrNotFound) {
		t.Fatalf("read missing key: %v, want ErrNotFound", err)
	}
}

func TestTraversalRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, _ := newLocal(t)

	if _, err := l.Put(ctx, "../escape", strings.NewReader(payload)); err == nil {
		t.Fatal("put escaped the blob root")
	}
	if _, err := l.ReadAt(ctx, "../../etc/passwd", 0, 10); err == nil {
		t.Fatal("read escaped the blob root")
	}
}

func TestKey(t *testing.T) {
	t.Parallel()

	if got := blob.Key(4242); got != "92/4242" {
		t.Fatalf("Key(4242) = %q, want %q", got, "92/4242")
	}
	// Ids differing only above the low byte share a shard directory.
	a, b := blob.Key(4242), blob.Key(4242+256)
	if filepath.Dir(a) != filepath.Dir(b) {
		t.Fatalf("shards differ: %q vs %q", a, b)
	}
	if strings.Count(a, "/") != 1 {
		t.Fatalf("Key(4242) = %q, want exactly one separator", a)
	}
}

func TestParseKeyRoundTrips(t *testing.T) {
	t.Parallel()

	for _, id := range []int64{1, 255, 256, 4242, 1 << 40, math.MaxInt64} {
		got, ok := blob.ParseKey(blob.Key(id))
		if !ok || got != id {
			t.Errorf("ParseKey(Key(%d)) = %d, %v; want %d, true", id, got, ok, id)
		}
	}
}

// Everything Key does not produce is not a key. The strictness is the point:
// whatever acts on this classification treats a path it cannot parse as
// unexplained, so a spelling that parses loosely is a path getting misfiled as
// a blob.
func TestParseKeyRefusesAnythingKeyCannotProduce(t *testing.T) {
	t.Parallel()

	for name, key := range map[string]string{
		"empty":            "",
		"no shard":         "4242",
		"shard only":       "92",
		"wrong shard":      "91/4242",
		"padded shard":     "092/4242",
		"upper case shard": "9A/154",
		"non hex shard":    "9g/4242",
		"padded id":        "92/04242",
		"signed id":        "92/+4242",
		"negative id":      "6f/-4242",
		"zero id":          "00/0",
		"id not a number":  "92/four",
		"id overflows":     "00/9223372036854775808",
		"temp file":        "92/4242.tmp",
		"extra element":    "92/deep/4242",
		"trailing slash":   "92/4242/",
		"leading slash":    "/92/4242",
	} {
		if id, ok := blob.ParseKey(key); ok {
			t.Errorf("%s: ParseKey(%q) = %d, true; want false", name, key, id)
		}
	}
}

// Every key NewPartKey draws parses back, which is what makes the parts class
// a round trip through the writer rather than a prefix match.
func TestParsePartKeyRoundTrips(t *testing.T) {
	t.Parallel()

	for range 32 {
		k, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("new part key: %v", err)
		}
		if !blob.ParsePartKey(k) {
			t.Fatalf("ParsePartKey(%q) = false for a key the writer drew", k)
		}
	}
}

// TestKeyspacesAreDisjoint pins half of what the part-orphan pass's temporary
// class rests on: an assembled blob can never land under the parts prefix, so
// the only bytes that walk reaches are the bounded writer's. Key's shard is two
// hex digits and a separator, which cannot spell "parts/", and a part key names
// no file id — but "cannot" here is a property of the layout, and the pass now
// deletes on the strength of it, so it is checked rather than read off the
// format string.
func TestKeyspacesAreDisjoint(t *testing.T) {
	t.Parallel()

	edges := []int64{1, 2, 15, 16, 255, 256, 257, 4242, 1 << 20, 1 << 40, math.MaxInt64}
	ids := make([]int64, 0, len(edges)+512)
	ids = append(ids, edges...)
	for i := range 512 {
		ids = append(ids, int64(i)+1)
	}
	for _, id := range ids {
		k := blob.Key(id)
		if strings.HasPrefix(k, blob.PartsPrefix) {
			t.Fatalf("assembled key %q for id %d falls under the parts prefix", k, id)
		}
		if blob.ParsePartKey(k) {
			t.Fatalf("assembled key %q for id %d parses as a part key", k, id)
		}
		// The writer's temporary for an assembled key is outside the prefix
		// too: it is the one write in this tree that can run long, and the
		// cutoff argument holds only while the pass cannot see it.
		if strings.HasPrefix(k+blob.TempSuffix, blob.PartsPrefix) {
			t.Fatalf("assembled temporary %q falls under the parts prefix", k+blob.TempSuffix)
		}
	}

	for range 64 {
		k, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("new part key: %v", err)
		}
		if _, ok := blob.ParseKey(k); ok {
			t.Fatalf("part key %q parses as an assembled key", k)
		}
	}
}

// Everything NewPartKey does not produce is not a part key. The strictness is
// the point: whatever acts on this classification treats a path it cannot
// parse as unexplained, so a spelling that parses loosely is a path getting
// misfiled as a live upload part.
func TestParsePartKeyRefusesAnythingTheWriterCannotProduce(t *testing.T) {
	t.Parallel()

	for name, key := range map[string]string{
		"empty":          "",
		"prefix only":    "parts/",
		"no suffix":      "parts",
		"too short":      "parts/deadbeef",
		"too long":       "parts/deadbeef0000000000000000000000000000",
		"upper case":     "parts/DEADBEEF000000000000000000000000",
		"non hex":        "parts/zzzzbeef000000000000000000000000",
		"extra element":  "parts/deadbeef000000000000000000000000/deep",
		"trailing slash": "parts/deadbeef000000000000000000000000/",
		"temp file":      "parts/deadbeef000000000000000000000000.tmp",
		"other prefix":   "partx/deadbeef000000000000000000000000",
		"leading slash":  "/parts/deadbeef000000000000000000000000",
	} {
		if blob.ParsePartKey(key) {
			t.Errorf("%s: ParsePartKey(%q) = true; want false", name, key)
		}
	}
}

func TestIsShard(t *testing.T) {
	t.Parallel()

	// Every shard Key can produce is one.
	for _, id := range []int64{0, 1, 15, 16, 200, 255, 4242} {
		dir := path.Dir(blob.Key(id))
		if !blob.IsShard(dir) {
			t.Errorf("IsShard(%q) = false for Key(%d)", dir, id)
		}
	}
	for _, name := range []string{"", "9", "9a2", "9A", "9g", "+f", " f", "92/", "lost+found"} {
		if blob.IsShard(name) {
			t.Errorf("IsShard(%q) = true", name)
		}
	}
}

// Walk enumerates what is actually there, whatever put it there: the blobs, the
// shard directories, an in-progress write, and anything that has no business in
// the tree at all. Deciding what those mean is the caller's job, so nothing is
// filtered out here.
func TestWalk(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)

	if _, err := l.Put(ctx, blob.Key(4242), strings.NewReader(payload)); err != nil {
		t.Fatalf("put: %v", err)
	}
	write(t, filepath.Join(dir, "92", "4242"+blob.TempSuffix), "half a bl")
	write(t, filepath.Join(dir, "92", "notanid"), "x")
	if err := os.MkdirAll(filepath.Join(dir, "junk", "nested"), 0o700); err != nil {
		t.Fatalf("mkdir junk: %v", err)
	}

	got := map[string]blob.Entry{}
	if err := l.Walk(ctx, func(e blob.Entry) error {
		if _, dup := got[e.Key]; dup {
			t.Errorf("walk yielded %q twice", e.Key)
		}
		got[e.Key] = e
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}

	want := map[string]struct {
		dir  bool
		size int64
	}{
		"92":                        {dir: true},
		"92/4242":                   {size: int64(len(payload))},
		"92/4242" + blob.TempSuffix: {size: 9},
		"92/notanid":                {size: 1},
		"junk":                      {dir: true},
		"junk/nested":               {dir: true},
	}
	if len(got) != len(want) {
		t.Fatalf("walk yielded %d entries, want %d: %v", len(got), len(want), got)
	}
	for key, w := range want {
		e, ok := got[key]
		if !ok {
			t.Errorf("walk did not yield %q", key)
			continue
		}
		if e.Dir != w.dir || e.Regular == w.dir {
			t.Errorf("%q: Dir=%v Regular=%v, want Dir=%v", key, e.Dir, e.Regular, w.dir)
		}
		if e.Size != w.size {
			t.Errorf("%q: Size = %d, want %d", key, e.Size, w.size)
		}
		if !w.dir && e.ModTime.IsZero() {
			t.Errorf("%q: zero ModTime, which is what dates an in-progress write", key)
		}
	}
}

// A walk of a store nothing has written yet is not an error and yields nothing:
// NewLocal creates the root, so "empty" and "absent" are the same state.
func TestWalkEmptyStore(t *testing.T) {
	t.Parallel()
	l, _ := newLocal(t)

	n := 0
	if err := l.Walk(context.Background(), func(blob.Entry) error {
		n++
		return nil
	}); err != nil {
		t.Fatalf("walk: %v", err)
	}
	if n != 0 {
		t.Fatalf("walk of an empty store yielded %d entries", n)
	}
}

// The callback's error stops the walk and reaches the caller unchanged, so a
// classification that cannot continue is not reported as a complete one.
func TestWalkPropagatesCallbackError(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, _ := newLocal(t)
	for _, id := range []int64{1, 2, 3} {
		if _, err := l.Put(ctx, blob.Key(id), strings.NewReader(payload)); err != nil {
			t.Fatalf("put: %v", err)
		}
	}

	stop := errors.New("stop")
	seen := 0
	err := l.Walk(ctx, func(blob.Entry) error {
		seen++
		return stop
	})
	if !errors.Is(err, stop) {
		t.Fatalf("walk error = %v, want %v", err, stop)
	}
	if seen != 1 {
		t.Fatalf("walk kept going after an error: %d entries", seen)
	}
}

// A cancelled context stops the walk rather than reading the whole tree. The
// pass is background work on a server doing something more important.
func TestWalkStopsOnCancelledContext(t *testing.T) {
	t.Parallel()
	l, _ := newLocal(t)
	if _, err := l.Put(context.Background(), blob.Key(1), strings.NewReader(payload)); err != nil {
		t.Fatalf("put: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := l.Walk(ctx, func(blob.Entry) error {
		t.Error("walk called back under a cancelled context")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("walk error = %v, want context.Canceled", err)
	}
}

// write plants a file at an absolute path, creating its parent.
func write(t *testing.T, name, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(name), 0o700); err != nil {
		t.Fatalf("mkdir %s: %v", name, err)
	}
	if err := os.WriteFile(name, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
}
