package blob_test

import (
	"context"
	"errors"
	"os"
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
