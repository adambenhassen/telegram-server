// Package blob stores opaque byte ranges at server-chosen keys. It knows
// nothing about files, users or media: callers pick the key and own whatever
// the bytes mean, including any encryption applied above this layer.
package blob

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"time"
)

// ErrNotFound is returned by [Store.ReadAt] for a key that was never stored.
var ErrNotFound = errors.New("blob not found")

// Store is the seam an object-storage backend lands on later. It has a single
// implementation, [Local], on purpose.
type Store interface {
	// Put stores r under key and returns the number of bytes written.
	Put(ctx context.Context, key string, r io.Reader) (int64, error)
	// ReadAt returns at most limit bytes of key starting at offset. A window
	// running past the end of the blob yields a short slice and a nil error.
	// A negative offset or limit is an error: the window comes from the
	// client and is not trusted.
	ReadAt(ctx context.Context, key string, offset, limit int64) ([]byte, error)
	// Remove deletes key. Deleting a key that is not there is a no-op, so a
	// caller that races a sweep or a re-save never needs to know which won.
	Remove(ctx context.Context, key string) error
	// ListPrefix returns at most limit objects whose keys start with prefix,
	// each with the size and modification time an age-based pass needs. The
	// result is a page, not the whole prefix: keys are ordered, a call
	// returning fewer than limit objects is the last page, and a caller that
	// walks the prefix repeats the call from where the previous page ended.
	// An empty prefix lists nothing: the assembled keyspace has no single
	// prefix, so nothing outside a named prefix is reachable through this.
	ListPrefix(ctx context.Context, prefix string, limit int) ([]Object, error)
}

// Object is one entry of a [Store.ListPrefix] page: the key and the two
// facts an age-based pass decides from, how big the object is and when it
// last changed.
type Object struct {
	Key      string
	Size     int64
	Modified time.Time
}

// Key returns the storage key for a file id, sharded on the id's low byte so
// that no single directory accumulates every blob.
func Key(id int64) string { return fmt.Sprintf("%02x/%d", id&0xff, id) }

// PartsPrefix is the key prefix every in-flight upload part lives under. It
// is statically disjoint from the assembled-blob keyspace: assembled keys are
// "xx/<id>" with id a positive BIGSERIAL, so they never start with this
// prefix, and a part key can never be one. No client input contributes to the
// key: the random suffix is drawn here, and the row that records it is
// user-scoped, so a key under this prefix is reachable only by the account
// that owns its row.
const PartsPrefix = "parts/"

// NewPartKey draws a random key for one in-flight upload part under
// PartsPrefix. It is not derivable from anything the client names, which is
// what makes two accounts that chose the same file_id hold disjoint bytes.
func NewPartKey() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("part key: %w", err)
	}
	return PartsPrefix + hex.EncodeToString(buf[:]), nil
}

// Local stores blobs as files under a directory.
type Local struct {
	root *os.Root
	dir  string
}

// NewLocal creates dir if needed and opens it as the root of the store.
func NewLocal(dir string) (*Local, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("blob dir: %w", err)
	}
	// Every path operation goes through this *os.Root handle, which confines
	// it to dir at the OS level: a key containing ".." or an absolute path
	// fails inside the standard library rather than reaching the filesystem.
	// That is why this package validates no keys of its own.
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open blob root: %w", err)
	}
	return &Local{root: root, dir: dir}, nil
}

// RootDir reports the directory the store is rooted in.
func (l *Local) RootDir() string { return l.dir }

// ListPrefix returns up to limit objects under prefix, in key order. The
// walk is confined to the prefix's own subtree: a sibling of the prefix
// directory, and a file that merely carries the prefix as a name prefix
// ("parts" vs "parts/"), are not under it. Keys come back slash-separated,
// the same form Put and Remove take, so a page feeds straight back into the
// store. limit must be positive; an empty prefix lists nothing for the reason
// in the interface.
//
// A page is a snapshot: an object written or deleted between pages may appear
// or vanish, and a caller that deletes what it lists must not care which of
// the two happened.
func (l *Local) ListPrefix(_ context.Context, prefix string, limit int) ([]Object, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("blob list: limit %d must be positive", limit)
	}
	if prefix == "" {
		return nil, nil
	}
	// Walk the shallowest directory that contains only the prefix, rather than
	// the store root: an unrelated sibling is not a walk it will error out on,
	// and the assembled shards are not. A cursor that carries a byte past the
	// last key (the "last key + 1" form a paged caller resumes with) resolves
	// to a file or a missing path inside that directory; the walk still
	// starts at the directory, and the filter below handles the cursor.
	root := strings.TrimSuffix(prefix, "/")
	if i := strings.LastIndex(prefix, "/"); i+1 < len(prefix) {
		root = strings.TrimSuffix(prefix[:i+1], "/")
	}
	var all []Object
	var walk func(p string) error
	walk = func(p string) error {
		f, err := l.root.Open(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil // the prefix has no objects yet
			}
			return err
		}
		defer func() { _ = f.Close() }() //nolint:errcheck // read-only close
		entries, err := f.Readdir(-1)
		if err != nil {
			return err
		}
		for _, e := range entries {
			child := e.Name()
			if p != "." {
				child = p + "/" + child
			}
			if e.IsDir() {
				if err := walk(child); err != nil {
					return err
				}
				continue
			}
			all = append(all, Object{Key: child, Size: e.Size(), Modified: e.ModTime()})
		}
		return nil
	}
	if err := walk(root); err != nil {
		return nil, fmt.Errorf("blob list: %w", err)
	}
	// A prefix that ends in "/" or names a real directory is a containment
	// filter: only keys under it. A prefix that carries a byte past the last
	// key is a cursor: keys at or after it, still under the same directory.
	// The two are told apart by whether the prefix itself is a directory.
	cursor := false
	if !strings.HasSuffix(prefix, "/") {
		if fi, err := l.root.Lstat(prefix); err != nil || !fi.IsDir() {
			cursor = true
		}
	}
	var out []Object
	for _, o := range all {
		if cursor {
			if o.Key < prefix {
				continue
			}
		} else if !strings.HasPrefix(o.Key, prefix) {
			continue
		}
		out = append(out, o)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	if len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

// Put writes r to a temporary file and renames it into place, so a reader
// never observes a partially written blob. O_EXCL on the temporary file also
// means two concurrent Puts to the same key cannot interleave: the second
// fails instead of corrupting the first. Keys come from freshly allocated file
// ids today, so that collision does not arise; the flag keeps it impossible if
// that ever changes.
func (l *Local) Put(_ context.Context, key string, r io.Reader) (int64, error) {
	if err := l.root.MkdirAll(path.Dir(key), 0o700); err != nil {
		return 0, fmt.Errorf("blob mkdir: %w", err)
	}
	tmp := key + ".tmp"
	f, err := l.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return 0, fmt.Errorf("blob create: %w", err)
	}
	n, err := io.Copy(f, r)
	if err != nil {
		// These cleanups run on an already-failing path, where a second
		// error has nowhere useful to go.
		_ = f.Close()          //nolint:errcheck // discarding a partial write
		_ = l.root.Remove(tmp) //nolint:errcheck // best-effort cleanup of a partial write
		return 0, fmt.Errorf("blob write: %w", err)
	}
	if err = f.Close(); err != nil {
		_ = l.root.Remove(tmp) //nolint:errcheck // best-effort cleanup of a partial write
		return 0, fmt.Errorf("blob close: %w", err)
	}
	if err = l.root.Rename(tmp, key); err != nil {
		_ = l.root.Remove(tmp) //nolint:errcheck // best-effort cleanup of a partial write
		return 0, fmt.Errorf("blob rename: %w", err)
	}
	return n, nil
}

// Remove deletes key. Deleting a key that is not there is a no-op, so a
// caller that races a sweep or a re-save never needs to know which won.
func (l *Local) Remove(_ context.Context, key string) error {
	err := l.root.Remove(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("blob remove: %w", err)
	}
	return nil
}

// ReadAt returns at most limit bytes of key starting at offset, or ErrNotFound
// if the key was never stored. A short read at end of file is not an error: a
// download asks for a fixed-size window and the last window of a blob is
// short, so io.EOF alongside a partial read returns those bytes with a nil
// error. Any other error is wrapped and returned.
//
// The window is client-supplied, so it is validated here rather than trusted:
// a negative offset or limit is rejected, and the buffer is sized against the
// blob rather than against limit, so a huge limit over a small blob cannot
// turn into a huge allocation.
func (l *Local) ReadAt(_ context.Context, key string, offset, limit int64) ([]byte, error) {
	if offset < 0 || limit < 0 {
		return nil, fmt.Errorf("blob read: negative window offset=%d limit=%d", offset, limit)
	}

	f, err := l.root.Open(key)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("blob open: %w", err)
	}
	defer func() { _ = f.Close() }() //nolint:errcheck // read-only close

	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("blob stat: %w", err)
	}
	if rest := max(fi.Size()-offset, 0); rest < limit {
		limit = rest
	}

	buf := make([]byte, limit)
	n, err := f.ReadAt(buf, offset)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("blob read: %w", err)
	}
	return buf[:n], nil
}
