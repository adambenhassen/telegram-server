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
	// ListPrefix returns keys at or after after within prefix, each with the
	// size and modification time an age-based pass needs. Containment is
	// always enforced: every key returned is under prefix, and a prefix that
	// does not resolve to a containment scope fails closed rather than
	// widening. after is the resume position, a separate input from prefix:
	// an empty after starts at the beginning of the prefix. The result is
	// globally ordered by key. limit bounds the page: a positive limit
	// returns at most that many keys, and a caller that walks the prefix
	// passes the last key of each page as the next after. limit of zero
	// returns every key under the prefix in one call, for a caller that
	// walks the whole prefix once and chunks its own work. A negative limit
	// is an error. An empty prefix lists nothing: the assembled keyspace has
	// no single prefix, so nothing outside a named prefix is reachable
	// through this.
	ListPrefix(ctx context.Context, prefix, after string, limit int) ([]Object, error)
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

// ListPrefix returns the limit smallest keys at or after after within
// prefix, globally ordered by key. Containment is always enforced: every key
// returned starts with prefix, and a prefix that does not name a directory
// scope (a file, a missing path, or the empty string) fails closed rather
// than widening to the store root. after is the resume position, separate
// from prefix: an empty after starts at the beginning of the prefix, and a
// non-empty after skips every key that sorts at or below it. A page never
// exceeds limit.
//
// The walk collects every key under the prefix's directory subtree, filters
// by containment and resume position, sorts globally, and returns the first
// limit. The walk cannot stop early: for a nested prefix, a directory that
// sorts late may hold keys that sort before keys in an earlier directory, so
// the limit smallest are not known until the whole subtree is seen. The parts
// prefix is flat (one directory), so in practice this is one directory read.
func (l *Local) ListPrefix(_ context.Context, prefix, after string, limit int) ([]Object, error) {
	if limit < 0 {
		return nil, fmt.Errorf("blob list: limit %d must be non-negative", limit)
	}
	if prefix == "" {
		return nil, nil
	}
	// The containment scope is the directory the prefix names. A prefix with
	// no separator ("92") names a top-level shard directory; one with a
	// separator ("parts/aaa") names the directory it sits in. A prefix that
	// does not resolve to a directory fails closed: widening to the store
	// root would enumerate the assembled keyspace, which is exactly what this
	// method must not do.
	scope := strings.TrimSuffix(prefix, "/")
	if i := strings.LastIndex(prefix, "/"); i >= 0 && i+1 < len(prefix) {
		scope = strings.TrimSuffix(prefix[:i+1], "/")
	}
	fi, err := l.root.Lstat(scope)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil // the prefix has no objects yet
		}
		return nil, fmt.Errorf("blob list: %w", err)
	}
	if !fi.IsDir() {
		// A file, not a directory: the prefix names no containment scope.
		// Fail closed rather than widen to the parent.
		return nil, fmt.Errorf("blob list: %q is not a directory", scope)
	}
	// Collect every object under the scope.
	var all []Object
	var walk func(p string) error
	walk = func(p string) error {
		f, err := l.root.Open(p)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				return nil
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
			// Containment: only keys under the caller's prefix.
			if !strings.HasPrefix(child, prefix) {
				continue
			}
			// Resume: skip keys at or below the after position.
			if after != "" && child <= after {
				continue
			}
			all = append(all, Object{Key: child, Size: e.Size(), Modified: e.ModTime()})
		}
		return nil
	}
	if err := walk(scope); err != nil {
		return nil, fmt.Errorf("blob list: %w", err)
	}
	sort.Slice(all, func(i, j int) bool { return all[i].Key < all[j].Key })
	if limit > 0 && len(all) > limit {
		all = all[:limit]
	}
	return all, nil
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
