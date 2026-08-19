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
	"io/fs"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

// ErrNotFound is returned by [Store.ReadAt] for a key that was never stored.
var ErrNotFound = errors.New("blob not found")

// TempSuffix is what [Local.Put] appends to a key while the bytes are being
// written. A path carrying it is a write in progress, not a stored blob, and it
// is named here rather than spelled inline at both ends: a pass classifying the
// tree has to recognise the writer's working file, and a second spelling of it
// is a second place to drift from.
const TempSuffix = ".tmp"

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
	// WalkPrefix calls fn for every entry under prefix, streaming, and stops
	// at the first error fn returns. Containment is the backend's, not the
	// caller's: every entry fn sees is under prefix, an empty prefix
	// enumerates nothing, and a prefix that names no containment scope fails
	// closed rather than widening. A prefix holding no objects yields nothing
	// and is not an error.
	//
	// It streams because what the caller must hold is then the caller's
	// choice: an age-based pass batches it into whatever its own gate can
	// carry, and a remote backend hands its listing over a page at a time
	// rather than buffering a whole bucket to answer one call.
	WalkPrefix(ctx context.Context, prefix string, fn func(Entry) error) error
}

// Key returns the storage key for a file id, sharded on the id's low byte so
// that no single directory accumulates every blob.
func Key(id int64) string { return fmt.Sprintf("%02x/%d", id&0xff, id) }

// ParseKey returns the file id key names, and reports whether key is one Key
// could have produced.
//
// It is deliberately strict, because it is how a pass over the tree separates
// blobs it can reason about from paths it cannot. A padded number, a shard that
// does not match its id, upper-case hex, an extra path element: none of those
// come from this package, so none of them is a key, and whatever acts on the
// classification must treat them as unexplained rather than as blobs. The check
// is a round trip through Key rather than a second reading of the layout, so
// the two cannot drift apart.
func ParseKey(key string) (int64, bool) {
	_, num, ok := strings.Cut(key, "/")
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseInt(num, 10, 64)
	if err != nil || id <= 0 || Key(id) != key {
		return 0, false
	}
	return id, true
}

// IsShard reports whether name is a shard directory Key's layout produces: an
// id's low byte, as exactly two lower-case hex digits. It is the directory half
// of ParseKey and exists for the same reason — a directory under the blob root
// that no key could live in is something else's, not the store's.
func IsShard(name string) bool {
	b, err := strconv.ParseUint(name, 16, 8)
	return err == nil && fmt.Sprintf("%02x", b) == name
}

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

// ParsePartKey reports whether key is one NewPartKey could have produced:
// PartsPrefix, then exactly 32 lower-case hex characters, and nothing after.
//
// It is the part-key half of ParseKey and exists for the same reason: a class
// is earned by round-tripping through what the writer produces, never by where
// a path sits. A prefix match would count parts/README and parts/sub/nested
// as in-flight upload bytes, and the report would contradict itself on the
// same two files while the directory holding them was warn-logged as
// unexplained. The round trip is through the same 16 bytes NewPartKey draws,
// so the two cannot drift apart.
func ParsePartKey(key string) bool {
	suffix, ok := strings.CutPrefix(key, PartsPrefix)
	if !ok || len(suffix) != 32 {
		return false
	}
	for i := range suffix {
		c := suffix[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
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
	tmp := key + TempSuffix
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

// Entry is one path found under a store's root by [Local.Walk] or under a key
// prefix by [Store.WalkPrefix].
type Entry struct {
	// Key is the path relative to the store root, slash-separated, so it
	// compares directly against what Key produces.
	Key string
	// Dir reports a directory.
	Dir bool
	// Regular reports an ordinary file. A symlink, socket, device or fifo is
	// neither Dir nor Regular: Put creates none of them, so a caller must not
	// have to infer that from the name it happens to carry.
	Regular bool
	// Size is the byte size of a regular file, and zero for anything else.
	Size int64
	// ModTime is when the entry was last written, which is what dates a write
	// still in progress.
	ModTime time.Time
}

// Walk calls fn for every entry under the store root, the root itself excluded,
// and stops at the first error fn returns. It is read-only and takes no lock:
// it observes whatever the tree held as it passed, so a blob written into a
// directory it has already left is simply not seen by this call.
//
// An entry that vanishes between being listed and being examined is skipped
// rather than failed on. Put renames its temporary file into place while a walk
// may be running, so a path disappearing mid-walk is ordinary traffic, not a
// fault. Every other error stops the walk and is returned — a walk that quietly
// skipped an unreadable subtree would report nothing for it, which reads
// exactly like finding nothing there.
func (l *Local) Walk(ctx context.Context, fn func(Entry) error) error {
	return l.walk(ctx, ".", "", fn)
}

// WalkPrefix calls fn for every entry under prefix, the directory the prefix is
// contained by excluded, and stops at the first error fn returns. It carries
// the same read-only, lock-free, vanishing-path semantics as [Local.Walk].
//
// Containment is the point of the method and lives here rather than in the
// caller: every entry fn sees is under prefix, and a prefix that names no
// containment scope fails closed rather than widening. An empty prefix
// enumerates nothing — the assembled keyspace has no single prefix, so nothing
// outside a named prefix is reachable through this — and a prefix that resolves
// to a file rather than a directory is an error, because widening to that
// file's parent would enumerate keys the caller never named. A prefix with no
// objects yet is not an error: it yields nothing.
//
// It streams rather than returning a page, so a caller holds only what it
// chooses to accumulate and the enumeration itself is one directory read per
// run whatever the prefix holds. That is also what a remote backend's listing
// natively is — a continuation token, a page at a time — so the shape does not
// require one to buffer a whole bucket listing to satisfy it.
func (l *Local) WalkPrefix(ctx context.Context, prefix string, fn func(Entry) error) error {
	if prefix == "" {
		return nil
	}
	// The containment scope is the directory the prefix names. A prefix with
	// no separator ("92") names a top-level shard directory; one with a
	// separator ("parts/aaa") names the directory it sits in. A prefix that
	// does not resolve to a directory fails closed: widening to the store root
	// would enumerate the assembled keyspace, which is exactly what this
	// method must not do.
	scope := strings.TrimSuffix(prefix, "/")
	if i := strings.LastIndex(prefix, "/"); i >= 0 && i+1 < len(prefix) {
		scope = strings.TrimSuffix(prefix[:i+1], "/")
	}
	fi, err := l.root.Lstat(scope)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil // the prefix has no objects yet
		}
		return fmt.Errorf("blob walk: %w", err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("blob walk: %q is not a directory", scope)
	}
	return l.walk(ctx, scope, prefix, fn)
}

// walk is the package's one enumeration: it yields every entry under root
// except root itself, keeping only those whose key carries prefix. Walk passes
// the store root and an empty prefix, WalkPrefix passes the prefix's
// containment scope and the prefix; there is no second traversal to drift from
// this one.
func (l *Local) walk(ctx context.Context, root, prefix string, fn func(Entry) error) error {
	return fs.WalkDir(l.root.FS(), root, func(p string, d fs.DirEntry, err error) error {
		switch {
		case errors.Is(err, fs.ErrNotExist):
			return nil
		case err != nil:
			return fmt.Errorf("blob walk %s: %w", p, err)
		case p == root:
			return nil
		case !strings.HasPrefix(p, prefix):
			return nil
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		info, err := d.Info()
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("blob walk stat %s: %w", p, err)
		}
		e := Entry{Key: p, Dir: d.IsDir(), Regular: info.Mode().IsRegular(), ModTime: info.ModTime()}
		if e.Regular {
			e.Size = info.Size()
		}
		return fn(e)
	})
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
