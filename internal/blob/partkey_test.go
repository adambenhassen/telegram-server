package blob_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/blob"
)

// TestPartKeyDisjointFromAssembledKeys is the prefix rule as a test, not a
// convention: no part key may fall inside the assembled-blob keyspace, where
// the authorization is the download gate rather than the key. Assembled keys
// are "xx/<id>" with id a positive BIGSERIAL, so they are exactly the two
// lowercase hex characters, a slash, and digits — none of which a part key's
// fixed shape can produce, but this is the property that must stay true.
func TestPartKeyDisjointFromAssembledKeys(t *testing.T) {
	ctx := context.Background()
	b, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("new local: %v", err)
	}
	for range 200 {
		k, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("part key: %v", err)
		}
		if len(k) != len(blob.PartsPrefix)+32 {
			t.Fatalf("part key %q has length %d, want %d", k, len(k), len(blob.PartsPrefix)+32)
		}
		if k[:len(blob.PartsPrefix)] != blob.PartsPrefix {
			t.Fatalf("part key %q does not start with %q", k, blob.PartsPrefix)
		}
		if isAssembledKey(k) {
			t.Fatalf("part key %q is inside the assembled keyspace", k)
		}
		// The key must round-trip through the store, which is what makes the
		// prefix a real confinement rather than a string convention.
		if _, err := b.Put(ctx, k, bytes.NewReader([]byte("x"))); err != nil {
			t.Fatalf("put part key: %v", err)
		}
		if err := b.Remove(ctx, k); err != nil {
			t.Fatalf("remove part key: %v", err)
		}
	}
}

// isAssembledKey reports whether key has the exact shape blob.Key produces:
// two lowercase hex characters, a slash, and a positive decimal id.
func isAssembledKey(k string) bool {
	if len(k) < 4 || k[2] != '/' {
		return false
	}
	for i := range 2 {
		if !isHexNibble(k[i]) {
			return false
		}
	}
	for i := 3; i < len(k); i++ {
		if k[i] < '0' || k[i] > '9' {
			return false
		}
	}
	return true
}

func isHexNibble(c byte) bool {
	return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')
}

// TestPartKeysAreUnique is the collision property the isolation rests on: two
// distinct saves must never draw the same key, or two accounts' parts would
// share an object. 128 random bits makes a collision at this scale
// unreachable, so a hit is a bug in the draw, not chance.
func TestPartKeysAreUnique(t *testing.T) {
	seen := make(map[string]struct{}, 10000)
	for range 10000 {
		k, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("part key: %v", err)
		}
		if _, dup := seen[k]; dup {
			t.Fatalf("duplicate part key %q", k)
		}
		seen[k] = struct{}{}
	}
}
