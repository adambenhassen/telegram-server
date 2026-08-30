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

func TestValidateKeyRejectsHostileKeysByClass(t *testing.T) {
	t.Parallel()

	for name, key := range map[string]string{
		"empty":              "",
		"too long in bytes":  strings.Repeat("a", 1025),
		"leading separator":  "/a",
		"trailing separator": "a/",
		"empty segment":      "a//b",
		"dot segment":        "a/./b",
		"dot-dot segment":    "a/../b",
		"space":              "a b",
		"percent":            "a%b",
		"question mark":      "a?b",
		"hash":               "a#b",
		"control byte":       "a\nb",
		"non-ASCII":          "a/é",
		"backslash":          `a\b`,
	} {
		if err := blob.ValidateKey(key); !errors.Is(err, blob.ErrInvalidKey) {
			t.Errorf("%s: ValidateKey(%q) = %v, want ErrInvalidKey", name, key, err)
		}
	}
}

func TestValidateKeyAcceptsPackageGeneratedKeys(t *testing.T) {
	t.Parallel()

	ids := []int64{0, 1, 15, 16, 255, 256, 4242, 1 << 40, -1}
	for _, id := range ids {
		key := blob.Key(id)
		for _, generated := range []string{key, key + blob.TempSuffix} {
			if err := blob.ValidateKey(generated); err != nil {
				t.Errorf("ValidateKey(%q), from Key(%d): %v", generated, id, err)
			}
		}
	}

	for range 32 {
		key, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("NewPartKey: %v", err)
		}
		if err := blob.ValidateKey(key); err != nil {
			t.Errorf("ValidateKey(%q), from NewPartKey: %v", key, err)
		}
	}
}

func TestLocalRejectsInvalidKeysBeforeFilesystemOperations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	l, dir := newLocal(t)

	if _, err := l.ReadAt(ctx, "../secrets", 0, 16); !errors.Is(err, blob.ErrInvalidKey) {
		t.Fatalf("ReadAt hostile key: %v, want ErrInvalidKey", err)
	}

	if _, err := l.Put(ctx, "a//b", strings.NewReader("payload")); !errors.Is(err, blob.ErrInvalidKey) {
		t.Fatalf("Put hostile key: %v, want ErrInvalidKey", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Put hostile key created a directory: stat = %v", err)
	}

	if err := os.Mkdir(filepath.Join(dir, "x"), 0o700); err != nil {
		t.Fatalf("create remove target: %v", err)
	}
	if err := l.Remove(ctx, "x/"); !errors.Is(err, blob.ErrInvalidKey) {
		t.Fatalf("Remove hostile key: %v, want ErrInvalidKey", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "x")); err != nil {
		t.Fatalf("Remove hostile key removed its target: %v", err)
	}
}
