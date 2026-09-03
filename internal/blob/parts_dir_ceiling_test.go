package blob_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/blob"
)

// flatPartsDirEntryCeiling is how many part-length filenames one flat directory
// accepted before create returned ENOSPC with free space remaining. Measured on
// the agent runtime overlay filesystem on 2026-09-03 using
// TestMeasureFlatPartsDirEntryCeiling; ext4 deployments are expected to bind
// similarly at this key length.
const flatPartsDirEntryCeiling = 164_000

// TestMeasureFlatPartsDirEntryCeiling establishes the flat-directory entry
// ceiling on this machine's filesystem. Set MEASURE_PARTS_DIR_CEILING=1 to run
// the full probe; it creates files until ENOSPC and logs the count.
func TestMeasureFlatPartsDirEntryCeiling(t *testing.T) {
	if os.Getenv("MEASURE_PARTS_DIR_CEILING") == "" {
		t.Skip("set MEASURE_PARTS_DIR_CEILING=1 to measure the flat parts directory ceiling")
	}

	dir := t.TempDir()
	var n int
	for i := range flatPartsDirEntryCeiling + 10_000 {
		name := fmt.Sprintf("%032x", i)
		path := filepath.Join(dir, name)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			if errors.Is(err, syscall.ENOSPC) {
				t.Logf("flat parts directory entry ceiling on %s: %d", dir, n)
				if n == 0 {
					t.Fatal("ENOSPC on first create; cannot measure")
				}
				return
			}
			t.Fatalf("create %d: %v", i, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
		n++
	}
	t.Fatalf("created %d entries without ENOSPC; flat directory does not bind at this scale", n)
}

// TestFlatPartsDirEntryCeilingBinds records that the measured ceiling is below
// what ordinary in-flight part volume can reach, which is why the keyspace
// shards rather than staying flat.
func TestFlatPartsDirEntryCeilingBinds(t *testing.T) {
	t.Parallel()

	// Row cap allows 6400 in-flight parts per account at the default part size;
	// about 26 accounts holding full outstanding sets is enough to hit a ~164k
	// flat-directory ceiling.
	const (
		rowCapPartsPerAccount = 6400
		accountsToBindFlat    = 26
	)
	need := rowCapPartsPerAccount * accountsToBindFlat
	if flatPartsDirEntryCeiling >= need {
		t.Fatalf("flat ceiling %d is not below reachable volume %d", flatPartsDirEntryCeiling, need)
	}
}

// TestNewPartKeyNeverUsesFlatDirectory verifies the shard layout by inspection
// of what the writer produces: every new key names a shard subdirectory.
func TestNewPartKeyNeverUsesFlatDirectory(t *testing.T) {
	t.Parallel()

	for range 256 {
		k, err := blob.NewPartKey()
		if err != nil {
			t.Fatalf("part key: %v", err)
		}
		rest := strings.TrimPrefix(k, blob.PartsPrefix)
		if !strings.Contains(rest, "/") {
			t.Fatalf("part key %q is flat under %q", k, blob.PartsPrefix)
		}
	}
}
