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

// reachableFlatPartObjects is how many in-flight part objects ordinary load
// can hold before row caps bind: 6400 parts per account at the default part
// size, across about 26 accounts each holding a full outstanding set.
const reachableFlatPartObjects = 6400 * 26

// measureFlatPartsDirEntryCeiling fills dir with part-length filenames until
// create returns ENOSPC. It returns how many objects fit and whether the
// filesystem bound.
func measureFlatPartsDirEntryCeiling(t *testing.T, dir string) (int, bool) {
	t.Helper()
	var n int
	for i := range reachableFlatPartObjects * 2 {
		name := fmt.Sprintf("%032x", i)
		path := filepath.Join(dir, name)
		f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			if errors.Is(err, syscall.ENOSPC) {
				if n == 0 {
					t.Fatal("ENOSPC on first create; cannot measure")
				}
				return n, true
			}
			t.Fatalf("create %d: %v", i, err)
		}
		if err := f.Close(); err != nil {
			t.Fatalf("close %d: %v", i, err)
		}
		n++
	}
	return n, false
}

// TestFlatPartsDirEntryCeiling establishes acceptance criterion 1 on the
// runner's filesystem. It is gated behind PARTS_DIR_CEILING_PROBE=1 and run
// from an isolated CI step so the probe does not fill a shared temp directory
// while other test binaries run under go test ./... When the probe binds, the
// logged count is the ceiling. A ceiling at or above reachable in-flight
// volume is a valid AC1 outcome (flat directory does not bind within
// operational volume), not a failure.
func TestFlatPartsDirEntryCeiling(t *testing.T) {
	if os.Getenv("PARTS_DIR_CEILING_PROBE") == "" {
		t.Skip("set PARTS_DIR_CEILING_PROBE=1 to measure the flat parts directory ceiling")
	}

	dir := t.TempDir()
	ceiling, bound := measureFlatPartsDirEntryCeiling(t, dir)
	t.Logf("flat parts directory entry ceiling on %s: count=%d bound=%v", dir, ceiling, bound)
	if !bound {
		t.Logf(
			"flat directory accepted %d part-length filenames without ENOSPC within the probe limit; sharding remains for filesystems that bind earlier",
			ceiling,
		)
		return
	}
	if ceiling >= reachableFlatPartObjects {
		t.Logf(
			"flat ceiling %d is at or above reachable in-flight volume %d; flat directory does not bind within operational volume",
			ceiling, reachableFlatPartObjects,
		)
		return
	}
	t.Logf(
		"flat ceiling %d is below reachable in-flight volume %d; sharding is justified",
		ceiling, reachableFlatPartObjects,
	)
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
