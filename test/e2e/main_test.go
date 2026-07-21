package e2e_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

// TestMain pre-warms the shared Postgres container before any test runs, so the
// one-time cold start (image pull/boot) is not charged against a test's context
// deadline. Without this, the first e2e run on a fresh machine can exceed the
// per-test timeout while the container boots.
func TestMain(m *testing.M) {
	if err := pgtest.Prewarm(); err != nil {
		fmt.Fprintf(os.Stderr, "pgtest prewarm: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
