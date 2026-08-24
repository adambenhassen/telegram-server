package store_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestOpenToleratesPoolParamsInDSN pins that a DSN tuning the pool through its
// URL — a documented pgxpool feature, accepted since the pool existed — still
// opens. The schema-check connection must be derived from the already-parsed
// config rather than re-parsing the raw DSN: pgx.Connect reads pool_* keys as
// runtime params and Postgres rejects any startup packet carrying them with
// "unrecognized configuration parameter".
func TestOpenToleratesPoolParamsInDSN(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t) + "&pool_max_conns=4"
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatalf("open with pool_max_conns in DSN: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	})
}
