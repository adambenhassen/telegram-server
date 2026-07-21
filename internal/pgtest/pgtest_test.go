package pgtest_test

import (
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

// TestDSNIsolatesDatabases proves isolation deterministically: create a table in
// DB A, then get a second DB B and assert it does not see A's table. Sequential
// within one test body so B is always requested after A's table exists — unlike
// two parallel tests, which could race and both pass even if DSN returned the
// same database.
func TestDSNIsolatesDatabases(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	connA, err := pgx.Connect(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("connect A: %v", err)
	}
	defer func() { _ = connA.Close(ctx) }() //nolint:errcheck // best-effort close in test
	if _, err := connA.Exec(ctx, `CREATE TABLE only_here (id int)`); err != nil {
		t.Fatalf("create table in A: %v", err)
	}

	connB, err := pgx.Connect(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("connect B: %v", err)
	}
	defer func() { _ = connB.Close(ctx) }() //nolint:errcheck // best-effort close in test

	var exists bool
	if err := connB.QueryRow(ctx,
		`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'only_here')`,
	).Scan(&exists); err != nil {
		t.Fatalf("query B: %v", err)
	}
	if exists {
		t.Error("table leaked from database A into database B")
	}
}
