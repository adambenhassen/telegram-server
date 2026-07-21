package pgtest_test

import (
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

func TestDSNIsIsolated(t *testing.T) {
	t.Parallel()
	dsn := pgtest.DSN(t)

	ctx := t.Context()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close in test

	if _, err := conn.Exec(ctx, `CREATE TABLE only_here (id int)`); err != nil {
		t.Fatalf("exec in isolated db: %v", err)
	}
}

func TestDSNSecondDBIsSeparate(t *testing.T) {
	t.Parallel()
	// A second isolated DB must not see the first test's table.
	dsn := pgtest.DSN(t)
	ctx := t.Context()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close in test

	var exists bool
	if err := conn.QueryRow(ctx,
		`SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'only_here')`,
	).Scan(&exists); err != nil {
		t.Fatalf("query: %v", err)
	}
	if exists {
		t.Error("table leaked across isolated databases")
	}
}
