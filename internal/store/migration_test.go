package store_test

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestOpenRejectsUnmigratedSchema proves Open's fail-fast schema check fires:
// dropping a table the latest migration created makes Open error at startup
// instead of failing later with a confusing "relation does not exist".
func TestOpenRejectsUnmigratedSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx, `DROP TABLE message_events`); err != nil {
		t.Fatalf("drop table: %v", err)
	}

	if _, err := store.Open(ctx, dsn, pgtest.EncKey()); err == nil {
		t.Fatal("Open must fail against an un-migrated schema")
	} else if !strings.Contains(err.Error(), "not migrated") {
		t.Fatalf("want a migration error, got %v", err)
	}
}
