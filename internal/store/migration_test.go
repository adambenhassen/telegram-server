package store_test

import (
	"context"
	"os"
	"path/filepath"
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

	requireOpenMigrationError(t, ctx, dsn)
}

// TestOpenRejectsPrePartBlobSchema proves the sentinel also catches the state a
// deployment can be in before the part-blob migration is applied: the template
// carries the latest schema, so undo the newest migration's artifacts by hand
// (re-add payload, drop size and blob_key) and Open must fail at startup rather
// than later, on the first upload query that names a missing column.
func TestOpenRejectsPrePartBlobSchema(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx, `ALTER TABLE upload_parts DROP COLUMN size, DROP COLUMN blob_key, ADD COLUMN payload BYTEA NOT NULL DEFAULT ''`); err != nil {
		t.Fatalf("revert part-blob migration: %v", err)
	}

	requireOpenMigrationError(t, ctx, dsn)
}

// TestOpenRejectsPartBlobMigrationMissing proves the sentinel catches a
// migrations/ directory that stops short of the part-blob migration: a fresh
// database built from every migration file except the newest one must fail
// Open, not pass it and die on the first upload.
func TestOpenRejectsPartBlobMigrationMissing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	migs, err := os.ReadDir(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	var latest string
	for _, e := range migs {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") && e.Name() > latest {
			latest = e.Name()
		}
	}
	if latest == "" {
		t.Fatal("no migration files found")
	}

	admin, err := pgx.Connect(ctx, pgtest.AdminDSN())
	if err != nil {
		t.Fatalf("admin connect: %v", err)
	}
	defer func() { _ = admin.Close(ctx) }() //nolint:errcheck // best-effort close

	name := "t_" + pgtest.RandomHex()
	if _, err := admin.Exec(ctx, `CREATE DATABASE `+name); err != nil {
		t.Fatalf("create db: %v", err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		conn, err := pgx.Connect(ctx, pgtest.AdminDSN())
		if err != nil {
			t.Logf("cleanup connect: %v", err)
			return
		}
		defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
		if _, err := conn.Exec(ctx, `DROP DATABASE IF EXISTS `+name+` WITH (FORCE)`); err != nil {
			t.Logf("cleanup drop %s: %v", name, err)
		}
	})

	dsn := pgtest.DSNFrom(name)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	for _, e := range migs {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") || e.Name() == latest {
			continue
		}
		b, err := os.ReadFile(filepath.Join("..", "..", "migrations", e.Name()))
		if err != nil {
			t.Fatalf("read migration %s: %v", e.Name(), err)
		}
		if _, err := conn.Exec(ctx, string(b)); err != nil {
			t.Fatalf("apply migration %s: %v", e.Name(), err)
		}
	}

	requireOpenMigrationError(t, ctx, dsn)
}

// requireOpenMigrationError asserts Open fails with the migration error, not a
// connection error or nil.
func requireOpenMigrationError(t *testing.T, ctx context.Context, dsn string) {
	t.Helper()
	if _, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t))); err == nil {
		t.Fatal("Open must fail against an un-migrated schema")
	} else if !strings.Contains(err.Error(), "not migrated") {
		t.Fatalf("want a migration error, got %v", err)
	}
}
