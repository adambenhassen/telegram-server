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
// carries the current schema, so undo that migration's artifacts by hand
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

// TestOpenRejectsPartBlobPayloadNotDropped proves the sentinel's payload clause
// fires on its own: with the migration's size and blob_key columns already in
// place, the one thing left is dropping payload. Without that clause the
// sentinel would see a migrated schema where the part bytes are still in
// Postgres.
func TestOpenRejectsPartBlobPayloadNotDropped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx, `ALTER TABLE upload_parts ADD COLUMN payload BYTEA NOT NULL DEFAULT ''`); err != nil {
		t.Fatalf("re-add payload: %v", err)
	}

	requireOpenMigrationError(t, ctx, dsn)
}

// TestOpenRejectsMissingMessageFileIdx proves the sentinel covers an index and
// not just tables and columns. A missing index shows up in no column check, so
// without this clause a database that stopped at the part-blob migration opens
// clean and then plans the per-row Seq Scan the index exists to remove.
func TestOpenRejectsMissingMessageFileIdx(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close
	if _, err := conn.Exec(ctx, `DROP INDEX messages_file_idx`); err != nil {
		t.Fatalf("drop index: %v", err)
	}

	requireOpenMigrationError(t, ctx, dsn)
}

// TestOpenRejectsPartBlobMigrationMissing proves the sentinel catches a
// migrations/ directory that stops short of the part-blob migration: a fresh
// database built from every migration file except that one must fail Open, not
// pass it and die on the first upload.
func TestOpenRejectsPartBlobMigrationMissing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	migs, err := os.ReadDir(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("read migrations dir: %v", err)
	}
	// The sentinel guards against a migrations/ directory that stops short of
	// the part-blob migration: the one that adds the blob_key column. Find it
	// by what it does rather than by position or by name. Position would let an
	// index-only migration added after it shift the "latest", leaving the test
	// withholding a file whose absence is harmless; name would match this
	// branch's own upload_parts_blob_key_idx just as readily as the column
	// migration it is named after.
	//
	// The discriminator is "blob_key" together with "ADD COLUMN", and it takes
	// the last match: exactly one file in migrations/ adds a column by that
	// name today. A future migration that adds another column called blob_key —
	// on any table — moves the selector onto it, and the sentinel this test
	// drives is upload_parts.blob_key specifically. Whoever writes that
	// migration has to narrow this, and the failure will be loud rather than
	// vacuous: withholding the wrong file leaves the startup sentinel's
	// conditions satisfied, so Open succeeds and the assertion below fires.
	var latest string
	for _, e := range migs {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		b, err := os.ReadFile(filepath.Join("..", "..", "migrations", e.Name()))
		if err != nil {
			t.Fatalf("read migration %s: %v", e.Name(), err)
		}
		if strings.Contains(string(b), "blob_key") && strings.Contains(string(b), "ADD COLUMN") {
			latest = e.Name()
		}
	}
	if latest == "" {
		t.Fatal("part-blob migration not found")
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
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		// Skip the part-blob migration and every migration that comes after
		// it: they may depend on the blob_key column it adds, and one of them
		// does — upload_parts_blob_key_idx indexes that column, so applying it
		// without the column is an error rather than a schema short of the
		// sentinel.
		if e.Name() >= latest {
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
