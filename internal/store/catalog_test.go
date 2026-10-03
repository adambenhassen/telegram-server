package store_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/catalog"
	"github.com/teagramhq/teagram-server/internal/catalogpublish"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestCatalogSnapshotLoadsMetadataCurrentStringsAndHistoryAtomically(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	first := catalogFixture(t, map[string]string{"a": "one", "removed": "old"})
	if _, err := catalogpublish.Publish(ctx, dsn, first, "reviewed-one", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	second := catalogFixture(t, map[string]string{"a": "two", "added": "new"})
	if _, err := catalogpublish.Publish(ctx, dsn, second, "reviewed-two", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish second: %v", err)
	}

	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	snapshot, err := st.LoadCatalogSnapshot(ctx)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	pack := snapshot.Pack(catalog.PackTDesktop, catalog.LanguageEnglish)
	if pack == nil || pack.Version != 2 || pack.OldestVersion != 1 {
		t.Fatalf("pack = %+v, want English version 2 with history from 1", pack)
	}
	if len(pack.Entries) != 2 || pack.Entries[0].Key != "a" || pack.Entries[1].Key != "added" {
		t.Fatalf("current entries = %+v, want a/added", pack.Entries)
	}
	diff := pack.Difference(1)
	if len(diff.Entries) != 3 || diff.Entries[0].Key != "a" || diff.Entries[1].Key != "added" || diff.Entries[2].Key != "removed" || !diff.Entries[2].Deleted {
		t.Fatalf("difference = %+v, want edit and tombstone", diff)
	}
}

func TestCatalogSnapshotSkipsInvalidNonEnglishPackAndKeepsEnglish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	artifact := catalogFixture(t, map[string]string{"key": "value"})
	if _, err := catalogpublish.Publish(ctx, dsn, artifact, "reviewed", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish English: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // test cleanup
	if _, err := conn.Exec(ctx, `
		INSERT INTO language_catalog_packs (
			lang_pack, lang_code, current_version, name, native_name, plural_code,
			current_content_sha256, current_manifest_sha256, source_url, source_revision,
			source_sha256, source_notice, attribution
		) VALUES ('tdesktop', 'de', 1, 'German', 'Deutsch', 'de',
			decode(repeat('a', 64), 'hex'), decode(repeat('b', 64), 'hex'),
			'https://example.test/source', 'revision', decode(repeat('c', 64), 'hex'),
			'notice', 'attribution')
	`); err != nil {
		t.Fatalf("insert invalid pack: %v", err)
	}
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	snapshot, err := st.LoadCatalogSnapshot(ctx)
	if err != nil {
		t.Fatalf("load snapshot: %v", err)
	}
	if snapshot.Pack(catalog.PackTDesktop, catalog.LanguageEnglish) == nil {
		t.Fatalf("English pack missing from snapshot")
	}
	if snapshot.Pack(catalog.PackTDesktop, "de") != nil {
		t.Fatalf("non-English pack loaded")
	}
}

func TestCatalogRefreshFailureKeepsPreviousSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	artifact := catalogFixture(t, map[string]string{"key": "value"})
	if _, err := catalogpublish.Publish(ctx, dsn, artifact, "reviewed", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	if err := st.RefreshCatalogSnapshot(ctx); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	prior := st.CatalogSnapshot()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `DROP TABLE language_catalog_changes`); err != nil {
		_ = conn.Close(ctx) //nolint:errcheck // close after setup failure
		t.Fatalf("drop history table: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := st.RefreshCatalogSnapshot(ctx); err == nil {
		t.Fatal("refresh succeeded with missing history table")
	}
	if got := st.CatalogSnapshot(); got != prior {
		t.Fatalf("failed refresh replaced prior snapshot: got %p want %p", got, prior)
	}
}

func TestCatalogRefreshChecksumFailureKeepsPreviousSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	artifact := catalogFixture(t, map[string]string{"key": "value"})
	if _, err := catalogpublish.Publish(ctx, dsn, artifact, "reviewed", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	if err := st.RefreshCatalogSnapshot(ctx); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	prior := st.CatalogSnapshot()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE language_catalog_packs
		SET current_content_sha256 = set_byte(current_content_sha256, 0, (get_byte(current_content_sha256, 0) + 1) % 256)
		WHERE lang_pack = 'tdesktop' AND lang_code = 'en'
	`); err != nil {
		_ = conn.Close(ctx) //nolint:errcheck // close after setup failure
		t.Fatalf("corrupt checksum: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := st.RefreshCatalogSnapshot(ctx); err == nil {
		t.Fatal("refresh succeeded with an invalid English checksum")
	}
	if got := st.CatalogSnapshot(); got != prior {
		t.Fatalf("failed refresh replaced prior snapshot: got %p want %p", got, prior)
	}
}

func TestCatalogRefreshLatestChangeMismatchKeepsPreviousSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	first := catalogFixture(t, map[string]string{"key": "first"})
	if _, err := catalogpublish.Publish(ctx, dsn, first, "reviewed-one", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	second := catalogFixture(t, map[string]string{"key": "second"})
	if _, err := catalogpublish.Publish(ctx, dsn, second, "reviewed-two", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish second: %v", err)
	}
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	if err := st.RefreshCatalogSnapshot(ctx); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	prior := st.CatalogSnapshot()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE language_catalog_changes
		SET value = 'corrupt'
		WHERE lang_pack = 'tdesktop' AND lang_code = 'en' AND version = 2 AND key = 'key'
	`); err != nil {
		_ = conn.Close(ctx) //nolint:errcheck // close after setup failure
		t.Fatalf("corrupt latest change: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := st.RefreshCatalogSnapshot(ctx); err == nil {
		t.Fatal("refresh succeeded with a latest change that disagrees with current strings")
	}
	if got := st.CatalogSnapshot(); got != prior {
		t.Fatalf("failed refresh replaced prior snapshot: got %p want %p", got, prior)
	}
	if diff := prior.Pack(catalog.PackTDesktop, catalog.LanguageEnglish).Difference(1); len(diff.Entries) != 1 || diff.Entries[0].Value != "second" {
		t.Fatalf("previous snapshot difference = %+v, want validated current value", diff)
	}
}

func TestCatalogRefreshLatestTombstoneMismatchKeepsPreviousSnapshot(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	first := catalogFixture(t, map[string]string{"removed": "value"})
	if _, err := catalogpublish.Publish(ctx, dsn, first, "reviewed-one", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish first: %v", err)
	}
	second := catalogFixture(t, map[string]string{})
	if _, err := catalogpublish.Publish(ctx, dsn, second, "reviewed-two", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish second: %v", err)
	}
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	if err := st.RefreshCatalogSnapshot(ctx); err != nil {
		t.Fatalf("initial refresh: %v", err)
	}
	prior := st.CatalogSnapshot()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		DELETE FROM language_catalog_tombstones
		WHERE lang_pack = 'tdesktop' AND lang_code = 'en' AND version = 2 AND key = 'removed'
	`); err != nil {
		_ = conn.Close(ctx) //nolint:errcheck // close after setup failure
		t.Fatalf("remove latest tombstone: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if err := st.RefreshCatalogSnapshot(ctx); err == nil {
		t.Fatal("refresh succeeded without the current deletion tombstone")
	}
	if got := st.CatalogSnapshot(); got != prior {
		t.Fatalf("failed refresh replaced prior snapshot: got %p want %p", got, prior)
	}
}

func TestCatalogSnapshotRejectsCurrentRowsWithFutureChangeVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	artifact := catalogFixture(t, map[string]string{"key": "value"})
	if _, err := catalogpublish.Publish(ctx, dsn, artifact, "reviewed", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
		t.Fatalf("publish: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	if _, err := conn.Exec(ctx, `
		UPDATE language_catalog_current_strings
		SET last_changed_version = 2
		WHERE lang_pack = 'tdesktop' AND lang_code = 'en' AND key = 'key'
	`); err != nil {
		_ = conn.Close(ctx) //nolint:errcheck // close after setup failure
		t.Fatalf("corrupt current row: %v", err)
	}
	if err := conn.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	if _, err := st.LoadCatalogSnapshot(ctx); err == nil {
		t.Fatal("corrupt current row did not fail snapshot validation")
	}
}

func TestCatalogRefreshOrphanHistoryKeepsPreviousSnapshot(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		remove string
	}{
		{name: "change and tombstone"},
		{name: "change only", remove: "DELETE FROM language_catalog_tombstones"},
		{name: "tombstone only", remove: "DELETE FROM language_catalog_changes"},
		{name: "missing pack metadata", remove: "DELETE FROM language_catalog_packs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dsn := pgtest.DSN(t)
			for i, values := range []map[string]string{{"removed": "value"}, {}} {
				if _, err := catalogpublish.Publish(ctx, dsn, catalogFixture(t, values), "reviewed", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
					t.Fatalf("publish version %d: %v", i+1, err)
				}
			}
			st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
			if err := st.RefreshCatalogSnapshot(ctx); err != nil {
				t.Fatalf("initial refresh: %v", err)
			}
			prior := st.CatalogSnapshot()
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // test cleanup
			if _, err := conn.Exec(ctx, `DELETE FROM language_catalog_current_strings
				WHERE lang_pack = 'tdesktop' AND lang_code = 'en' AND key = 'removed'`); err != nil {
				t.Fatalf("remove current row: %v", err)
			}
			if tc.remove != "" {
				if _, err := conn.Exec(ctx, tc.remove); err != nil {
					t.Fatalf("isolate orphan history: %v", err)
				}
			}
			if err := st.RefreshCatalogSnapshot(ctx); err == nil {
				t.Fatal("refresh succeeded with history lacking a current row")
			}
			if got := st.CatalogSnapshot(); got != prior {
				t.Fatalf("failed refresh replaced prior snapshot: got %p want %p", got, prior)
			}
			pack := prior.Pack(catalog.PackTDesktop, catalog.LanguageEnglish)
			if pack == nil || pack.Version != 2 || len(pack.Entries) != 0 {
				t.Fatalf("prior pack = %+v, want empty version 2", pack)
			}
			diff := pack.Difference(1)
			if diff.FromVersion != 1 || diff.Version != 2 || len(diff.Entries) != 1 || diff.Entries[0].Key != "removed" || !diff.Entries[0].Deleted {
				t.Fatalf("prior difference = %+v, want removed tombstone at version 2", diff)
			}
		})
	}
}

func catalogFixture(t *testing.T, values map[string]string) catalog.Artifact {
	t.Helper()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	var source strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&source, "%q = %q;\n", key, values[key])
	}
	bytes := []byte(source.String())
	sum := sha256.Sum256(bytes)
	artifact, err := catalog.BuildEnglish(bytes, catalog.Source{
		URL:      "https://example.test/tdesktop/lang.strings",
		Revision: "revision",
		SHA256:   hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatalf("build fixture: %v", err)
	}
	return artifact
}
