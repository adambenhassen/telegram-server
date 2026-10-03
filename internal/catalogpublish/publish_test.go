package catalogpublish_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/teagramhq/teagram-server/internal/catalog"
	"github.com/teagramhq/teagram-server/internal/catalogpublish"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

type Result = catalogpublish.Result
type PublishOptions = catalogpublish.PublishOptions

var Publish = catalogpublish.Publish

func TestPublishIsAtomicIdempotentAndRetainsHistory(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	pool := testPool(t, dsn)

	first := testArtifact(t, map[string]string{"a": "one", "gone": "old", "stable": "same"})
	result, err := Publish(ctx, dsn, first, strings.Repeat("1", 40), PublishOptions{OSUser: "reviewer"})
	if err != nil {
		t.Fatalf("publish first: %v", err)
	}
	if !result.Changed || result.NewVersion != 1 {
		t.Fatalf("first result = %+v, want changed version 1", result)
	}
	countsBeforeFailure := readCatalogHistoryCounts(t, ctx, pool)

	second := testArtifact(t, map[string]string{"a": "two", "new": "added", "stable": "same"})
	_, err = Publish(ctx, dsn, second, strings.Repeat("2", 40), PublishOptions{
		OSUser: "reviewer",
		BeforeActivate: func(context.Context) error {
			return errors.New("injected failure")
		},
	})
	if err == nil || !strings.Contains(err.Error(), "injected failure") {
		t.Fatalf("failed publish error = %v, want injected failure", err)
	}
	assertCurrentVersion(t, ctx, pool, 1)
	assertCurrentKeys(t, ctx, pool, map[string]string{"a": "one", "gone": "old", "stable": "same"})
	if countsAfterFailure := readCatalogHistoryCounts(t, ctx, pool); countsAfterFailure != countsBeforeFailure {
		t.Fatalf("injected failure changed history counts: before %v after %v", countsBeforeFailure, countsAfterFailure)
	}

	result, err = Publish(ctx, dsn, second, strings.Repeat("2", 40), PublishOptions{OSUser: "reviewer"})
	if err != nil {
		t.Fatalf("publish second: %v", err)
	}
	if !result.Changed || result.OldVersion != 1 || result.NewVersion != 2 {
		t.Fatalf("second result = %+v, want 1 -> 2", result)
	}
	assertCurrentVersion(t, ctx, pool, 2)
	if counts := readCatalogHistoryCounts(t, ctx, pool); counts[0] != 2 {
		t.Fatalf("retained version count = %d, want both versions", counts[0])
	}
	assertCurrentKeys(t, ctx, pool, map[string]string{"a": "two", "new": "added", "stable": "same"})
	var stableVersion, editedVersion int64
	if err := pool.QueryRow(ctx, `SELECT last_changed_version FROM language_catalog_current_strings WHERE lang_pack = 'tdesktop' AND lang_code = 'en' AND key = 'stable'`).Scan(&stableVersion); err != nil {
		t.Fatalf("read stable version: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT last_changed_version FROM language_catalog_current_strings WHERE lang_pack = 'tdesktop' AND lang_code = 'en' AND key = 'a'`).Scan(&editedVersion); err != nil {
		t.Fatalf("read edited version: %v", err)
	}
	if stableVersion != 1 || editedVersion != 2 {
		t.Fatalf("last changed versions = stable %d, edited %d; want 1 and 2", stableVersion, editedVersion)
	}

	var versions, changes, tombstones, audits int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_versions`).Scan(&versions); err != nil {
		t.Fatalf("count versions: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_changes`).Scan(&changes); err != nil {
		t.Fatalf("count changes: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_tombstones`).Scan(&tombstones); err != nil {
		t.Fatalf("count tombstones: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_publication_audit`).Scan(&audits); err != nil {
		t.Fatalf("count audits: %v", err)
	}
	result, err = Publish(ctx, dsn, second, strings.Repeat("2", 40), PublishOptions{OSUser: "reviewer"})
	if err != nil {
		t.Fatalf("republish second: %v", err)
	}
	if result.Changed || result.NewVersion != 2 {
		t.Fatalf("idempotent result = %+v, want unchanged version 2", result)
	}
	var versionsAfter, changesAfter, tombstonesAfter, auditsAfter int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_versions`).Scan(&versionsAfter); err != nil {
		t.Fatalf("count versions after: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_changes`).Scan(&changesAfter); err != nil {
		t.Fatalf("count changes after: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_tombstones`).Scan(&tombstonesAfter); err != nil {
		t.Fatalf("count tombstones after: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM language_catalog_publication_audit`).Scan(&auditsAfter); err != nil {
		t.Fatalf("count audits after: %v", err)
	}
	if versionsAfter != versions || changesAfter != changes || tombstonesAfter != tombstones || auditsAfter != audits {
		t.Fatalf("idempotent publish changed counts: before %d/%d/%d/%d after %d/%d/%d/%d", versions, changes, tombstones, audits, versionsAfter, changesAfter, tombstonesAfter, auditsAfter)
	}

	var auditText string
	if err := pool.QueryRow(ctx, `SELECT row_to_json(language_catalog_publication_audit)::text FROM language_catalog_publication_audit ORDER BY id DESC LIMIT 1`).Scan(&auditText); err != nil {
		t.Fatalf("read audit: %v", err)
	}
	if strings.Contains(auditText, "one") || strings.Contains(auditText, "two") || strings.Contains(auditText, "added") {
		t.Fatalf("audit contains catalog values: %s", auditText)
	}
}

func TestConcurrentDistinctPublicationsReceiveMonotonicVersions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	pool := testPool(t, dsn)
	artifacts := []catalog.Artifact{
		testArtifact(t, map[string]string{"key": "one"}),
		testArtifact(t, map[string]string{"key": "two"}),
	}
	results := make([]Result, len(artifacts))
	errs := make([]error, len(artifacts))
	var wg sync.WaitGroup
	for i := range artifacts {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			results[i], errs[i] = Publish(ctx, dsn, artifacts[i], fmt.Sprintf("%040d", i+1), PublishOptions{OSUser: "reviewer"})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
	}
	if results[0].NewVersion == results[1].NewVersion || results[0].NewVersion+results[1].NewVersion != 3 {
		t.Fatalf("concurrent results = %+v, want versions 1 and 2", results)
	}
	assertCurrentVersion(t, ctx, pool, 2)
	winner := 0
	if results[1].NewVersion == 2 {
		winner = 1
	}
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	snapshot, err := st.LoadCatalogSnapshot(ctx)
	if err != nil {
		t.Fatalf("load concurrent snapshot: %v", err)
	}
	pack := snapshot.Pack(catalog.PackTDesktop, catalog.LanguageEnglish)
	if pack == nil || pack.Version != 2 || pack.OldestVersion != 1 || len(pack.Entries) != 1 ||
		pack.Entries[0].Key != "key" || pack.Entries[0].Value != artifacts[winner].Entries[0].Value {
		t.Fatalf("final snapshot = %+v, want complete version-2 publication %+v", pack, artifacts[winner].Entries)
	}
	if len(pack.Changes) != 1 || pack.Changes[0].Version != 2 || pack.Changes[0].Entry.Value != artifacts[winner].Entries[0].Value {
		t.Fatalf("concurrent history = %+v, want latest complete version 2", pack.Changes)
	}
}

func TestPublishRejectsInvalidArtifactBeforeOpeningDatabase(t *testing.T) {
	t.Parallel()
	artifact := testArtifact(t, map[string]string{"key": "value"})
	artifact.LanguageCode = "de"
	_, err := Publish(context.Background(), "not-a-dsn", artifact, strings.Repeat("1", 40), PublishOptions{})
	if !errors.Is(err, catalog.ErrEnglishOnly) {
		t.Fatalf("error = %v, want English-only validation error", err)
	}
}

func testArtifact(t *testing.T, values map[string]string) catalog.Artifact {
	t.Helper()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	// The source checksum is tied to the exact source bytes, while map ordering
	// is made deterministic here so concurrent publication fixtures are stable.
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	var raw strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&raw, "%q = %q;\n", key, values[key])
	}
	sourceBytes := []byte(raw.String())
	sum := sha256.Sum256(sourceBytes)
	artifact, err := catalog.BuildEnglish(sourceBytes, catalog.Source{
		URL:      "https://example.test/tdesktop/lang.strings",
		Revision: "revision",
		SHA256:   hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatalf("build fixture: %v", err)
	}
	return artifact
}

func assertCurrentVersion(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want int64) {
	t.Helper()
	var got int64
	if err := pool.QueryRow(ctx, `SELECT current_version FROM language_catalog_packs WHERE lang_pack = 'tdesktop' AND lang_code = 'en'`).Scan(&got); err != nil {
		t.Fatalf("read current version: %v", err)
	}
	if got != want {
		t.Fatalf("current version = %d, want %d", got, want)
	}
}

func assertCurrentKeys(t *testing.T, ctx context.Context, pool *pgxpool.Pool, want map[string]string) {
	t.Helper()
	rows, err := pool.Query(ctx, `SELECT key, value FROM language_catalog_current_strings WHERE lang_pack = 'tdesktop' AND lang_code = 'en' AND deleted = false ORDER BY key`)
	if err != nil {
		t.Fatalf("read current strings: %v", err)
	}
	defer rows.Close()
	got := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			t.Fatalf("scan current string: %v", err)
		}
		got[key] = value
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("current string rows: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("current strings = %v, want %v", got, want)
	}
	for key, value := range want {
		if got[key] != value {
			t.Fatalf("current string %q = %q, want %q", key, got[key], value)
		}
	}
}

func readCatalogHistoryCounts(t *testing.T, ctx context.Context, pool *pgxpool.Pool) [4]int64 {
	t.Helper()
	var counts [4]int64
	err := pool.QueryRow(ctx, `
		SELECT
			(SELECT count(*) FROM language_catalog_versions),
			(SELECT count(*) FROM language_catalog_changes),
			(SELECT count(*) FROM language_catalog_tombstones),
			(SELECT count(*) FROM language_catalog_publication_audit)
	`).Scan(&counts[0], &counts[1], &counts[2], &counts[3])
	if err != nil {
		t.Fatalf("read catalog history counts: %v", err)
	}
	return counts
}

func testPool(t *testing.T, dsn string) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatalf("open pool: %v", err)
	}
	t.Cleanup(pool.Close)
	return pool
}
