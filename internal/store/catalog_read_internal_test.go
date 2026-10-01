package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/catalog"
	"github.com/adambenhassen/telegram-server/internal/catalogpublish"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
)

func TestCatalogHistoryLoadIsBoundedByCurrentKeys(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	for version := 1; version <= 13; version++ {
		values := map[string]string{"a": fmt.Sprintf("a-%d", version)}
		if version == 1 || version%2 == 1 {
			values["b"] = fmt.Sprintf("b-%d", version)
		}
		artifact := catalogReadFixture(t, values)
		if _, err := catalogpublish.Publish(ctx, dsn, artifact, "reviewed", catalogpublish.PublishOptions{OSUser: "tester"}); err != nil {
			t.Fatalf("publish version %d: %v", version, err)
		}
	}

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // test cleanup
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin snapshot: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // test cleanup

	id := catalogPackID{pack: catalog.PackTDesktop, lang: catalog.LanguageEnglish}
	changes, err := loadCatalogChanges(ctx, tx)
	if err != nil {
		t.Fatalf("load latest changes: %v", err)
	}
	if got := len(changes[id]); got != 2 {
		t.Fatalf("loaded %d change rows for two current keys, want exactly two", got)
	}
	byKey := make(map[string]catalog.Change, len(changes[id]))
	for _, change := range changes[id] {
		byKey[change.Entry.Key] = change
	}
	if got := byKey["a"]; got.Version != 13 || got.Entry.Value != "a-13" || got.Deleted {
		t.Fatalf("latest edit = %+v, want version 13 value a-13", got)
	}
	if got := byKey["b"]; got.Version != 13 || got.Entry.Value != "b-13" || got.Deleted {
		t.Fatalf("latest edit = %+v, want restored value at version 13", got)
	}

	tombstones, err := loadCatalogTombstones(ctx, tx)
	if err != nil {
		t.Fatalf("load latest tombstones: %v", err)
	}
	if got := len(tombstones[id]); got != 1 || tombstones[id][0].Version != 12 || tombstones[id][0].Entry.Key != "b" {
		t.Fatalf("latest tombstones = %+v, want only b at version 12", tombstones[id])
	}

	st, err := Open(ctx, dsn, pgtest.EncKey(), WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // test cleanup
	snapshot, err := st.LoadCatalogSnapshot(ctx)
	if err != nil {
		t.Fatalf("load restored snapshot: %v", err)
	}
	pack := snapshot.Pack(catalog.PackTDesktop, catalog.LanguageEnglish)
	if pack == nil {
		t.Fatal("restored English pack missing from snapshot")
	}
	difference := pack.Difference(12)
	if len(difference.Entries) != 2 || difference.Entries[1].Key != "b" || difference.Entries[1].Value != "b-13" || difference.Entries[1].Deleted {
		t.Fatalf("difference after deletion and restore = %+v, want b restored at version 13", difference)
	}
}

func TestCatalogHistoryQueriesSeekPastRetainedVersions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, pgtest.DSN(t))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // test cleanup
	if _, err := conn.Exec(ctx, `
		INSERT INTO language_catalog_changes (lang_pack, lang_code, version, key, kind, value, deleted)
		SELECT 'tdesktop', 'en', v, k, 0, '', true
		FROM generate_series(1, 10000) v CROSS JOIN (VALUES ('a'), ('b')) keys(k);
		INSERT INTO language_catalog_tombstones (lang_pack, lang_code, version, key)
		SELECT 'tdesktop', 'en', v, 'b' FROM generate_series(1, 10000) v;
		ANALYZE language_catalog_changes;
		ANALYZE language_catalog_tombstones;
	`); err != nil {
		t.Fatalf("seed retained history: %v", err)
	}
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatalf("begin snapshot: %v", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // test cleanup
	id := catalogPackID{pack: catalog.PackTDesktop, lang: catalog.LanguageEnglish}
	changes, err := loadCatalogChanges(ctx, tx)
	if err != nil {
		t.Fatalf("load latest changes: %v", err)
	}
	if len(changes[id]) != 2 || changes[id][0].Version != 10000 || changes[id][1].Version != 10000 {
		t.Fatalf("latest changes = %+v, want two keys at version 10000", changes[id])
	}
	tombstones, err := loadCatalogTombstones(ctx, tx)
	if err != nil {
		t.Fatalf("load latest tombstones: %v", err)
	}
	if len(tombstones[id]) != 1 || tombstones[id][0].Version != 10000 {
		t.Fatalf("latest tombstones = %+v, want one key at version 10000", tombstones[id])
	}
	for _, query := range []string{catalogLatestChangesSQL, catalogLatestTombstonesSQL} {
		var raw []byte
		if err := tx.QueryRow(ctx, "EXPLAIN (ANALYZE, FORMAT JSON) "+query, catalog.LanguageEnglish).Scan(&raw); err != nil {
			t.Fatalf("explain history query: %v", err)
		}
		var plans []struct {
			Plan catalogHistoryPlan `json:"Plan"`
		}
		if err := json.Unmarshal(raw, &plans); err != nil {
			t.Fatalf("decode query plan: %v", err)
		}
		// A full scan or DISTINCT over retained versions visits thousands of
		// rows. Index seeks should visit only the latest row for each key.
		if visited := plans[0].Plan.historyRowsVisited(); visited > 32 {
			t.Fatalf("history query visited %.0f rows for at most two keys: %s", visited, raw)
		}
	}
}

type catalogHistoryPlan struct {
	Relation string               `json:"Relation Name"`
	Rows     float64              `json:"Actual Rows"`
	Loops    float64              `json:"Actual Loops"`
	Removed  float64              `json:"Rows Removed by Filter"`
	Plans    []catalogHistoryPlan `json:"Plans"`
}

func (p catalogHistoryPlan) historyRowsVisited() float64 {
	var visited float64
	if p.Relation == "language_catalog_changes" || p.Relation == "language_catalog_tombstones" {
		visited = (p.Rows + p.Removed) * p.Loops
	}
	for _, child := range p.Plans {
		visited += child.historyRowsVisited()
	}
	return visited
}

func catalogReadFixture(t *testing.T, values map[string]string) catalog.Artifact {
	t.Helper()
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var source strings.Builder
	for _, key := range keys {
		fmt.Fprintf(&source, "%q = %q;\n", key, values[key])
	}
	raw := []byte(source.String())
	sum := sha256.Sum256(raw)
	artifact, err := catalog.BuildEnglish(raw, catalog.Source{
		URL:      "https://example.test/tdesktop/lang.strings",
		Revision: "revision",
		SHA256:   hex.EncodeToString(sum[:]),
	})
	if err != nil {
		t.Fatalf("build catalog fixture: %v", err)
	}
	return artifact
}
