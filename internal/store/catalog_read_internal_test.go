package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
