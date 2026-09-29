package store

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/adambenhassen/telegram-server/internal/catalog"
)

const maxCatalogPacks = 128

type catalogPackID struct {
	pack string
	lang string
}

type catalogPackRow struct {
	id             catalogPackID
	version        int64
	name           string
	nativeName     string
	pluralCode     string
	contentSHA     []byte
	manifestSHA    []byte
	sourceURL      string
	sourceRevision string
	sourceSHA      []byte
	sourceNotice   string
	attribution    string
}

type catalogCurrentRow struct {
	entry              catalog.Entry
	deleted            bool
	lastChangedVersion int64
}

// LoadCatalogSnapshot reads all catalog state in one repeatable-read,
// read-only transaction. A corrupt pack is excluded without preventing a
// separate valid English pack from loading.
func (s *Store) LoadCatalogSnapshot(ctx context.Context) (*catalog.Snapshot, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, fmt.Errorf("catalog snapshot begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	packRows, err := loadCatalogPackRows(ctx, tx)
	if err != nil {
		return nil, err
	}
	oldest, err := loadCatalogOldestVersions(ctx, tx)
	if err != nil {
		return nil, err
	}
	current, err := loadCatalogCurrentStrings(ctx, tx)
	if err != nil {
		return nil, err
	}
	changes, err := loadCatalogChanges(ctx, tx)
	if err != nil {
		return nil, err
	}
	tombstones, err := loadCatalogTombstones(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("catalog snapshot commit: %w", err)
	}

	result := &catalog.Snapshot{Packs: make([]catalog.Pack, 0, len(packRows))}
	for _, row := range packRows {
		if row.id.lang != catalog.LanguageEnglish {
			continue
		}
		id := row.id
		if len(result.Packs) >= maxCatalogPacks {
			s.log.Warn("catalog pack rejected", "reason", "pack limit")
			break
		}
		currentRows, ok := current[id]
		if !ok {
			currentRows = []catalogCurrentRow{}
		}
		entries := make([]catalog.Entry, 0, len(currentRows))
		validCurrent := true
		for _, currentRow := range currentRows {
			if currentRow.lastChangedVersion <= 0 || currentRow.lastChangedVersion > row.version || !validCatalogEntry(currentRow.entry, currentRow.deleted) {
				validCurrent = false
				break
			}
			if !currentRow.deleted {
				entries = append(entries, currentRow.entry)
			}
		}
		if !validCurrent {
			s.log.Warn("catalog pack rejected", "reason", "current strings validation failed")
			continue
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
		artifact := catalog.Artifact{
			SchemaVersion: catalog.ArtifactSchemaVersion,
			Pack:          row.id.pack,
			LanguageCode:  row.id.lang,
			Name:          row.name,
			NativeName:    row.nativeName,
			PluralCode:    row.pluralCode,
			Source: catalog.Source{
				URL:      row.sourceURL,
				Revision: row.sourceRevision,
				SHA256:   hex.EncodeToString(row.sourceSHA),
				Notice:   row.sourceNotice,
			},
			Attribution: row.attribution,
			Entries:     entries,
		}
		contentSHA, contentErr := artifact.ContentSHA256()
		manifestSHA, manifestErr := artifact.ManifestSHA256()
		if contentErr != nil || manifestErr != nil || artifact.Validate() != nil || !bytes.Equal(row.contentSHA, mustDecodeSHA(contentSHA)) || !bytes.Equal(row.manifestSHA, mustDecodeSHA(manifestSHA)) {
			s.log.Warn("catalog pack rejected", "reason", "validation failed")
			continue
		}
		if len(row.contentSHA) != 32 || len(row.manifestSHA) != 32 || len(row.sourceSHA) != 32 {
			s.log.Warn("catalog pack rejected", "reason", "checksum failed")
			continue
		}
		packChanges := append([]catalog.Change(nil), changes[id]...)
		for _, tombstone := range tombstones[id] {
			found := false
			for _, change := range packChanges {
				if change.Version == tombstone.Version && change.Entry.Key == tombstone.Entry.Key {
					found = true
					break
				}
			}
			if !found {
				packChanges = append(packChanges, tombstone)
			}
		}
		packChanges = latestCatalogChanges(packChanges)
		validHistory := true
		for _, change := range packChanges {
			if change.Version > row.version || change.Version <= 0 || !validCatalogEntry(change.Entry, change.Deleted) {
				validHistory = false
				break
			}
		}
		oldestVersion := oldest[id]
		if !validHistory || oldestVersion <= 0 || oldestVersion > row.version {
			s.log.Warn("catalog pack rejected", "reason", "history validation failed")
			continue
		}
		sort.Slice(packChanges, func(i, j int) bool {
			if packChanges[i].Version != packChanges[j].Version {
				return packChanges[i].Version < packChanges[j].Version
			}
			return packChanges[i].Entry.Key < packChanges[j].Entry.Key
		})
		result.Packs = append(result.Packs, catalog.Pack{
			Pack:          row.id.pack,
			LanguageCode:  row.id.lang,
			Name:          row.name,
			NativeName:    row.nativeName,
			PluralCode:    row.pluralCode,
			Version:       row.version,
			OldestVersion: oldestVersion,
			Entries:       entries,
			Changes:       packChanges,
		})
	}
	sort.Slice(result.Packs, func(i, j int) bool {
		if result.Packs[i].Pack != result.Packs[j].Pack {
			return result.Packs[i].Pack < result.Packs[j].Pack
		}
		return result.Packs[i].LanguageCode < result.Packs[j].LanguageCode
	})
	return result, nil
}

func latestCatalogChanges(changes []catalog.Change) []catalog.Change {
	latest := make(map[string]catalog.Change, len(changes))
	for _, change := range changes {
		previous, ok := latest[change.Entry.Key]
		if !ok || change.Version > previous.Version {
			latest[change.Entry.Key] = change
		}
	}
	result := make([]catalog.Change, 0, len(latest))
	for _, change := range latest {
		result = append(result, change)
	}
	return result
}

// RefreshCatalogSnapshot atomically replaces the previous valid snapshot only
// after LoadCatalogSnapshot succeeds. A failed database read leaves the prior
// snapshot untouched.
func (s *Store) RefreshCatalogSnapshot(ctx context.Context) error {
	snapshot, err := s.LoadCatalogSnapshot(ctx)
	if err != nil {
		return err
	}
	s.catalogSnapshot.Store(snapshot)
	return nil
}

// CatalogSnapshot returns the latest complete snapshot, or nil before the
// first successful refresh.
func (s *Store) CatalogSnapshot() *catalog.Snapshot {
	return s.catalogSnapshot.Load()
}

func loadCatalogPackRows(ctx context.Context, tx pgx.Tx) ([]catalogPackRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT lang_pack, lang_code, current_version, name, native_name,
		       plural_code, current_content_sha256, current_manifest_sha256,
		       source_url, source_revision, source_sha256, source_notice, attribution
		FROM language_catalog_packs
		ORDER BY lang_pack, lang_code
	`)
	if err != nil {
		return nil, fmt.Errorf("catalog snapshot metadata: %w", err)
	}
	defer rows.Close()
	var result []catalogPackRow
	for rows.Next() {
		var row catalogPackRow
		if err := rows.Scan(&row.id.pack, &row.id.lang, &row.version, &row.name, &row.nativeName,
			&row.pluralCode, &row.contentSHA, &row.manifestSHA, &row.sourceURL, &row.sourceRevision,
			&row.sourceSHA, &row.sourceNotice, &row.attribution); err != nil {
			return nil, fmt.Errorf("catalog snapshot metadata row: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog snapshot metadata rows: %w", err)
	}
	return result, nil
}

func loadCatalogOldestVersions(ctx context.Context, tx pgx.Tx) (map[catalogPackID]int64, error) {
	rows, err := tx.Query(ctx, `
		SELECT lang_pack, lang_code, version
		FROM language_catalog_versions
		ORDER BY lang_pack, lang_code, version
	`)
	if err != nil {
		return nil, fmt.Errorf("catalog snapshot versions: %w", err)
	}
	defer rows.Close()
	result := make(map[catalogPackID]int64)
	for rows.Next() {
		var id catalogPackID
		var version int64
		if err := rows.Scan(&id.pack, &id.lang, &version); err != nil {
			return nil, fmt.Errorf("catalog snapshot version row: %w", err)
		}
		if result[id] == 0 || version < result[id] {
			result[id] = version
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog snapshot version rows: %w", err)
	}
	return result, nil
}

func loadCatalogCurrentStrings(ctx context.Context, tx pgx.Tx) (map[catalogPackID][]catalogCurrentRow, error) {
	rows, err := tx.Query(ctx, `
		SELECT lang_pack, lang_code, key, kind, value, plural_zero, plural_one,
		       plural_two, plural_few, plural_many, plural_other, deleted,
		       last_changed_version
		FROM language_catalog_current_strings
	`)
	if err != nil {
		return nil, fmt.Errorf("catalog snapshot current strings: %w", err)
	}
	defer rows.Close()
	result := make(map[catalogPackID][]catalogCurrentRow)
	for rows.Next() {
		var id catalogPackID
		var entry catalog.Entry
		var deleted bool
		var lastChangedVersion int64
		if err := scanCatalogEntry(rows, &id, &entry, &deleted, &lastChangedVersion); err != nil {
			return nil, fmt.Errorf("catalog snapshot current string row: %w", err)
		}
		result[id] = append(result[id], catalogCurrentRow{entry: entry, deleted: deleted, lastChangedVersion: lastChangedVersion})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog snapshot current string rows: %w", err)
	}
	return result, nil
}

func loadCatalogChanges(ctx context.Context, tx pgx.Tx) (map[catalogPackID][]catalog.Change, error) {
	rows, err := tx.Query(ctx, `
		SELECT lang_pack, lang_code, version, key, kind, value, plural_zero,
		       plural_one, plural_two, plural_few, plural_many, plural_other, deleted
		FROM language_catalog_changes
	`)
	if err != nil {
		return nil, fmt.Errorf("catalog snapshot changes: %w", err)
	}
	defer rows.Close()
	result := make(map[catalogPackID][]catalog.Change)
	for rows.Next() {
		var id catalogPackID
		var version int64
		var entry catalog.Entry
		var deleted bool
		if err := scanCatalogEntryWithVersion(rows, &id, &version, &entry, &deleted); err != nil {
			return nil, fmt.Errorf("catalog snapshot change row: %w", err)
		}
		result[id] = append(result[id], catalog.Change{Version: version, Entry: entry, Deleted: deleted})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog snapshot change rows: %w", err)
	}
	return result, nil
}

func loadCatalogTombstones(ctx context.Context, tx pgx.Tx) (map[catalogPackID][]catalog.Change, error) {
	rows, err := tx.Query(ctx, `
		SELECT lang_pack, lang_code, version, key
		FROM language_catalog_tombstones
	`)
	if err != nil {
		return nil, fmt.Errorf("catalog snapshot tombstones: %w", err)
	}
	defer rows.Close()
	result := make(map[catalogPackID][]catalog.Change)
	for rows.Next() {
		var id catalogPackID
		var version int64
		var key string
		if err := rows.Scan(&id.pack, &id.lang, &version, &key); err != nil {
			return nil, fmt.Errorf("catalog snapshot tombstone row: %w", err)
		}
		result[id] = append(result[id], catalog.Change{Version: version, Entry: catalog.Entry{Key: key}, Deleted: true})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("catalog snapshot tombstone rows: %w", err)
	}
	return result, nil
}

type catalogRowScanner interface {
	Scan(...any) error
}

func scanCatalogEntry(row catalogRowScanner, id *catalogPackID, entry *catalog.Entry, deleted *bool, lastChangedVersion *int64) error {
	var kind int16
	var key, value, zero, one, two, few, many, other pgtype.Text
	if err := row.Scan(&id.pack, &id.lang, &key, &kind, &value, &zero, &one, &two, &few, &many, &other, deleted, lastChangedVersion); err != nil {
		return err
	}
	*entry = decodeCatalogEntry(key.String, kind, value, zero, one, two, few, many, other)
	return nil
}

func scanCatalogEntryWithVersion(row catalogRowScanner, id *catalogPackID, version *int64, entry *catalog.Entry, deleted *bool) error {
	var kind int16
	var key, value, zero, one, two, few, many, other pgtype.Text
	if err := row.Scan(&id.pack, &id.lang, version, &key, &kind, &value, &zero, &one, &two, &few, &many, &other, deleted); err != nil {
		return err
	}
	*entry = decodeCatalogEntry(key.String, kind, value, zero, one, two, few, many, other)
	return nil
}

func decodeCatalogEntry(key string, kind int16, value, zero, one, two, few, many, other pgtype.Text) catalog.Entry {
	entry := catalog.Entry{Key: key, Value: value.String}
	switch kind {
	case 0:
		if anyCatalogTextValid(zero, one, two, few, many, other) {
			entry.Key = ""
		}
	case 1:
		if value.String != "" {
			entry.Key = ""
		}
		entry.Value = ""
		entry.Plural = &catalog.Plural{Other: other.String}
		entry.Plural.Zero = catalogTextPointer(zero)
		entry.Plural.One = catalogTextPointer(one)
		entry.Plural.Two = catalogTextPointer(two)
		entry.Plural.Few = catalogTextPointer(few)
		entry.Plural.Many = catalogTextPointer(many)
	default:
		entry.Key = ""
	}
	return entry
}

func anyCatalogTextValid(values ...pgtype.Text) bool {
	for _, value := range values {
		if value.Valid {
			return true
		}
	}
	return false
}

func catalogTextPointer(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func validCatalogEntry(entry catalog.Entry, deleted bool) bool {
	if entry.Key == "" || !utf8.ValidString(entry.Key) || len(entry.Key) > catalog.MaxKeyBytes || strings.IndexByte(entry.Key, 0) >= 0 || strings.ContainsRune(entry.Key, '#') {
		return false
	}
	if deleted {
		return entry.Plural == nil && entry.Value == ""
	}
	if entry.Plural == nil {
		return validCatalogValue(entry.Value)
	}
	if entry.Plural.Other == "" || !validCatalogValue(entry.Plural.Other) {
		return false
	}
	for _, value := range []*string{entry.Plural.Zero, entry.Plural.One, entry.Plural.Two, entry.Plural.Few, entry.Plural.Many} {
		if value != nil && !validCatalogValue(*value) {
			return false
		}
	}
	return true
}

func validCatalogValue(value string) bool {
	return utf8.ValidString(value) && strings.IndexByte(value, 0) < 0 && len(value) <= catalog.MaxValueBytes
}

func mustDecodeSHA(value string) []byte {
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil
	}
	return decoded
}
