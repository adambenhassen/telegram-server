// Package catalogpublish contains the write-only catalog publication path.
// Runtime packages must not import it; the catalogctl command is its explicit
// executable entry point.
package catalogpublish

import (
	"context"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"os/user"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/catalog"
)

// PublishOptions carries the reviewed execution identity and the test-only
// pre-activation fault seam. The seam is deliberately before pointer
// activation, so a returned error rolls back all rows in the transaction.
type PublishOptions struct {
	OSUser         string
	BeforeActivate func(context.Context) error
}

// Result describes one committed publication or an idempotent no-op.
type Result struct {
	Changed        bool
	OldVersion     int64
	NewVersion     int64
	ContentSHA256  string
	ManifestSHA256 string
}

type currentRow struct {
	Version       int64
	ContentSHA256 []byte
}

// Publish validates an artifact and publishes it through a dedicated
// Postgres connection. The runtime server has no call path to this package.
func Publish(ctx context.Context, dsn string, artifact catalog.Artifact, reviewedCommit string, opts PublishOptions) (Result, error) {
	if err := artifact.Validate(); err != nil {
		return Result{}, err
	}
	if strings.TrimSpace(reviewedCommit) == "" || strings.ContainsAny(reviewedCommit, " \t\r\n") {
		return Result{}, errors.New("catalog: invalid reviewed source commit")
	}
	if opts.OSUser == "" {
		opts.OSUser = currentOSUser()
	}
	contentSHA, err := artifact.ContentSHA256()
	if err != nil {
		return Result{}, err
	}
	manifestSHA, err := artifact.ManifestSHA256()
	if err != nil {
		return Result{}, err
	}
	contentBytes, err := hex.DecodeString(contentSHA)
	if err != nil {
		return Result{}, catalog.ErrChecksumMismatch
	}
	manifestBytes, err := hex.DecodeString(manifestSHA)
	if err != nil {
		return Result{}, catalog.ErrChecksumMismatch
	}
	sourceBytes, err := hex.DecodeString(artifact.Source.SHA256)
	if err != nil {
		return Result{}, catalog.ErrChecksumMismatch
	}
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return Result{}, errors.New("catalog: invalid database configuration")
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return Result{}, errors.New("catalog: database connection failed")
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return Result{}, errors.New("catalog: database ping failed")
	}
	return publishWithPool(ctx, pool, artifact, reviewedCommit, opts, contentBytes, manifestBytes, sourceBytes, contentSHA, manifestSHA)
}

func publishWithPool(ctx context.Context, pool *pgxpool.Pool, artifact catalog.Artifact, reviewedCommit string, opts PublishOptions, contentSHA, manifestSHA, sourceSHA []byte, contentHex, manifestHex string) (Result, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Result{}, errors.New("catalog: begin publication failed")
	}
	defer func() { _ = tx.Rollback(ctx) }() //nolint:errcheck // no-op after commit

	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, catalogLockKey(artifact.Pack, artifact.LanguageCode)); err != nil {
		return Result{}, errors.New("catalog: publication lock failed")
	}

	var current currentRow
	err = tx.QueryRow(ctx, `
		SELECT current_version, current_content_sha256
		FROM language_catalog_packs
		WHERE lang_pack = $1 AND lang_code = $2
		FOR UPDATE
	`, artifact.Pack, artifact.LanguageCode).Scan(&current.Version, &current.ContentSHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		current = currentRow{}
	} else if err != nil {
		return Result{}, errors.New("catalog: read current publication failed")
	}
	result := Result{
		OldVersion:     current.Version,
		NewVersion:     current.Version,
		ContentSHA256:  contentHex,
		ManifestSHA256: manifestHex,
	}
	if current.Version > 0 && string(current.ContentSHA256) == string(contentSHA) {
		if err := tx.Commit(ctx); err != nil {
			return Result{}, errors.New("catalog: commit idempotent publication failed")
		}
		return result, nil
	}

	oldEntries, err := loadCurrentEntries(ctx, tx, artifact.Pack, artifact.LanguageCode)
	if err != nil {
		return Result{}, err
	}
	newVersion := current.Version + 1
	changes := diffEntries(oldEntries, artifact.Entries, newVersion)
	if _, err := tx.Exec(ctx, `
		INSERT INTO language_catalog_versions (
			lang_pack, lang_code, version, name, native_name, plural_code,
			content_sha256, manifest_sha256, source_url, source_revision,
			source_sha256, source_notice, attribution
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`, artifact.Pack, artifact.LanguageCode, newVersion, artifact.Name, artifact.NativeName, artifact.PluralCode,
		contentSHA, manifestSHA, artifact.Source.URL, artifact.Source.Revision, sourceSHA, artifact.Source.Notice, artifact.Attribution); err != nil {
		return Result{}, errors.New("catalog: write immutable version failed")
	}
	for _, change := range changes {
		if _, err := tx.Exec(ctx, `
			INSERT INTO language_catalog_changes (
				lang_pack, lang_code, version, key, kind, value,
				plural_zero, plural_one, plural_two, plural_few, plural_many,
				plural_other, deleted
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		`, artifact.Pack, artifact.LanguageCode, newVersion, change.Entry.Key, entryKind(change.Entry), entryValue(change.Entry),
			optionalValue(change.Entry.Plural, "zero"), optionalValue(change.Entry.Plural, "one"), optionalValue(change.Entry.Plural, "two"),
			optionalValue(change.Entry.Plural, "few"), optionalValue(change.Entry.Plural, "many"), optionalValue(change.Entry.Plural, "other"), change.Deleted); err != nil {
			return Result{}, errors.New("catalog: write catalog change failed")
		}
		if change.Deleted {
			if _, err := tx.Exec(ctx, `
				INSERT INTO language_catalog_tombstones (lang_pack, lang_code, version, key)
				VALUES ($1, $2, $3, $4)
			`, artifact.Pack, artifact.LanguageCode, newVersion, change.Entry.Key); err != nil {
				return Result{}, errors.New("catalog: write deletion tombstone failed")
			}
		}
	}
	for _, change := range changes {
		if change.Deleted {
			continue
		}
		entry := change.Entry
		if _, err := tx.Exec(ctx, `
			INSERT INTO language_catalog_current_strings (
				lang_pack, lang_code, key, kind, value, plural_zero, plural_one,
				plural_two, plural_few, plural_many, plural_other, deleted,
				last_changed_version
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, false, $12)
			ON CONFLICT (lang_pack, lang_code, key) DO UPDATE SET
				kind = EXCLUDED.kind,
				value = EXCLUDED.value,
				plural_zero = EXCLUDED.plural_zero,
				plural_one = EXCLUDED.plural_one,
				plural_two = EXCLUDED.plural_two,
				plural_few = EXCLUDED.plural_few,
				plural_many = EXCLUDED.plural_many,
				plural_other = EXCLUDED.plural_other,
				deleted = false,
				last_changed_version = EXCLUDED.last_changed_version
		`, artifact.Pack, artifact.LanguageCode, entry.Key, entryKind(entry), entryValue(entry),
			optionalValue(entry.Plural, "zero"), optionalValue(entry.Plural, "one"), optionalValue(entry.Plural, "two"),
			optionalValue(entry.Plural, "few"), optionalValue(entry.Plural, "many"), optionalValue(entry.Plural, "other"), newVersion); err != nil {
			return Result{}, errors.New("catalog: write current catalog failed")
		}
	}
	for _, change := range changes {
		if !change.Deleted {
			continue
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO language_catalog_current_strings (
				lang_pack, lang_code, key, kind, value, plural_zero, plural_one,
				plural_two, plural_few, plural_many, plural_other, deleted,
				last_changed_version
			) VALUES ($1, $2, $3, 0, '', NULL, NULL, NULL, NULL, NULL, NULL, true, $4)
			ON CONFLICT (lang_pack, lang_code, key) DO UPDATE SET
				kind = 0, value = '', plural_zero = NULL, plural_one = NULL,
				plural_two = NULL, plural_few = NULL, plural_many = NULL,
				plural_other = NULL, deleted = true,
				last_changed_version = EXCLUDED.last_changed_version
		`, artifact.Pack, artifact.LanguageCode, change.Entry.Key, newVersion); err != nil {
			return Result{}, errors.New("catalog: write current tombstone failed")
		}
	}
	if opts.BeforeActivate != nil {
		if err := opts.BeforeActivate(ctx); err != nil {
			return Result{}, err
		}
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO language_catalog_publication_audit (
			lang_pack, lang_code, old_version, new_version, source_commit_sha,
			source_revision, source_sha256, content_sha256, manifest_sha256,
			validator_result, os_user
		) VALUES ($1, $2, NULLIF($3, 0), $4, $5, $6, $7, $8, $9, 'valid', $10)
	`, artifact.Pack, artifact.LanguageCode, current.Version, newVersion, reviewedCommit, artifact.Source.Revision,
		sourceSHA, contentSHA, manifestSHA, opts.OSUser); err != nil {
		return Result{}, errors.New("catalog: write publication audit failed")
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO language_catalog_packs (
			lang_pack, lang_code, current_version, name, native_name, plural_code,
			current_content_sha256, current_manifest_sha256, source_url,
			source_revision, source_sha256, source_notice, attribution
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (lang_pack, lang_code) DO UPDATE SET
			current_version = EXCLUDED.current_version,
			name = EXCLUDED.name,
			native_name = EXCLUDED.native_name,
			plural_code = EXCLUDED.plural_code,
			current_content_sha256 = EXCLUDED.current_content_sha256,
			current_manifest_sha256 = EXCLUDED.current_manifest_sha256,
			source_url = EXCLUDED.source_url,
			source_revision = EXCLUDED.source_revision,
			source_sha256 = EXCLUDED.source_sha256,
			source_notice = EXCLUDED.source_notice,
			attribution = EXCLUDED.attribution,
			updated_at = now()
	`, artifact.Pack, artifact.LanguageCode, newVersion, artifact.Name, artifact.NativeName, artifact.PluralCode,
		contentSHA, manifestSHA, artifact.Source.URL, artifact.Source.Revision, sourceSHA, artifact.Source.Notice, artifact.Attribution); err != nil {
		return Result{}, errors.New("catalog: activate publication failed")
	}
	if _, err := tx.Exec(ctx, `SELECT pg_notify('language_catalog_published', $1)`, artifact.Pack+":"+artifact.LanguageCode); err != nil {
		return Result{}, errors.New("catalog: notify publication failed")
	}
	if err := tx.Commit(ctx); err != nil {
		return Result{}, errors.New("catalog: commit publication failed")
	}
	result.Changed = true
	result.NewVersion = newVersion
	return result, nil
}

func currentOSUser() string {
	current, err := user.Current()
	if err == nil && current.Username != "" {
		return current.Username
	}
	return "unknown"
}

func catalogLockKey(pack, languageCode string) int64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(pack + "\x00" + languageCode))
	const maxInt64 = uint64(1<<63 - 1)
	return int64(h.Sum64() & maxInt64)
}

func loadCurrentEntries(ctx context.Context, tx pgx.Tx, pack, languageCode string) (map[string]catalog.Entry, error) {
	rows, err := tx.Query(ctx, `
		SELECT key, kind, value, plural_zero, plural_one, plural_two,
		       plural_few, plural_many, plural_other, deleted
		FROM language_catalog_current_strings
		WHERE lang_pack = $1 AND lang_code = $2
	`, pack, languageCode)
	if err != nil {
		return nil, errors.New("catalog: read current strings failed")
	}
	defer rows.Close()
	entries := make(map[string]catalog.Entry)
	for rows.Next() {
		entry, deleted, err := scanEntry(rows)
		if err != nil {
			return nil, errors.New("catalog: decode current strings failed")
		}
		if !deleted {
			entries[entry.Key] = entry
		}
	}
	if err := rows.Err(); err != nil {
		return nil, errors.New("catalog: read current strings failed")
	}
	return entries, nil
}

type rowScanner interface {
	Scan(...any) error
}

func scanEntry(row rowScanner) (catalog.Entry, bool, error) {
	var (
		key, value, zero, one, two, few, many, other pgtype.Text
		kind                                         int16
		deletedBool                                  bool
	)
	if err := row.Scan(&key, &kind, &value, &zero, &one, &two, &few, &many, &other, &deletedBool); err != nil {
		return catalog.Entry{}, false, err
	}
	entry := catalog.Entry{Key: key.String, Value: value.String}
	if kind == 1 {
		entry.Value = ""
		entry.Plural = &catalog.Plural{Other: other.String}
		entry.Plural.Zero = optionalPointer(zero)
		entry.Plural.One = optionalPointer(one)
		entry.Plural.Two = optionalPointer(two)
		entry.Plural.Few = optionalPointer(few)
		entry.Plural.Many = optionalPointer(many)
	}
	return entry, deletedBool, nil
}

func optionalPointer(value pgtype.Text) *string {
	if !value.Valid {
		return nil
	}
	result := value.String
	return &result
}

func diffEntries(old map[string]catalog.Entry, current []catalog.Entry, version int64) []catalog.Change {
	next := make(map[string]catalog.Entry, len(current))
	for _, entry := range current {
		next[entry.Key] = entry
	}
	keys := make([]string, 0, len(old)+len(next))
	seen := make(map[string]struct{}, len(old)+len(next))
	for key := range old {
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	for key := range next {
		if _, ok := seen[key]; !ok {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	changes := make([]catalog.Change, 0, len(keys))
	for _, key := range keys {
		before, hadBefore := old[key]
		after, hasAfter := next[key]
		switch {
		case !hasAfter && hadBefore:
			changes = append(changes, catalog.Change{Version: version, Entry: catalog.Entry{Key: key}, Deleted: true})
		case hasAfter && (!hadBefore || !entriesEqual(before, after)):
			changes = append(changes, catalog.Change{Version: version, Entry: after})
		}
	}
	return changes
}

func entriesEqual(a, b catalog.Entry) bool {
	if a.Key != b.Key || a.Value != b.Value || a.Deleted != b.Deleted {
		return false
	}
	if a.Plural == nil || b.Plural == nil {
		return a.Plural == nil && b.Plural == nil
	}
	return stringPointerEqual(a.Plural.Zero, b.Plural.Zero) &&
		stringPointerEqual(a.Plural.One, b.Plural.One) &&
		stringPointerEqual(a.Plural.Two, b.Plural.Two) &&
		stringPointerEqual(a.Plural.Few, b.Plural.Few) &&
		stringPointerEqual(a.Plural.Many, b.Plural.Many) &&
		a.Plural.Other == b.Plural.Other
}

func stringPointerEqual(a, b *string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

func entryKind(entry catalog.Entry) int16 {
	if entry.Plural != nil {
		return 1
	}
	return 0
}

func entryValue(entry catalog.Entry) string {
	if entry.Plural != nil {
		return ""
	}
	return entry.Value
}

func optionalValue(plural *catalog.Plural, category string) any {
	if plural == nil {
		return nil
	}
	var value *string
	switch category {
	case "zero":
		value = plural.Zero
	case "one":
		value = plural.One
	case "two":
		value = plural.Two
	case "few":
		value = plural.Few
	case "many":
		value = plural.Many
	case "other":
		value = &plural.Other
	}
	if value == nil {
		return nil
	}
	return *value
}
