-- M22: reviewed language-catalog publication state.
--
-- Catalog rows are additive and publication history is immutable. The current
-- tables are pointers/materialized state; no migration or publication path
-- deletes version, change, or tombstone history.
CREATE TABLE language_catalog_packs (
    lang_pack                 TEXT NOT NULL,
    lang_code                 TEXT NOT NULL,
    current_version           BIGINT NOT NULL CHECK (current_version > 0),
    name                      TEXT NOT NULL,
    native_name               TEXT NOT NULL,
    plural_code               TEXT NOT NULL,
    current_content_sha256    BYTEA NOT NULL CHECK (octet_length(current_content_sha256) = 32),
    current_manifest_sha256   BYTEA NOT NULL CHECK (octet_length(current_manifest_sha256) = 32),
    source_url                TEXT NOT NULL,
    source_revision           TEXT NOT NULL,
    source_sha256             BYTEA NOT NULL CHECK (octet_length(source_sha256) = 32),
    source_notice             TEXT NOT NULL,
    attribution               TEXT NOT NULL,
    updated_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (lang_pack, lang_code)
);

CREATE TABLE language_catalog_versions (
    lang_pack              TEXT NOT NULL,
    lang_code              TEXT NOT NULL,
    version                BIGINT NOT NULL CHECK (version > 0),
    name                   TEXT NOT NULL,
    native_name            TEXT NOT NULL,
    plural_code            TEXT NOT NULL,
    content_sha256         BYTEA NOT NULL CHECK (octet_length(content_sha256) = 32),
    manifest_sha256        BYTEA NOT NULL CHECK (octet_length(manifest_sha256) = 32),
    source_url             TEXT NOT NULL,
    source_revision        TEXT NOT NULL,
    source_sha256          BYTEA NOT NULL CHECK (octet_length(source_sha256) = 32),
    source_notice          TEXT NOT NULL,
    attribution            TEXT NOT NULL,
    published_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (lang_pack, lang_code, version)
);

CREATE TABLE language_catalog_current_strings (
    lang_pack             TEXT NOT NULL,
    lang_code             TEXT NOT NULL,
    key                   TEXT NOT NULL,
    kind                  SMALLINT NOT NULL CHECK (kind IN (0, 1)),
    value                 TEXT NOT NULL,
    plural_zero           TEXT,
    plural_one            TEXT,
    plural_two            TEXT,
    plural_few            TEXT,
    plural_many           TEXT,
    plural_other          TEXT,
    deleted               BOOLEAN NOT NULL,
    last_changed_version  BIGINT NOT NULL CHECK (last_changed_version > 0),
    PRIMARY KEY (lang_pack, lang_code, key),
    CHECK (
        deleted
        OR (kind = 0 AND plural_zero IS NULL AND plural_one IS NULL
            AND plural_two IS NULL AND plural_few IS NULL AND plural_many IS NULL
            AND plural_other IS NULL)
        OR (kind = 1 AND plural_other IS NOT NULL)
    )
);

CREATE TABLE language_catalog_changes (
    lang_pack             TEXT NOT NULL,
    lang_code             TEXT NOT NULL,
    version               BIGINT NOT NULL CHECK (version > 0),
    key                   TEXT NOT NULL,
    kind                  SMALLINT NOT NULL CHECK (kind IN (0, 1)),
    value                 TEXT NOT NULL,
    plural_zero           TEXT,
    plural_one            TEXT,
    plural_two            TEXT,
    plural_few            TEXT,
    plural_many            TEXT,
    plural_other          TEXT,
    deleted               BOOLEAN NOT NULL,
    PRIMARY KEY (lang_pack, lang_code, version, key),
    CHECK (
        deleted
        OR (kind = 0 AND plural_zero IS NULL AND plural_one IS NULL
            AND plural_two IS NULL AND plural_few IS NULL AND plural_many IS NULL
            AND plural_other IS NULL)
        OR (kind = 1 AND plural_other IS NOT NULL)
    )
);

CREATE TABLE language_catalog_tombstones (
    lang_pack  TEXT NOT NULL,
    lang_code  TEXT NOT NULL,
    version    BIGINT NOT NULL CHECK (version > 0),
    key        TEXT NOT NULL,
    PRIMARY KEY (lang_pack, lang_code, version, key)
);

CREATE TABLE language_catalog_publication_audit (
    id                 BIGSERIAL PRIMARY KEY,
    lang_pack          TEXT NOT NULL,
    lang_code          TEXT NOT NULL,
    old_version        BIGINT CHECK (old_version IS NULL OR old_version >= 0),
    new_version        BIGINT NOT NULL CHECK (new_version > 0),
    source_commit_sha  TEXT NOT NULL,
    source_revision    TEXT NOT NULL,
    source_sha256      BYTEA NOT NULL CHECK (octet_length(source_sha256) = 32),
    content_sha256     BYTEA NOT NULL CHECK (octet_length(content_sha256) = 32),
    manifest_sha256    BYTEA NOT NULL CHECK (octet_length(manifest_sha256) = 32),
    validator_result   TEXT NOT NULL CHECK (validator_result = 'valid'),
    os_user            TEXT NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX language_catalog_changes_key_idx
    ON language_catalog_changes (lang_pack, lang_code, key, version DESC);

CREATE INDEX language_catalog_tombstones_key_idx
    ON language_catalog_tombstones (lang_pack, lang_code, key, version DESC);
