-- contacts.search: add name_tsv tsvector column to users for full-text name search.
-- The column is populated by a trigger so inserts/updates stay consistent without
-- application-side bookkeeping.

ALTER TABLE users ADD COLUMN name_tsv TSVECTOR GENERATED ALWAYS AS (
    to_tsvector('simple', COALESCE(first_name, '') || ' ' || COALESCE(last_name, ''))
) STORED;
