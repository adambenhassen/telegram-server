-- M13: tsvector columns + GIN indexes for full-text search on messages and users.
--
-- Stored generated columns are computed by Postgres on insert/update and
-- backfilled for existing rows when the column is added. The 'simple'
-- dictionary provides case-insensitive matching without stemming.

-- Full-text search on message body.
ALTER TABLE messages
    ADD COLUMN message_tsv TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('simple', message)
    ) STORED;

CREATE INDEX messages_message_tsv_idx ON messages USING GIN (message_tsv);

-- Full-text search on user display name (first_name + last_name).
ALTER TABLE users
    ADD COLUMN name_tsv TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('simple', first_name || ' ' || COALESCE(last_name, ''))
    ) STORED;

CREATE INDEX users_name_tsv_idx ON users USING GIN (name_tsv);
