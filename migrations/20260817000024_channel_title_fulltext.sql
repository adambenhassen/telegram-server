-- M15: tsvector column + GIN index for channel title search, plus the reverse
-- index on the usernames table that the discovery query needs.
--
-- Same mechanism as the M13 message/user columns and the M14 channel_messages
-- column: stored generated column with the 'simple' dictionary, index-backed,
-- additive only. The ADD COLUMN rewrites the table, which is bounded by the
-- channels row count (one row per channel, not per post or per member).

ALTER TABLE channels
    ADD COLUMN title_tsv TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('simple', title)
    ) STORED;

CREATE INDEX channels_title_tsv_idx ON channels USING GIN (title_tsv);

-- usernames is keyed on handle, which serves resolveUsername. Channel discovery
-- walks it the other way — "does this channel own a handle" — once per candidate
-- row inside the search query. Without this index that predicate is a sequential
-- scan over the whole handle namespace on every search, which an unauthenticated
-- title is enough to pace.
CREATE INDEX usernames_owner_idx ON usernames (owner_type, owner_id);
