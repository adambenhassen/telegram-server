-- M14 schema: tsvector column + GIN index for full-text search on channel posts.
--
-- Same mechanism as the M13 message/user columns: stored generated column with
-- the 'simple' dictionary, index-backed, additive only.

ALTER TABLE channel_messages
    ADD COLUMN message_tsv TSVECTOR GENERATED ALWAYS AS (
        to_tsvector('simple', message)
    ) STORED;

CREATE INDEX channel_messages_message_tsv_idx
    ON channel_messages USING GIN (message_tsv);
