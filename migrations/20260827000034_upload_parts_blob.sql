-- M18: in-flight upload part bytes leave Postgres.

-- size is the part's byte length, measured server-side from the received
-- payload. The outstanding-byte caps and the assembly byte check now aggregate
-- over this column instead of length(payload).
ALTER TABLE upload_parts ADD COLUMN size BIGINT NOT NULL DEFAULT 0;

-- blob_key locates the part's bytes in the blob store. It is drawn from
-- crypto/rand at save time and recorded here, so it is not derivable from the
-- client-chosen (file_id, part_index) pair. The prefix is fixed by the server
-- (blob.PartsPrefix) and disjoint from the assembled-blob keyspace.
ALTER TABLE upload_parts ADD COLUMN blob_key TEXT NOT NULL DEFAULT '';

-- The payload column is gone: no table stores the part bytes.
ALTER TABLE upload_parts DROP COLUMN payload;
