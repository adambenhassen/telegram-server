-- Index on blob_key for the orphan pass's per-page live-key lookup.
-- The pass gates each enumeration page with WHERE blob_key = ANY($1);
-- without this index the lookup is a sequential scan of the table per page.
CREATE INDEX upload_parts_blob_key_idx ON upload_parts (blob_key);
