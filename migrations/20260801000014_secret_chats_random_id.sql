-- MAIN-142: add random_id to secret_chats for requestEncryption dedup.
--
-- random_id is the client-supplied dedup token. A second requestEncryption
-- with the same (admin_id, random_id) returns the existing row instead of
-- creating a second one. Partial index excludes random_id = 0 (no dedup).

ALTER TABLE secret_chats ADD COLUMN random_id BIGINT NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX secret_chats_admin_random_id_idx ON secret_chats (admin_id, random_id) WHERE random_id != 0 AND state = 'requested';
