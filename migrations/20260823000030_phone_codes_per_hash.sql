-- phone_codes: isolate attempt counters per code row (MAIN-280).
--
-- Before: phone was the PRIMARY KEY, so every caller sharing a phone number
--         shared the same code_hash, attempts, and consumed_at.
-- After:  code_hash is UNIQUE (each IssueCode inserts a new row). An id column
--         provides a stable surrogate key. The phone column is indexed for
--         cooldown lookups.
--
-- Forward-only: uses ALTER TABLE so in-flight codes survive the deploy.
-- Rollback: atlas migrate revert drops the index, removes the code_hash
--   uniqueness constraint, and removes the id column. The table reverts to
--   phone-as-PK with no data loss. The Go code on the pre-migration branch
--   still upserts on phone and ignores id, so the revert is safe.

-- Add a surrogate key for future use (sqlc models need a stable PK).
ALTER TABLE phone_codes ADD COLUMN id BIGSERIAL;

-- Replace the phone primary key with code_hash uniqueness.
ALTER TABLE phone_codes DROP CONSTRAINT phone_codes_pkey;
ALTER TABLE phone_codes ADD CONSTRAINT phone_codes_code_hash_key UNIQUE (code_hash);

-- Index for cooldown lookups: find the latest active code for a phone.
CREATE INDEX phone_codes_phone_created_at ON phone_codes (phone, created_at DESC);
