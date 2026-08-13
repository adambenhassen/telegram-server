-- phone_codes: isolate attempt counters per code row (MAIN-280).
--
-- Before: phone was the PRIMARY KEY, so every caller sharing a phone number
--         shared the same code_hash, attempts, and consumed_at.
-- After:  code_hash is the PRIMARY KEY. Each IssueCode call inserts a new row.
--         The phone column is indexed for cooldown lookups.
--
-- Rollback: this migration drops the old phone_codes table and replaces it.
--   Existing active codes are lost. A client holding a valid code_hash from
--   before the migration will fail to verify (ErrCodeInvalid). This is
--   acceptable because:
--     - Code TTL is 5 minutes; the window is short.
--     - The old schema is insecure (cross-session exhaustion).
--     - No data is silently corrupted — the table is dropped and recreated.
--   To roll back, restore from a pre-migration backup or revert to the
--   previous migration and re-apply from there:
--     atlas migrate revert --env local

-- Drop the old table and recreate with code_hash as PK.
DROP TABLE IF EXISTS phone_codes;

CREATE TABLE phone_codes (
    id          BIGSERIAL PRIMARY KEY,
    phone       TEXT    NOT NULL,
    code_hash   TEXT    NOT NULL UNIQUE,
    code        TEXT    NOT NULL,
    expires_at  TIMESTAMPTZ NOT NULL,
    attempts    INT     NOT NULL DEFAULT 0,
    consumed_at TIMESTAMPTZ NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index for cooldown lookups: find the latest active code for a phone.
CREATE INDEX phone_codes_phone_created_at ON phone_codes (phone, created_at DESC);
