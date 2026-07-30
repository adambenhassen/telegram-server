-- M8: peer lookup — phone number resolution with per-account quota.

-- Normalize existing phone numbers: strip leading '+' so that '+1555...' and
-- '1555...' are the same value. This backfill is required because the write
-- path (signIn) and read path (lookup) both now use the normalized form.
-- Without it, a row stored as '+1555...' would not be found by a lookup for
-- '1555...' (or vice versa).
UPDATE users
    SET phone = SUBSTRING(phone FROM 2)
    WHERE phone LIKE '+%';

-- Also normalize phone codes, which key on the same phone column.
UPDATE phone_codes
    SET phone = SUBSTRING(phone FROM 2)
    WHERE phone LIKE '+%';

-- phone_lookups tracks per-caller, per-phone lookup history for quota enforcement.
-- Rolling window is enforced by deleting expired rows before counting.
-- COUNT DISTINCT phone enforces the quota; one row per call is expected.
CREATE TABLE phone_lookups (
    caller_id   BIGINT    NOT NULL REFERENCES users (id),
    phone       TEXT      NOT NULL,
    looked_up_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (caller_id, phone, looked_up_at)
);

-- Index for the rolling-window count: caller + time range.
CREATE INDEX phone_lookups_caller_time ON phone_lookups (caller_id, looked_up_at);
