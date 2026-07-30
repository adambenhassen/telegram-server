-- M8: peer lookup — phone number resolution with per-account quota.

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
