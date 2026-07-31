-- M9 user online status: is_online and last_seen_at columns on users.
-- Defaults are safe for existing rows (false, NULL), no backfill needed.

ALTER TABLE users
    ADD COLUMN is_online    BOOL NOT NULL DEFAULT false,
    ADD COLUMN last_seen_at TIMESTAMPTZ NULL;
