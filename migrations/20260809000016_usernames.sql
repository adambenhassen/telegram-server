-- M8 usernames: shared namespace table for handle ownership across users and
-- channels, plus denormalized display columns on each entity table.
--
-- The usernames table is the single source of truth: its PRIMARY KEY on handle
-- enforces uniqueness across both entity types in one constraint. A user and a
-- channel cannot claim the same handle, and the check cannot be split across
-- two per-table indexes without racing concurrent claims.
--
-- The username columns on users and channels are denormalized display copies
-- kept in sync by the application in the same transaction as the claim/release.
-- They carry no uniqueness constraint of their own.

ALTER TABLE users ADD COLUMN username TEXT NULL;

ALTER TABLE channels ADD COLUMN username TEXT NULL;

CREATE TABLE usernames (
    handle      TEXT    PRIMARY KEY,
    owner_type  TEXT    NOT NULL CHECK (owner_type IN ('user', 'channel')),
    owner_id    BIGINT  NOT NULL
);
