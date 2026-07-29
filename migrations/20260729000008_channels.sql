-- M7 channels: broadcast peers with one message row per channel rather than one
-- per member, plus the per-channel pts stream that backs their difference.
--
-- Unlike M6 chats, a channel post writes a single row and every member reads it,
-- so the update sequence cannot live on update_state (which is per account).
-- channel_state / channel_events mirror update_state / message_events exactly,
-- keyed by channel instead of by user.
--
-- peer_type gains 3 = channel alongside 1 = user and 2 = chat (see
-- 20260725000006_group_chats.sql). No column changes here: a channel dialog is a
-- dialogs row with peer_type = 3 and peer_id = channels.id.

CREATE TABLE channels (
    id          BIGSERIAL PRIMARY KEY,
    title       TEXT   NOT NULL,
    about       TEXT   NOT NULL DEFAULT '',
    creator_id  BIGINT NOT NULL REFERENCES users (id),
    megagroup   BOOL   NOT NULL DEFAULT false,
    version     INT    NOT NULL DEFAULT 1,
    date        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Per-channel update sequence and message-id allocator. Mirrors update_state,
-- keyed by channel instead of by user.
CREATE TABLE channel_state (
    channel_id    BIGINT PRIMARY KEY REFERENCES channels (id),
    pts           BIGINT NOT NULL DEFAULT 0,
    next_local_id BIGINT NOT NULL DEFAULT 1,
    date          TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Bounds, enforced in Go by later tickets. Recorded here so the number is a
-- decision rather than an incident:
--
--   * at most 10 000 participants per channel;
--   * at most 500 channels joined per account;
--   * one post emits exactly ONE pg_notify carrying the channel id, never one
--     per member. A per-member fan-out of 100k NOTIFYs lands on the single
--     Listener goroutine whose callbacks may not block
--     (internal/store/notify.go:79) and stalls live delivery for every
--     unrelated account on the replica.
CREATE TABLE channel_participants (
    channel_id   BIGINT NOT NULL REFERENCES channels (id),
    user_id      BIGINT NOT NULL REFERENCES users (id),
    role         SMALLINT NOT NULL DEFAULT 0,   -- 0 member, 1 admin, 2 creator
    banned_until TIMESTAMPTZ NULL,              -- NULL = not banned
    join_pts     BIGINT NOT NULL DEFAULT 0,     -- channel pts at join; floors this member's difference
    date         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, user_id)
);

-- "which channels is this user in" drives the dialog list and the push fan-out.
CREATE INDEX channel_participants_user_idx ON channel_participants (user_id);

-- Admission is by invite hash ONLY. There is no join-by-channel-id path in M7:
-- channels.id is dense BIGSERIAL and the peer access_hash placeholder is
-- access_hash == id, so a join keyed on the id would let any account enumerate
-- and join every channel on the server, which makes the participant row worthless
-- as an authorization boundary. The hash is the secret; the channel id is never
-- an input to the join path.
--
-- hash is 128 bits from crypto/rand, base64url-encoded without padding (22 chars).
-- Bearer-grade for M7: no expiry, no usage limit, no per-invite revocation beyond
-- deleting the row.
CREATE TABLE channel_invites (
    hash       TEXT PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES channels (id),
    creator_id BIGINT NOT NULL REFERENCES users (id),
    date       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX channel_invites_channel_idx ON channel_invites (channel_id);

CREATE TABLE channel_messages (
    channel_id BIGINT NOT NULL REFERENCES channels (id),
    local_id   BIGINT NOT NULL,              -- per-channel message id
    from_id    BIGINT NOT NULL,              -- author user id
    date       TIMESTAMPTZ NOT NULL DEFAULT now(),
    message    TEXT   NOT NULL,
    edit_date  TIMESTAMPTZ NULL,
    deleted    BOOL   NOT NULL DEFAULT false,
    random_id  BIGINT NOT NULL DEFAULT 0,
    file_id    BIGINT NULL REFERENCES files (id),
    PRIMARY KEY (channel_id, local_id)
);

-- Send dedup, same rule as messages_random_uniq in the M4 migration.
CREATE UNIQUE INDEX channel_messages_random_uniq
    ON channel_messages (channel_id, random_id) WHERE random_id <> 0;

-- The M5 download gate (FileForDownload) entitles a caller by a messages row they
-- own. Channel posts write no per-member row, so the channel branch of that gate
-- looks a file up through this table instead — file_id must be indexed or that
-- branch is a sequential scan on every download.
CREATE INDEX channel_messages_file_idx ON channel_messages (file_id) WHERE file_id IS NOT NULL;

-- Per-channel ordered event log. Same contract as message_events: a row present
-- implies channel_state.pts for that channel is >= that event's pts.
CREATE TABLE channel_events (
    channel_id BIGINT NOT NULL REFERENCES channels (id),
    pts        BIGINT NOT NULL,   -- the pts AFTER this event
    type       SMALLINT NOT NULL, -- 1 new, 2 edit, 3 delete
    local_id   BIGINT NOT NULL,
    PRIMARY KEY (channel_id, pts)
);
