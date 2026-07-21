-- M4 messaging core: two-sided message storage plus the per-owner pts/event log
-- that backs updates.getDifference and real-time push.
--
-- Two-sided model: each account owns its own message-id space (local_id) and its
-- own pts. A single sendMessage writes two rows (sender outbox + recipient inbox),
-- each with its own local_id and its own pts++.

-- Per-owner update sequence + message-id allocator.
CREATE TABLE update_state (
    user_id       BIGINT PRIMARY KEY REFERENCES users (id),
    pts           BIGINT NOT NULL DEFAULT 0,
    seq           BIGINT NOT NULL DEFAULT 0,
    date          TIMESTAMPTZ NOT NULL DEFAULT now(),
    next_local_id BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE messages (
    owner_id      BIGINT NOT NULL REFERENCES users (id),
    local_id      BIGINT NOT NULL,              -- per-owner message id
    peer_id       BIGINT NOT NULL,              -- the other user in the 1:1
    from_id       BIGINT NOT NULL,              -- author (owner for out, peer for in)
    date          TIMESTAMPTZ NOT NULL DEFAULT now(),
    message       TEXT   NOT NULL,
    out           BOOL   NOT NULL,              -- owner is the author
    edit_date     TIMESTAMPTZ NULL,
    deleted       BOOL   NOT NULL DEFAULT false,
    random_id     BIGINT NOT NULL DEFAULT 0,    -- sender-supplied send-dedup token
    peer_local_id BIGINT NOT NULL DEFAULT 0,    -- mirror row's local_id on the other side
    PRIMARY KEY (owner_id, local_id)
);

-- Send dedup: at most one non-zero random_id per owner (a client resend returns
-- the original message instead of inserting a duplicate).
CREATE UNIQUE INDEX messages_random_uniq
    ON messages (owner_id, random_id) WHERE random_id <> 0;

-- History paging by (owner, peer) newest-first.
CREATE INDEX messages_owner_peer_idx ON messages (owner_id, peer_id, local_id);

CREATE TABLE dialogs (
    owner_id           BIGINT NOT NULL REFERENCES users (id),
    peer_id            BIGINT NOT NULL,
    top_message        BIGINT NOT NULL,             -- owner's local_id of newest message
    unread_count       INT    NOT NULL DEFAULT 0,
    read_inbox_max_id  BIGINT NOT NULL DEFAULT 0,   -- newest inbound owner has read
    read_outbox_max_id BIGINT NOT NULL DEFAULT 0,   -- newest outbound peer has read
    PRIMARY KEY (owner_id, peer_id)
);

-- Per-owner ordered event log the difference computation reads. A row present
-- implies update_state.pts for that owner is >= that event's pts.
CREATE TABLE message_events (
    owner_id BIGINT NOT NULL REFERENCES users (id),
    pts      BIGINT NOT NULL,     -- the pts AFTER this event
    type     SMALLINT NOT NULL,   -- 1 new, 2 edit, 3 delete, 4 read_in, 5 read_out
    local_id BIGINT NOT NULL,     -- affected message (or read max_id)
    PRIMARY KEY (owner_id, pts)
);
