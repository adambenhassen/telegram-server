-- Per-member channel history acknowledgements live as long as membership.
-- The zero default leaves existing memberships unread through their history;
-- admission code initializes new memberships at the channel's committed top.
-- A separate table keeps older binaries' channel_participants projections valid.
CREATE TABLE channel_read_state (
    channel_id  BIGINT NOT NULL,
    user_id     BIGINT NOT NULL,
    read_max_id BIGINT NOT NULL DEFAULT 0 CHECK (read_max_id >= 0),
    PRIMARY KEY (channel_id, user_id),
    FOREIGN KEY (channel_id, user_id)
        REFERENCES channel_participants (channel_id, user_id)
        ON DELETE CASCADE
);
