-- Post timestamps must outlive membership rows so leaving and rejoining cannot
-- reset a member's slow-mode interval.
CREATE TABLE channel_post_markers (
    channel_id   BIGINT NOT NULL REFERENCES channels (id),
    user_id      BIGINT NOT NULL REFERENCES users (id),
    last_post_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (channel_id, user_id)
);

CREATE INDEX channel_post_markers_user_idx ON channel_post_markers (user_id);
