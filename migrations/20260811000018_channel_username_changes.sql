-- M8: channel_username_changes — rolling-window ledger for the per-channel
-- username change rate limit. Mirrors username_changes (per-account) but keyed
-- on channel_id instead of user_id, so the per-channel cap (2 changes per 24h)
-- is enforced independently of who edits.

CREATE TABLE channel_username_changes (
    channel_id BIGINT      NOT NULL REFERENCES channels (id),
    changed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (channel_id, changed_at)
);

-- Index for the rolling-window count.
CREATE INDEX channel_username_changes_channel_time ON channel_username_changes (channel_id, changed_at);
