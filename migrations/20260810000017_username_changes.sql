-- M8: username_changes — rolling-window ledger for the per-account username
-- change rate limit. Same shape as phone_lookups: prune expired rows, count
-- distinct values, reject past the limit.

CREATE TABLE username_changes (
    user_id      BIGINT    NOT NULL REFERENCES users (id),
    changed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, changed_at)
);

-- Index for the rolling-window count.
CREATE INDEX username_changes_user_time ON username_changes (user_id, changed_at);
