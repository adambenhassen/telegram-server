-- M14 rate limiter: O(1) sliding-window counter per (subject, surface).
--
-- Each row holds the token count, the start of the current window, and the
-- expiry deadline (window_start + window). A check-and-consume atomically
-- resets the window (when expired) and bumps the count, or rejects when the
-- count is already at the limit.
--
-- subject_id is int64 to hold account ids today and admit other key shapes
-- later (e.g. a hashed ip address stored as a bigint).
--
-- expires_at enables the sweep query to delete fully-expired rows without
-- knowing per-surface window durations.

CREATE TABLE rate_limits (
    subject_id   BIGINT       NOT NULL,
    surface      TEXT         NOT NULL,
    token_count  INT          NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ  NOT NULL DEFAULT (now() + INTERVAL '1 hour'),
    PRIMARY KEY (subject_id, surface)
);

-- Index for the sweep query that deletes expired rows.
CREATE INDEX rate_limits_expires_at_idx ON rate_limits (expires_at);
