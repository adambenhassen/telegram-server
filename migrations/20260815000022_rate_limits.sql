-- M14 rate limiter: O(1) sliding-window counter per (subject, surface).
--
-- Each row holds the token count and the start of the current window. A
-- check-and-consume atomically resets the window (when expired) and bumps
-- the count, or rejects when the count is already at the limit.
--
-- subject_id is int64 to hold account ids today and admit other key shapes
-- later (e.g. a hashed ip address stored as a bigint).
--
-- The consumed column distinguishes an allowed request (true) from a denied
-- one (false) in the RETURNING clause of the check-and-consume query.
--
-- Expired rows (window_start + window < now) are swept periodically to
-- prevent unbounded growth — see SweepExpiredRateLimits.

CREATE TABLE rate_limits (
    subject_id   BIGINT       NOT NULL,
    surface      TEXT         NOT NULL,
    token_count  INT          NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ  NOT NULL DEFAULT now(),
    consumed     BOOLEAN      NOT NULL DEFAULT false,
    PRIMARY KEY (subject_id, surface)
);

-- Index for the sweep query that deletes expired rows.
CREATE INDEX rate_limits_window_start_idx ON rate_limits (window_start);
