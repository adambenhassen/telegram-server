-- M14 rate limiter: O(1) sliding-window counter per (subject, surface).
--
-- Each row holds the token count and the start of the current window. A
-- check-and-consume atomically resets the window (when expired) and bumps
-- the count, or rejects when the count is already at the limit.
--
-- subject_id is int64 to hold account ids today and admit other key shapes
-- later (e.g. a hashed ip address stored as a bigint).

CREATE TABLE rate_limits (
    subject_id   BIGINT    NOT NULL,
    surface      TEXT      NOT NULL,
    token_count  INT       NOT NULL DEFAULT 0,
    window_start TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (subject_id, surface)
);
