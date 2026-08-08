-- name: CheckAndConsumeRateLimit :one
-- Atomic check-and-consume on a rate-limit counter.
--
-- INSERT attempts to seed a new counter (always succeeds with consumed=true).
-- ON CONFLICT fires the DO UPDATE:
--   1. If the window has expired, reset to count=1, window_start=now, consumed=true.
--   2. If the window is active and count < limit, bump count, consumed=true.
--   3. If the window is active and count >= limit, leave count unchanged, consumed=false.
--
-- The RETURNING clause always produces one row. consumed=true means allowed;
-- consumed=false means denied (and the request consumed nothing).
--
-- Exactness under concurrency comes from the row-level lock taken by
-- INSERT ... ON CONFLICT — different subjects never block each other.
INSERT INTO rate_limits (subject_id, surface, token_count, window_start, consumed)
VALUES ($1, $2, 1, now(), true)
ON CONFLICT (subject_id, surface) DO UPDATE SET
    token_count = CASE
        WHEN now() - rate_limits.window_start >= $3::INTERVAL THEN 1
        WHEN rate_limits.token_count < $4 THEN rate_limits.token_count + 1
        ELSE rate_limits.token_count
    END,
    window_start = CASE
        WHEN now() - rate_limits.window_start >= $3::INTERVAL THEN now()
        ELSE rate_limits.window_start
    END,
    consumed = CASE
        WHEN now() - rate_limits.window_start >= $3::INTERVAL THEN true
        WHEN rate_limits.token_count < $4 THEN true
        ELSE false
    END
RETURNING token_count, window_start, consumed;

-- name: SweepExpiredRateLimits :exec
-- Delete rows whose window has fully expired (window_start + window < now).
-- Prevents unbounded growth of stale subject entries.
DELETE FROM rate_limits
WHERE window_start + $1::INTERVAL < now();
