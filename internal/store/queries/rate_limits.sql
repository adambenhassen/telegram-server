-- name: CheckAndConsumeRateLimit :one
-- Atomic check-and-consume on a rate-limit counter.
--
-- INSERT attempts to seed a new counter; ON CONFLICT fires the DO UPDATE:
--   1. If the window has expired, reset to count=1, window_start=now.
--   2. If the window is active and count < limit, bump count.
--   3. If the window is active and count >= limit, the WHERE clause prevents
--      the UPDATE — the existing (unchanged) row is returned instead.
--
-- The RETURNING clause always produces one row. The caller checks
-- consumed=true to distinguish "allowed" from "denied".
--
-- Exactness under concurrency comes from the row-level lock taken by
-- INSERT ... ON CONFLICT — no advisory lock needed.
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
WHERE now() - rate_limits.window_start >= $3::INTERVAL
   OR rate_limits.token_count < $4
RETURNING token_count, window_start, consumed;

-- name: SweepExpiredRateLimits :exec
-- Delete rows whose window has fully expired (window_start + window < now).
-- Prevents unbounded growth of stale subject entries.
DELETE FROM rate_limits
WHERE window_start + $1::INTERVAL < now();
