-- name: UpsertRateLimit :execrows
-- Atomic check-and-consume on a rate-limit counter.
--
-- Given a window duration, this query:
--   1. If the current window has expired (now - window_start >= window),
--      resets to count=1 and window_start=now (first token consumed).
--   2. If the current window is still active and count < limit, bumps count.
--   3. If the current window is still active and count >= limit, leaves the
--      row unchanged (denied request consumes nothing).
--
-- The WHERE clause on step 2 & 3 ensures denied requests mutate nothing.
-- The advisory lock (taken in Go) serialises concurrent callers for the same
-- (subject, surface) so the limit is exact.
UPDATE rate_limits
SET
    token_count = CASE
        -- Window expired: reset and consume first token.
        WHEN now() - window_start >= $3::INTERVAL THEN 1
        -- Window active, under limit: consume a token.
        WHEN token_count < $4 THEN token_count + 1
        -- Window active, at limit: denied — consume nothing.
        ELSE token_count
    END,
    window_start = CASE
        WHEN now() - window_start >= $3::INTERVAL THEN now()
        ELSE window_start
    END
WHERE subject_id = $1
  AND surface = $2;

-- name: GetRateLimit :one
-- Read the current state of a rate-limit counter.
SELECT token_count, window_start
FROM rate_limits
WHERE subject_id = $1
  AND surface = $2;

-- name: InsertRateLimit :exec
-- Seed a new rate-limit counter for a (subject, surface) that has no row yet.
-- Used after UpsertRateLimit reports that no row existed (0 rows affected).
INSERT INTO rate_limits (subject_id, surface, token_count, window_start)
VALUES ($1, $2, 1, now());
