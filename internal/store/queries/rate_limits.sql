-- name: TryConsumeRateLimit :one
-- Attempt to consume a rate-limit token.
--
-- INSERT seeds a new counter; ON CONFLICT fires the DO UPDATE:
--   - Window closed: reset count=1, window_start=now, expires_at=now+window.
--   - Window open, under limit: bump count.
--   - Window open, at limit: the WHERE clause prevents the UPDATE entirely.
--
-- Whether the window is closed is read from expires_at, the deadline stored
-- when the window opened, and never recomputed as window_start + the current
-- config. That is the one boundary the sweep can also see, and the two must
-- agree: derive it from the live config instead and changing the config splits
-- them, so the sweep collects a row the limiter is still counting against and
-- the subject silently gets a fresh budget. The consequence is deliberate — a
-- config change takes effect at the next window rather than mid-flight, in
-- either direction, and never grants a subject more than the window it opened
-- with.
--
-- Returns one row (from RETURNING) when the request is allowed.
-- Returns pgx.ErrNoRows when the request is denied (UPDATE prevented by WHERE).
--
-- Exactness under concurrency comes from the row-level lock taken by
-- INSERT ... ON CONFLICT — different subjects never block each other.
INSERT INTO rate_limits (subject_id, surface, token_count, window_start, expires_at)
VALUES ($1, $2, 1, now(), now() + $3::INTERVAL)
ON CONFLICT (subject_id, surface) DO UPDATE SET
    token_count = CASE
        WHEN rate_limits.expires_at <= now() THEN 1
        ELSE rate_limits.token_count + 1
    END,
    window_start = CASE
        WHEN rate_limits.expires_at <= now() THEN now()
        ELSE rate_limits.window_start
    END,
    expires_at = CASE
        WHEN rate_limits.expires_at <= now() THEN now() + $3::INTERVAL
        ELSE rate_limits.expires_at
    END
WHERE rate_limits.expires_at <= now()
   OR rate_limits.token_count < $4
RETURNING token_count, window_start, expires_at;

-- name: GetRateLimitExpiresAt :one
-- Read the expiry deadline for a denied rate-limit check.
-- Called after TryConsumeRateLimit returns ErrNoRows to compute the wait time.
-- Uses a fresh READ COMMITTED snapshot so it sees the row committed by the
-- concurrent transaction that won the race.
SELECT expires_at
FROM rate_limits
WHERE subject_id = $1
  AND surface = $2;

-- name: SweepExpiredRateLimits :execrows
-- Delete rows whose per-row expiry deadline has passed. The deadline is stored
-- on the row (expires_at = window_start + window), so the sweep does not need
-- to know per-surface window durations. The return value reports rows affected.
DELETE FROM rate_limits
WHERE expires_at < now();

-- name: CheckRateLimitBudget :one
-- Read-only check of the current budget for a subject/surface pair.
-- Returns the current token count and expiry deadline so the caller can decide
-- whether to proceed without consuming a token.
-- Returns pgx.ErrNoRows when no row exists (budget not yet exhausted).
SELECT token_count, expires_at
FROM rate_limits
WHERE subject_id = $1
  AND surface = $2;

-- name: ChargeRateLimit :exec
-- Charge a rate-limit counter after a failed attempt. Uses the same
-- INSERT ... ON CONFLICT pattern as TryConsumeRateLimit but is called only
-- after a failure, so it always increments (or seeds at 1).
INSERT INTO rate_limits (subject_id, surface, token_count, window_start, expires_at)
VALUES ($1, $2, 1, now(), now() + $3::INTERVAL)
ON CONFLICT (subject_id, surface) DO UPDATE SET
    token_count = CASE
        WHEN rate_limits.expires_at <= now() THEN 1
        ELSE rate_limits.token_count + 1
    END,
    window_start = CASE
        WHEN rate_limits.expires_at <= now() THEN now()
        ELSE rate_limits.window_start
    END,
    expires_at = CASE
        WHEN rate_limits.expires_at <= now() THEN now() + $3::INTERVAL
        ELSE rate_limits.expires_at
    END;
