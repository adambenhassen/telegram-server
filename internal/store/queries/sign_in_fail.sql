-- name: TryConsumeSignInFailCall :one
-- Attempt to consume one signIn-fail token for an IP key.
--
-- Same upsert pattern as send_code_ip_calls: one row per key per window,
-- token_count reset and bumped. Returns one row when allowed, pgx.ErrNoRows
-- when the window is open and at the limit.
INSERT INTO sign_in_fail_calls (ip_key, token_count, window_start, expires_at)
VALUES ($1, 1, now(), now() + $2::INTERVAL)
ON CONFLICT (ip_key) DO UPDATE SET
    token_count = CASE
        WHEN sign_in_fail_calls.expires_at <= now() THEN 1
        ELSE sign_in_fail_calls.token_count + 1
    END,
    window_start = CASE
        WHEN sign_in_fail_calls.expires_at <= now() THEN now()
        ELSE sign_in_fail_calls.window_start
    END,
    expires_at = CASE
        WHEN sign_in_fail_calls.expires_at <= now() THEN now() + $2::INTERVAL
        ELSE sign_in_fail_calls.expires_at
    END
WHERE sign_in_fail_calls.expires_at <= now()
   OR sign_in_fail_calls.token_count < $3
RETURNING token_count, expires_at;

-- name: GetSignInFailCallExpiry :one
-- Read the expiry deadline of a key's current window, for the wait a denied
-- attempt is told to observe.
SELECT expires_at
FROM sign_in_fail_calls
WHERE ip_key = $1;

-- name: SweepExpiredSignInFailCalls :execrows
-- Delete counter rows whose window has closed.
DELETE FROM sign_in_fail_calls
WHERE expires_at <= now();
