-- name: ChargeSignInFailCall :one
-- Conditional charge: add one token to the counter only if the budget is not
-- exhausted. Called before VerifyCode to reserve a slot. Returns pgx.ErrNoRows
-- when the window is open and at the limit — the racing request that already
-- consumed the last token wins, and the others get an error the handler fails
-- closed on.
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

-- name: RefundSignInFailCall :exec
-- Return a reserved slot to the counter. Called after a successful verification
-- or an internal error (no verification happened). Decrements token_count by 1,
-- floored at 0. Only applies to active windows.
UPDATE sign_in_fail_calls
SET token_count = GREATEST(token_count - 1, 0)
WHERE ip_key = $1 AND expires_at > now();

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
