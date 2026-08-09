-- name: TryConsumeSendCodeIPCall :one
-- Attempt to consume one sendCode token for an IP key.
--
-- INSERT seeds a new counter; ON CONFLICT fires the DO UPDATE:
--   - Window expired: reset count=1, window_start=now, expires_at=now+window.
--   - Window active, under limit: bump count.
--   - Window active, at limit: the WHERE clause prevents the UPDATE entirely.
--
-- Returns one row when the call is allowed, and pgx.ErrNoRows when it is not.
-- One row per key per window, never one per call: this is an unauthenticated
-- path and the table must not grow with request count.
INSERT INTO send_code_ip_calls (ip_key, token_count, window_start, expires_at)
VALUES ($1, 1, now(), now() + $2::INTERVAL)
ON CONFLICT (ip_key) DO UPDATE SET
    token_count = CASE
        WHEN now() - send_code_ip_calls.window_start >= $2::INTERVAL THEN 1
        ELSE send_code_ip_calls.token_count + 1
    END,
    window_start = CASE
        WHEN now() - send_code_ip_calls.window_start >= $2::INTERVAL THEN now()
        ELSE send_code_ip_calls.window_start
    END,
    expires_at = CASE
        WHEN now() - send_code_ip_calls.window_start >= $2::INTERVAL THEN now() + $2::INTERVAL
        ELSE send_code_ip_calls.expires_at
    END
WHERE now() - send_code_ip_calls.window_start >= $2::INTERVAL
   OR send_code_ip_calls.token_count < $3
RETURNING token_count, expires_at;

-- name: GetSendCodeIPCallExpiry :one
-- Read the expiry deadline of a key's current window, for the wait a denied
-- call is told to observe.
SELECT expires_at
FROM send_code_ip_calls
WHERE ip_key = $1;

-- name: DeleteExpiredSendCodeIPPhones :exec
-- Prune one key's expired phone rows before they are counted, so retention is
-- the limit window rather than whatever the periodic sweep last reached.
DELETE FROM send_code_ip_phones
WHERE ip_key = $1
  AND expires_at <= now();

-- name: GetSendCodeIPPhoneUsage :one
-- Report how many distinct phone numbers a key currently holds, and whether the
-- one being requested is already among them. A number the key has already been
-- charged for costs nothing to repeat; a new one needs a free slot. Caller must
-- have pruned first, so every row counted here is still inside its window.
SELECT
    count(*)::INT AS used,
    coalesce(bool_or(phone = $2), FALSE)::BOOLEAN AS counted
FROM send_code_ip_phones
WHERE ip_key = $1;

-- name: GetSendCodeIPPhoneNextExpiry :one
-- The deadline at which a key's oldest phone row frees a slot, which is the
-- wait a key denied on the distinct-number quota is told to observe.
SELECT expires_at
FROM send_code_ip_phones
WHERE ip_key = $1
ORDER BY expires_at
LIMIT 1;

-- name: InsertSendCodeIPPhone :exec
-- Charge one distinct phone number to a key. Only called once the usage read
-- above has shown the key has a free slot, which is what bounds rows per key by
-- the quota itself rather than by request count.
INSERT INTO send_code_ip_phones (ip_key, phone, expires_at)
VALUES ($1, $2, now() + $3::INTERVAL)
ON CONFLICT (ip_key, phone) DO NOTHING;

-- name: SweepExpiredSendCodeIPCalls :execrows
-- Delete counter rows whose window has closed. The deadline is on the row, so
-- the sweep does not need to know the window duration.
DELETE FROM send_code_ip_calls
WHERE expires_at <= now();

-- name: SweepExpiredSendCodeIPPhones :execrows
-- Delete phone rows past their retention deadline. Keys that stay active prune
-- their own rows on write; this is what clears the ones that go quiet.
DELETE FROM send_code_ip_phones
WHERE expires_at <= now();
