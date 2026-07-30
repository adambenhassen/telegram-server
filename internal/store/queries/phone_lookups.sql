-- name: CountPhoneLookups :one
-- Count distinct phones looked up by caller_id since the cutoff.
SELECT COUNT(DISTINCT phone)
FROM phone_lookups
WHERE caller_id = $1
  AND looked_up_at >= $2;

-- name: InsertPhoneLookup :exec
-- Record a lookup attempt. caller_id and phone are the normalized values.
-- COUNT DISTINCT phone enforces the quota; one row per call is expected.
INSERT INTO phone_lookups (caller_id, phone, looked_up_at)
VALUES ($1, $2, now());

-- name: DeleteExpiredPhoneLookups :exec
-- Remove lookups older than the window for a specific caller.
DELETE FROM phone_lookups
WHERE caller_id = $1
  AND looked_up_at < $2;
