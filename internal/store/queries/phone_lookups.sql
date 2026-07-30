-- name: CountPhoneLookups :one
-- Count distinct phones looked up by caller_id since the cutoff.
SELECT COUNT(DISTINCT phone)
FROM phone_lookups
WHERE caller_id = $1
  AND looked_up_at >= $2;

-- name: InsertPhoneLookup :exec
-- Record a lookup attempt. caller_id and phone are the normalized values.
INSERT INTO phone_lookups (caller_id, phone, looked_up_at)
VALUES ($1, $2, now())
ON CONFLICT DO NOTHING;

-- name: DeleteExpiredPhoneLookups :exec
-- Remove lookups older than the window so the table does not grow unbounded.
DELETE FROM phone_lookups
WHERE looked_up_at < $1;
