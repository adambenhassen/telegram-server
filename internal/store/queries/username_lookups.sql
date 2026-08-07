-- name: CountUsernameLookups :one
-- Count distinct handles looked up by caller_id since the cutoff.
SELECT COUNT(DISTINCT handle)
FROM username_lookups
WHERE caller_id = $1
  AND looked_up_at >= $2;

-- name: InsertUsernameLookup :exec
-- Record a lookup attempt. caller_id and handle are the normalized values.
-- looked_up_at is passed from Go (time.Now() after the advisory lock) so the
-- burst window is measured against wall-clock time, not transaction-start time.
INSERT INTO username_lookups (caller_id, handle, looked_up_at)
VALUES ($1, $2, $3);

-- name: DeleteExpiredUsernameLookups :exec
-- Remove lookups older than the window for a specific caller.
DELETE FROM username_lookups
WHERE caller_id = $1
  AND looked_up_at < $2;
