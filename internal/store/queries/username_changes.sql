-- name: CountUsernameChanges :one
-- Count changes by user_id since the cutoff.
SELECT COUNT(*)
FROM username_changes
WHERE user_id = $1
  AND changed_at >= $2;

-- name: InsertUsernameChange :exec
-- Record a username change attempt.
INSERT INTO username_changes (user_id, changed_at)
VALUES ($1, now());

-- name: DeleteExpiredUsernameChanges :exec
-- Remove changes older than the window for a specific user.
DELETE FROM username_changes
WHERE user_id = $1
  AND changed_at < $2;
