-- name: CountChannelUsernameChanges :one
-- Count changes by channel_id since the cutoff.
SELECT COUNT(*)
FROM channel_username_changes
WHERE channel_id = $1
  AND changed_at >= $2;

-- name: InsertChannelUsernameChange :exec
-- Record a channel username change.
INSERT INTO channel_username_changes (channel_id, changed_at)
VALUES ($1, now());

-- name: DeleteExpiredChannelUsernameChanges :exec
-- Remove changes older than the window for a specific channel.
DELETE FROM channel_username_changes
WHERE channel_id = $1
  AND changed_at < $2;
