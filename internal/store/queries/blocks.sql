-- name: InsertBlockedUser :execrows
INSERT INTO blocked_users (blocker_id, blocked_id)
VALUES ($1, $2)
ON CONFLICT (blocker_id, blocked_id) DO NOTHING;

-- name: DeleteBlockedUser :execrows
DELETE FROM blocked_users
WHERE blocker_id = $1 AND blocked_id = $2;

-- name: IsBlocked :one
SELECT EXISTS(
    SELECT 1 FROM blocked_users
    WHERE blocker_id = $1 AND blocked_id = $2
);

-- name: BlockedUsers :many
SELECT blocked_id, created_at, COUNT(*) OVER() AS total
FROM blocked_users
WHERE blocker_id = sqlc.arg(blocker_id)
ORDER BY created_at DESC, blocked_id DESC
LIMIT sqlc.arg(lim)::int OFFSET sqlc.arg(off)::int;

-- name: CountBlockedUsers :one
SELECT COUNT(*) FROM blocked_users WHERE blocker_id = $1;
