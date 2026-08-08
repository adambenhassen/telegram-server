-- name: CreateUser :one
INSERT INTO users (phone) VALUES ($1)
ON CONFLICT (phone) DO UPDATE SET phone = EXCLUDED.phone
RETURNING *;

-- name: UserByPhone :one
SELECT * FROM users WHERE phone = $1;

-- name: UserByID :one
SELECT * FROM users WHERE id = $1;

-- name: SetUserStatus :execrows
UPDATE users SET is_online = $2, last_seen_at = now() WHERE id = $1;

-- name: SetUsername :execrows
UPDATE users SET username = $2 WHERE id = $1;

-- name: SearchContactsByName :many
-- Search users by name within the caller's existing dialogs.
-- Only users with whom the caller has exchanged messages (has a dialog row) are returned.
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at, u.username
FROM users u
JOIN dialogs d ON (
    (d.owner_id = sqlc.arg(owner_id)::bigint AND d.peer_id = u.id AND d.peer_type = 1)
    OR
    (d.peer_id = sqlc.arg(owner_id)::bigint AND d.owner_id = u.id)
)
WHERE u.name_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
LIMIT sqlc.arg(lim)::int;


