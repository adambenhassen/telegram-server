-- name: GetUsernameByHandle :one
SELECT handle, owner_type, owner_id FROM usernames WHERE handle = lower(sqlc.arg(handle));

-- name: ClaimUsername :one
INSERT INTO usernames (handle, owner_type, owner_id) VALUES (lower(sqlc.arg(handle)), sqlc.arg(owner_type), sqlc.arg(owner_id))
RETURNING handle, owner_type, owner_id;

-- name: ReleaseUsername :execrows
DELETE FROM usernames WHERE handle = lower(sqlc.arg(handle)) AND owner_type = sqlc.arg(owner_type) AND owner_id = sqlc.arg(owner_id);

-- name: GetUserByUsername :one
SELECT u.* FROM users u
JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE un.handle = lower(sqlc.arg(handle));

-- name: GetChannelByUsername :one
SELECT c.* FROM channels c
JOIN usernames un ON un.owner_type = 'channel' AND un.owner_id = c.id
WHERE un.handle = lower(sqlc.arg(handle));
