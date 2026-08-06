-- name: GetUsernameByHandle :one
SELECT handle, owner_type, owner_id FROM usernames WHERE handle = lower($1);

-- name: ClaimUsername :one
INSERT INTO usernames (handle, owner_type, owner_id) VALUES (lower($1), $2, $3)
RETURNING handle, owner_type, owner_id;

-- name: ReleaseUsername :execrows
DELETE FROM usernames WHERE handle = lower($1) AND owner_type = $2 AND owner_id = $3;

-- name: GetUserByUsername :one
SELECT u.* FROM users u
JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE un.handle = lower($1);

-- name: GetChannelByUsername :one
SELECT c.* FROM channels c
JOIN usernames un ON un.owner_type = 'channel' AND un.owner_id = c.id
WHERE un.handle = lower($1);
