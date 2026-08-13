-- name: GetUsernameByHandle :one
SELECT handle, owner_type, owner_id FROM usernames WHERE handle = lower(sqlc.arg(handle));

-- name: ClaimUsername :one
INSERT INTO usernames (handle, owner_type, owner_id) VALUES (lower(sqlc.arg(handle)), sqlc.arg(owner_type), sqlc.arg(owner_id))
RETURNING handle, owner_type, owner_id;

-- name: ReleaseUsername :execrows
DELETE FROM usernames WHERE handle = lower(sqlc.arg(handle)) AND owner_type = sqlc.arg(owner_type) AND owner_id = sqlc.arg(owner_id);

-- name: ReleaseUsernameByOwner :execrows
DELETE FROM usernames WHERE owner_type = sqlc.arg(owner_type) AND owner_id = sqlc.arg(owner_id);

-- name: GetUserByUsername :one
-- The handle reported back is the joined usernames row — the one that admitted
-- the account to this result — not the denormalized users.username copy.
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       un.handle AS username
FROM users u
JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE un.handle = lower(sqlc.arg(handle));

-- name: GetUserByUsernameWithLoginMode :one
-- Resolves a normalized username to the owning user, including login_mode.
-- Only matches owner_type='user' — a channel with the same handle returns no row.
-- Returns login_mode so the signIn handler can decide 2FA vs sign-up vs error.
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       u.login_mode, un.handle AS username
FROM users u
JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE un.handle = lower(sqlc.arg(handle));

-- name: GetChannelByUsername :one
SELECT c.* FROM channels c
JOIN usernames un ON un.owner_type = 'channel' AND un.owner_id = c.id
WHERE un.handle = lower(sqlc.arg(handle));
