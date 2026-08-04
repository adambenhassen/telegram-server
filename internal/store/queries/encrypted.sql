-- name: InsertEncryptedChat :exec
INSERT INTO encrypted_chats (id, access_hash, user1_id, user2_id) VALUES ($1, $2, $3, $4)
ON CONFLICT (user1_id, user2_id) DO NOTHING;

-- name: GetEncryptedChat :one
SELECT id, access_hash, user1_id, user2_id FROM encrypted_chats WHERE id = $1;

-- name: InsertEncryptedEvent :exec
INSERT INTO encrypted_events (owner_id, qts, random_id, data) VALUES ($1, $2, $3, $4);

-- name: EncryptedEventsUpTo :many
SELECT owner_id, qts, random_id, data FROM encrypted_events
WHERE owner_id = $1 AND qts <= $2
ORDER BY qts;

-- name: DeleteEncryptedEventsUpTo :exec
DELETE FROM encrypted_events
WHERE owner_id = $1 AND qts <= $2;

-- name: GetQts :one
SELECT qts FROM update_state WHERE user_id = $1;

-- name: SetQts :exec
UPDATE update_state SET qts = $2 WHERE user_id = $1;
