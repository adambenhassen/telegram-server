-- name: InsertMessage :exec
INSERT INTO messages (owner_id, local_id, peer_type, peer_id, from_id, message, out, random_id, peer_local_id,
                      fanout_id, action_type, action_user_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12);

-- name: MessageByOwnerLocal :one
SELECT * FROM messages WHERE owner_id = $1 AND local_id = $2;

-- name: MessageByRandomID :one
SELECT * FROM messages WHERE owner_id = $1 AND random_id = $2 AND random_id <> 0;

-- name: HistoryPage :many
SELECT * FROM messages
WHERE owner_id = sqlc.arg(owner_id) AND peer_type = sqlc.arg(peer_type) AND peer_id = sqlc.arg(peer_id)
  AND deleted = false
  AND (sqlc.arg(offset_id)::bigint = 0 OR local_id < sqlc.arg(offset_id)::bigint)
ORDER BY local_id DESC
LIMIT sqlc.arg(lim)::int;

-- name: SetEditedText :exec
UPDATE messages SET message = $3, edit_date = now() WHERE owner_id = $1 AND local_id = $2;

-- name: SetDeleted :exec
UPDATE messages SET deleted = true WHERE owner_id = $1 AND local_id = $2;
