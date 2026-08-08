-- name: InsertMessage :exec
INSERT INTO messages (owner_id, local_id, peer_type, peer_id, from_id, message, out, random_id, peer_local_id,
                      fanout_id, action_type, action_user_id, file_id, reply_to_msg_id,
                      fwd_from_id, fwd_date, fwd_channel_id, fwd_channel_post)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18);

-- name: MessageByOwnerLocal :one
SELECT * FROM messages WHERE owner_id = $1 AND local_id = $2;

-- name: MessageByRandomID :one
SELECT * FROM messages WHERE owner_id = $1 AND random_id = $2 AND random_id <> 0;

-- NextFanoutID allocates the id shared by every per-member copy of one chat
-- message. One value per fan-out, never 0.
-- name: NextFanoutID :one
SELECT nextval('message_fanout_seq')::bigint AS fanout_id;

-- MessagesByFanout returns every per-member copy of one chat message, ascending
-- by owner_id so a caller can take its advisory locks in that order. The
-- `fanout_id <> 0` predicate is not redundant with the equality: 0 is the "not a
-- chat message" sentinel every 1:1 row carries, so a zero argument would select
-- the entire table instead of nothing.
-- name: MessagesByFanout :many
SELECT * FROM messages
WHERE fanout_id = $1 AND fanout_id <> 0
ORDER BY owner_id;

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

-- name: SearchMessages :many
SELECT * FROM messages
WHERE owner_id = sqlc.arg(owner_id)
  AND peer_type = sqlc.arg(peer_type)
  AND peer_id = sqlc.arg(peer_id)
  AND out = true
  AND deleted = false
  AND message_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
  AND (sqlc.arg(offset_id)::bigint = 0 OR local_id < sqlc.arg(offset_id)::bigint)
ORDER BY local_id DESC
LIMIT sqlc.arg(lim)::int;
