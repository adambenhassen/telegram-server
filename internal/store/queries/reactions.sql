-- name: UpsertReaction :exec
INSERT INTO message_reactions (owner_id, local_id, reactor_id, reaction)
VALUES ($1, $2, $3, $4)
ON CONFLICT (owner_id, local_id, reactor_id)
DO UPDATE SET reaction = $4;

-- name: DeleteReaction :exec
DELETE FROM message_reactions
WHERE owner_id = $1 AND local_id = $2 AND reactor_id = $3;

-- name: ReactionsByMessage :many
SELECT * FROM message_reactions
WHERE owner_id = $1 AND local_id = $2
ORDER BY reactor_id;

-- name: ReactionsByMessages :many
SELECT * FROM message_reactions
WHERE owner_id = sqlc.arg(owner_id)
  AND local_id = ANY(sqlc.arg(local_ids)::bigint[])
ORDER BY local_id, reactor_id;

-- name: MessagesByOwnerLocalIDs :many
SELECT * FROM messages
WHERE owner_id = sqlc.arg(owner_id)
  AND local_id = ANY(sqlc.arg(local_ids)::bigint[]);
