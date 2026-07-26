-- UpsertDialog advances top_message and adds an unread delta (0 for the sender's
-- outbox side, 1 for the recipient's inbox side).
-- name: UpsertDialog :exec
INSERT INTO dialogs (owner_id, peer_type, peer_id, top_message, unread_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (owner_id, peer_type, peer_id) DO UPDATE
  SET top_message  = EXCLUDED.top_message,
      unread_count = dialogs.unread_count + EXCLUDED.unread_count;

-- name: DialogsForOwner :many
SELECT * FROM dialogs WHERE owner_id = $1 ORDER BY top_message DESC;

-- AdvanceReadInbox raises the reader's read_inbox_max_id monotonically and
-- recomputes unread as the count of still-unread inbound messages above it.
-- name: AdvanceReadInbox :one
UPDATE dialogs SET
  read_inbox_max_id = GREATEST(read_inbox_max_id, sqlc.arg(max_id)::bigint),
  unread_count = (
    SELECT count(*) FROM messages m
    WHERE m.owner_id = dialogs.owner_id AND m.peer_type = dialogs.peer_type AND m.peer_id = dialogs.peer_id
      AND m.out = false AND m.deleted = false
      AND m.local_id > GREATEST(dialogs.read_inbox_max_id, sqlc.arg(max_id)::bigint)
  )::int
WHERE owner_id = sqlc.arg(owner_id)::bigint AND peer_type = sqlc.arg(peer_type)::smallint
  AND peer_id = sqlc.arg(peer_id)::bigint
RETURNING read_inbox_max_id, unread_count;

-- AdvanceReadOutbox raises the peer's read_outbox_max_id monotonically.
-- name: AdvanceReadOutbox :execrows
UPDATE dialogs SET read_outbox_max_id = GREATEST(read_outbox_max_id, $4)
WHERE owner_id = $1 AND peer_type = $2 AND peer_id = $3;
