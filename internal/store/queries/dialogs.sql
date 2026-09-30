-- UpsertDialog advances top_message and adds an unread delta (0 for the sender's
-- outbox side, 1 for the recipient's inbox side).
-- name: UpsertDialog :exec
INSERT INTO dialogs (owner_id, peer_type, peer_id, top_message, unread_count)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (owner_id, peer_type, peer_id) DO UPDATE
  SET top_message  = EXCLUDED.top_message,
      unread_count = dialogs.unread_count + EXCLUDED.unread_count;

-- name: DialogsForOwner :many
SELECT * FROM dialogs
WHERE owner_id = sqlc.arg(owner_id)
  AND (sqlc.arg(offset_id)::bigint = 0 OR top_message < sqlc.arg(offset_id)::bigint)
ORDER BY top_message DESC
LIMIT sqlc.arg(lim)::int;

-- CountDialogsForOwner is the total the truncated reply advertises, so a client
-- knows more pages exist. Unfiltered by offset on purpose: it is the size of the
-- whole list, not of the remainder.
-- name: CountDialogsForOwner :one
SELECT count(*)::int FROM dialogs WHERE owner_id = $1;

-- AdvanceReadInbox raises the reader's read_inbox_max_id monotonically and
-- recomputes unread as the count of still-unread inbound messages above it.
-- name: AdvanceReadInbox :one
UPDATE dialogs SET
  read_inbox_max_id = GREATEST(read_inbox_max_id, sqlc.arg(max_id)::bigint),
  read_outbox_max_id = CASE
    WHEN dialogs.owner_id = dialogs.peer_id
      THEN GREATEST(dialogs.read_outbox_max_id, sqlc.arg(max_id)::bigint)
    ELSE dialogs.read_outbox_max_id
  END,
  unread_count = (
    SELECT count(*) FROM messages m
    WHERE m.owner_id = dialogs.owner_id AND m.peer_type = dialogs.peer_type AND m.peer_id = dialogs.peer_id
      AND m.out = false AND m.deleted = false
      AND m.local_id > GREATEST(dialogs.read_inbox_max_id, sqlc.arg(max_id)::bigint)
  )::int
WHERE owner_id = sqlc.arg(owner_id)::bigint AND peer_type = sqlc.arg(peer_type)::smallint
  AND peer_id = sqlc.arg(peer_id)::bigint
RETURNING read_inbox_max_id, unread_count;

-- AdvanceChatReadInbox leaves the chat's outbox marker alone even when its id
-- numerically equals the reader's user id.
-- name: AdvanceChatReadInbox :one
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

-- ReadMarkers reads the current inbox and outbox boundaries for one dialog.
-- name: ReadMarkers :one
SELECT read_inbox_max_id, read_outbox_max_id FROM dialogs
WHERE owner_id = $1 AND peer_type = $2 AND peer_id = $3;

-- name: MaxChatReadMessageID :one
SELECT COALESCE(MAX(local_id), 0)::bigint AS max_id
FROM messages
WHERE owner_id = $1 AND peer_type = $2 AND peer_id = $3
  AND deleted = false AND local_id <= $4;

-- ChatReadReceiptTargets maps covered inbound chat copies to each current
-- sender's own outbound message-id space. The current participant list is
-- supplied only after its owners have been locked and rechecked.
-- name: ChatReadReceiptTargets :many
WITH covered AS (
    SELECT DISTINCT m.from_id, m.fanout_id
    FROM messages AS m
    WHERE m.owner_id = sqlc.arg(owner_id)::bigint
      AND m.peer_type = sqlc.arg(peer_type)::smallint
      AND m.peer_id = sqlc.arg(peer_id)::bigint
      AND m.out = false AND m.deleted = false AND m.fanout_id <> 0
      AND m.local_id > sqlc.arg(after_id)::bigint
      AND m.local_id <= sqlc.arg(max_id)::bigint
      AND m.from_id = ANY(sqlc.arg(member_ids)::bigint[])
)
SELECT covered.from_id AS sender_id, MAX(sender.local_id)::bigint AS max_id
FROM covered
JOIN messages AS sender
  ON sender.owner_id = covered.from_id
 AND sender.fanout_id = covered.fanout_id
 AND sender.fanout_id <> 0
 AND sender.out = true
 AND sender.peer_type = sqlc.arg(peer_type)::smallint
 AND sender.peer_id = sqlc.arg(peer_id)::bigint
GROUP BY covered.from_id
ORDER BY covered.from_id;
