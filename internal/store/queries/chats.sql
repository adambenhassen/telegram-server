-- name: InsertChat :one
INSERT INTO chats (title, creator_id) VALUES ($1, $2)
RETURNING *;

-- name: InsertChatParticipant :exec
INSERT INTO chat_participants (chat_id, user_id, inviter_id) VALUES ($1, $2, $3);

-- name: ChatByID :one
SELECT * FROM chats WHERE id = $1;

-- ChatByIDForUpdate takes the chats row lock that serialises everything touching
-- one chat's member set: the fan-out reads the member set under it, and the
-- membership mutations take it before changing that set. See the lock-order
-- comment at the top of chats.go.
-- name: ChatByIDForUpdate :one
SELECT * FROM chats WHERE id = $1 FOR UPDATE;

-- ChatParticipants is ascending by user_id: the fan-out takes its advisory locks
-- in that order, so a stable ascending member list is what keeps it deadlock-free.
-- name: ChatParticipants :many
SELECT * FROM chat_participants WHERE chat_id = $1 ORDER BY user_id;

-- InsertChatParticipantIfAbsent reports 0 rows when the user is already a member,
-- which is what makes a repeated add a no-op instead of an error.
-- name: InsertChatParticipantIfAbsent :execrows
INSERT INTO chat_participants (chat_id, user_id, inviter_id) VALUES ($1, $2, $3)
ON CONFLICT (chat_id, user_id) DO NOTHING;

-- DeleteChatParticipant is called from exactly one place: removeParticipant in
-- chats.go, which also takes the removed user's advisory lock. See its comment.
-- name: DeleteChatParticipant :execrows
DELETE FROM chat_participants WHERE chat_id = $1 AND user_id = $2;

-- name: BumpChatVersion :one
UPDATE chats SET version = version + 1 WHERE id = $1 RETURNING *;

-- name: SetChatTitle :one
UPDATE chats SET title = $2, version = version + 1 WHERE id = $1 RETURNING *;

-- name: SetChatDefaultBannedRights :one
UPDATE chats
SET default_banned_rights = $2, version = version + 1
WHERE id = $1
RETURNING *;

-- name: IsChatMember :one
SELECT EXISTS(SELECT 1 FROM chat_participants WHERE chat_id = $1 AND user_id = $2);

-- name: ChatsForUser :many
SELECT c.* FROM chats c
JOIN chat_participants p ON p.chat_id = c.id
WHERE p.user_id = $1
ORDER BY c.id;

-- A missing chat and a chat without a membership row for the viewer both
-- produce no result. Keep the read on the basic-chat tables so equal numeric
-- ids in users or channels cannot resolve to those peer types.
-- name: ChatsByIDsForMember :many
SELECT c.* FROM chats c
JOIN chat_participants p ON p.chat_id = c.id
WHERE p.user_id = sqlc.arg(user_id)::bigint
  AND c.id = ANY(sqlc.arg(chat_ids)::bigint[])
ORDER BY c.id;

-- name: ChatParticipantCountsByChatIDs :many
SELECT chat_id, count(*)::bigint AS participant_count
FROM chat_participants
WHERE chat_id = ANY(sqlc.arg(chat_ids)::bigint[])
GROUP BY chat_id
ORDER BY chat_id;

-- SetChatPinnedMessage sets or clears the pinned message id on a chat.
-- The pinned_message_id is the local_id of the pinned message (identical across
-- members for a given fanout). NULL clears the pin.
-- name: SetChatPinnedMessage :one
UPDATE chats SET pinned_message_id = $2, version = version + 1 WHERE id = $1 RETURNING *;

-- GetChatPinnedMessage reads the current pinned message id for a chat.
-- name: GetChatPinnedMessage :one
SELECT pinned_message_id FROM chats WHERE id = $1;

-- ChatPinnedMessageForOwner resolves the creator-owned pin to the requested
-- member's copy. fanout_id identifies one logical chat message across each
-- member-owned row; the returned local_id always belongs to owner_id.
-- Deleted or missing source/member copies intentionally produce no row.
-- name: ChatPinnedMessageForOwner :one
SELECT viewer_copy.local_id
FROM chats c
JOIN chat_participants p ON p.chat_id = c.id AND p.user_id = sqlc.arg(owner_id)::bigint
JOIN messages creator_copy
  ON creator_copy.owner_id = c.creator_id
 AND creator_copy.local_id = c.pinned_message_id
 AND creator_copy.peer_type = sqlc.arg(peer_type)::smallint
 AND creator_copy.peer_id = c.id
 AND creator_copy.fanout_id <> 0
 AND creator_copy.deleted = false
JOIN messages viewer_copy
  ON viewer_copy.owner_id = p.user_id
 AND viewer_copy.fanout_id = creator_copy.fanout_id
 AND viewer_copy.peer_type = sqlc.arg(peer_type)::smallint
 AND viewer_copy.peer_id = c.id
 AND viewer_copy.deleted = false
WHERE c.id = sqlc.arg(chat_id)::bigint
  AND c.pinned_message_id IS NOT NULL;

-- ChatPinSnapshot reads the selected pin and each member's local copy from one
-- statement snapshot, so a concurrent repin cannot split one notification.
-- Missing or deleted copies remain NULL while the participant still receives
-- the pinned state.
-- name: ChatPinSnapshot :many
SELECT p.user_id,
       c.pinned_message_id,
       viewer_copy.local_id
FROM chats c
JOIN chat_participants p ON p.chat_id = c.id
LEFT JOIN messages creator_copy
  ON creator_copy.owner_id = c.creator_id
 AND creator_copy.local_id = c.pinned_message_id
 AND creator_copy.peer_type = sqlc.arg(peer_type)::smallint
 AND creator_copy.peer_id = c.id
 AND creator_copy.fanout_id <> 0
 AND creator_copy.deleted = false
LEFT JOIN messages viewer_copy
  ON viewer_copy.owner_id = p.user_id
 AND viewer_copy.fanout_id = creator_copy.fanout_id
 AND viewer_copy.peer_type = sqlc.arg(peer_type)::smallint
 AND viewer_copy.peer_id = c.id
 AND viewer_copy.deleted = false
WHERE c.id = sqlc.arg(chat_id)::bigint
ORDER BY p.user_id;
