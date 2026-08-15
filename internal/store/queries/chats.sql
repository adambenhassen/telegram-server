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

-- name: IsChatMember :one
SELECT EXISTS(SELECT 1 FROM chat_participants WHERE chat_id = $1 AND user_id = $2);

-- name: ChatsForUser :many
SELECT c.* FROM chats c
JOIN chat_participants p ON p.chat_id = c.id
WHERE p.user_id = $1
ORDER BY c.id;

-- SetChatPinnedMessage sets or clears the pinned message id on a chat.
-- The pinned_message_id is the local_id of the pinned message (identical across
-- members for a given fanout). NULL clears the pin.
-- name: SetChatPinnedMessage :one
UPDATE chats SET pinned_message_id = $2, version = version + 1 WHERE id = $1 RETURNING *;

-- GetChatPinnedMessage reads the current pinned message id for a chat.
-- name: GetChatPinnedMessage :one
SELECT pinned_message_id FROM chats WHERE id = $1;
