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

-- name: IsChatMember :one
SELECT EXISTS(SELECT 1 FROM chat_participants WHERE chat_id = $1 AND user_id = $2);

-- name: ChatsForUser :many
SELECT c.* FROM chats c
JOIN chat_participants p ON p.chat_id = c.id
WHERE p.user_id = $1
ORDER BY c.id;
