-- name: NextSecretChatID :one
-- Server-allocated chat id. Ids are never reused, including by a discarded chat.
SELECT nextval('secret_chats_id_seq')::bigint;

-- name: CountRequestedSecretChats :one
-- Outstanding requests the caller has initiated, for the per-account cap. Only
-- rows the caller admins are counted, so an inbound request never costs quota.
SELECT COUNT(*) FROM secret_chats WHERE admin_id = $1 AND state = 'requested';

-- name: InsertSecretChat :one
INSERT INTO secret_chats (id, admin_id, participant_id, state, g_a_hash, g_a)
VALUES ($1, $2, $3, 'requested', $4, $5)
RETURNING *;

-- name: SecretChatByID :one
SELECT * FROM secret_chats WHERE id = $1;

-- AcceptSecretChat is the whole replay guard: the state and participant
-- predicates make a second accept, and an accept by the initiator, match zero
-- rows instead of rewriting the fingerprint under a chat the initiator has
-- already keyed.
-- name: AcceptSecretChat :one
UPDATE secret_chats
SET state = 'active', g_a_or_b = sqlc.arg(g_a_or_b), key_fingerprint = sqlc.arg(key_fingerprint)
WHERE id = sqlc.arg(id) AND participant_id = sqlc.arg(participant_id) AND state = 'requested'
RETURNING *;

-- DiscardSecretChat is idempotent by predicate: a chat already discarded matches
-- zero rows, and 'discarded' is terminal.
-- name: DiscardSecretChat :one
UPDATE secret_chats
SET state = 'discarded'
WHERE id = $1 AND state IN ('requested', 'active')
RETURNING *;
