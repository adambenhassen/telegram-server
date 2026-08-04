-- name: NextSecretChatID :one
-- Server-allocated chat id. Ids are never reused, including by a discarded chat.
SELECT nextval('secret_chats_id_seq')::bigint;

-- name: CountRequestedSecretChats :one
-- Outstanding requests the caller has initiated, for the per-account cap. Only
-- rows the caller admins are counted, so an inbound request never costs quota.
SELECT COUNT(*) FROM secret_chats WHERE admin_id = $1 AND state = 'requested';

-- name: GetSecretChatByAdminRandomID :one
-- ponytail: lookup-before-insert dedup; avoids ON CONFLICT and sequence waste.
SELECT * FROM secret_chats WHERE admin_id = $1 AND random_id = $2 AND random_id != 0 AND state = 'requested';

-- name: InsertSecretChat :one
INSERT INTO secret_chats (id, admin_id, participant_id, state, g_a_hash, g_a, random_id)
VALUES ($1, $2, $3, 'requested', $4, $5, $6)
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

-- InsertEncryptedEvent stores one encrypted payload for the recipient. The qts
-- is taken from update_state.qts + 1, which matches the value BumpQts will
-- write in the same transaction. The advisory lock on the recipient held by the
-- caller serialises concurrent sends so the (owner_id, qts) primary key is
-- never contended. ON CONFLICT on (owner_id, random_id) is the dedup guard:
-- a repeated random_id returns no rows (pgx.ErrNoRows).
-- name: InsertEncryptedEvent :one
INSERT INTO encrypted_events (owner_id, qts, chat_id, random_id, bytes, date)
SELECT $1, us.qts + 1, $2, $3, $4, now()
FROM update_state us WHERE us.user_id = $1
ON CONFLICT (owner_id, random_id) DO NOTHING
RETURNING *;

-- BumpQts atomically increments the recipient's qts. Called in the same
-- transaction as InsertEncryptedEvent after the insert confirms this is not a
-- dedup. The new qts matches the value InsertEncryptedEvent stored.
-- name: BumpQts :one
UPDATE update_state SET qts = qts + 1, date = now() WHERE user_id = $1 RETURNING qts;

-- GetEncryptedEventByRandomID fetches an existing event by dedup key for the
-- dedup response path.
-- name: GetEncryptedEventByRandomID :one
SELECT * FROM encrypted_events WHERE owner_id = $1 AND random_id = $2;

-- GetEncryptedEvent fetches one event by its (owner_id, qts) primary key.
-- Used by the push handler to build updateNewEncryptedMessage after a NOTIFY.
-- name: GetEncryptedEvent :one
SELECT * FROM encrypted_events WHERE owner_id = $1 AND qts = $2;
