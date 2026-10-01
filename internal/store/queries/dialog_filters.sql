-- name: EnsureDialogFilterState :exec
INSERT INTO user_dialog_filter_state (owner_id)
VALUES ($1)
ON CONFLICT (owner_id) DO NOTHING;

-- name: DialogFilterState :one
SELECT owner_id, order_ids, changed_at
FROM user_dialog_filter_state
WHERE owner_id = $1;

-- name: DialogFilterStateForUpdate :one
SELECT owner_id, order_ids, changed_at
FROM user_dialog_filter_state
WHERE owner_id = $1
FOR UPDATE;

-- name: LockUpdateState :one
SELECT user_id
FROM update_state
WHERE user_id = $1
FOR UPDATE;

-- name: ListDialogFilters :many
SELECT *
FROM user_dialog_filters
WHERE owner_id = $1
ORDER BY filter_id;

-- name: ListDialogFilterPeers :many
SELECT owner_id, filter_id, list_type, peer_type, peer_id, peer_position
FROM user_dialog_filter_peers
WHERE owner_id = $1
ORDER BY filter_id, list_type, peer_position;

-- name: ListDialogFilterEntities :many
SELECT owner_id, filter_id, entity_position, entity_offset, length, document_id
FROM user_dialog_filter_entities
WHERE owner_id = $1
ORDER BY filter_id, entity_position;

-- name: DialogFilterChangeAt :one
SELECT changed_at
FROM user_dialog_filter_state
WHERE owner_id = $1;

-- name: DialogFilterFolderCount :one
SELECT count(*)::integer
FROM user_dialog_filters
WHERE owner_id = $1;

-- name: DialogFilterExists :one
SELECT EXISTS (
    SELECT 1 FROM user_dialog_filters
    WHERE owner_id = $1 AND filter_id = $2
);

-- name: UpsertDialogFilter :exec
INSERT INTO user_dialog_filters (
    owner_id, filter_id, title, emoticon, color, contacts, non_contacts,
    groups, broadcasts, bots, exclude_muted, exclude_read, exclude_archived,
    title_noanimate
)
VALUES (
    $1, $2, $3, $4, $5, $6, $7,
    $8, $9, $10, $11, $12, $13,
    $14
)
ON CONFLICT (owner_id, filter_id) DO UPDATE SET
    title = EXCLUDED.title,
    emoticon = EXCLUDED.emoticon,
    color = EXCLUDED.color,
    contacts = EXCLUDED.contacts,
    non_contacts = EXCLUDED.non_contacts,
    groups = EXCLUDED.groups,
    broadcasts = EXCLUDED.broadcasts,
    bots = EXCLUDED.bots,
    exclude_muted = EXCLUDED.exclude_muted,
    exclude_read = EXCLUDED.exclude_read,
    exclude_archived = EXCLUDED.exclude_archived,
    title_noanimate = EXCLUDED.title_noanimate;

-- name: DeleteDialogFilter :execrows
DELETE FROM user_dialog_filters
WHERE owner_id = $1 AND filter_id = $2;

-- name: DeleteDialogFilterPeers :exec
DELETE FROM user_dialog_filter_peers
WHERE owner_id = $1 AND filter_id = $2;

-- name: InsertDialogFilterPeer :exec
INSERT INTO user_dialog_filter_peers (
    owner_id, filter_id, list_type, peer_type, peer_id, peer_position
)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: DeleteDialogFilterEntities :exec
DELETE FROM user_dialog_filter_entities
WHERE owner_id = $1 AND filter_id = $2;

-- name: InsertDialogFilterEntity :exec
INSERT INTO user_dialog_filter_entities (
    owner_id, filter_id, entity_position, entity_offset, length, document_id
)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: CommitDialogFilterMutation :exec
UPDATE user_dialog_filter_state
SET order_ids = $2, changed_at = clock_timestamp()
WHERE owner_id = $1;

-- name: NotifyDialogFilterMutation :exec
SELECT pg_notify('tg_dialog_filters', $1);

-- name: DialogFilterChatMemberships :many
SELECT chat_id
FROM chat_participants
WHERE user_id = $1 AND chat_id = ANY(sqlc.arg(chat_ids)::bigint[]);

-- name: DialogFilterChannelMemberships :many
SELECT *
FROM channel_participants
WHERE user_id = $1 AND channel_id = ANY(sqlc.arg(channel_ids)::bigint[]);
