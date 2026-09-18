-- PeerDialogsForOwner selects only owner-scoped basic dialogs named by the
-- caller. The peer-type predicates stay separate so a user id and a chat id
-- with the same numeric value cannot cross-match.
-- name: PeerDialogsForOwner :many
SELECT * FROM dialogs
WHERE owner_id = sqlc.arg(owner_id)::bigint
  AND (
    (peer_type = 1 AND peer_id = ANY(sqlc.arg(user_ids)::bigint[]))
    OR (peer_type = 2 AND peer_id = ANY(sqlc.arg(chat_ids)::bigint[]))
  );

-- name: MessagesByOwnerLocals :many
SELECT * FROM messages
WHERE owner_id = sqlc.arg(owner_id)::bigint
  AND local_id = ANY(sqlc.arg(local_ids)::bigint[]);

-- name: ChatsByIDs :many
SELECT * FROM chats
WHERE id = ANY(sqlc.arg(chat_ids)::bigint[]);

-- name: ChatParticipantsByChatIDs :many
SELECT * FROM chat_participants
WHERE chat_id = ANY(sqlc.arg(chat_ids)::bigint[])
ORDER BY chat_id, user_id;

-- PeerChannelDialogsForOwner is the channel counterpart of
-- PeerDialogsForOwner. A channel has no dialogs row; its current, unbanned
-- membership and newest live post are the dialog selection. The membership
-- predicate and top-post lookup are in this one statement so the selected post
-- can never come from a channel the viewer is not entitled to read in the same
-- snapshot.
-- name: PeerChannelDialogsForOwner :many
SELECT
    c.id AS channel_id,
    c.title AS channel_title,
    c.about AS channel_about,
    c.creator_id AS channel_creator_id,
    c.megagroup AS channel_megagroup,
    c.version AS channel_version,
    c.date AS channel_date,
    c.pinned_message_id AS channel_pinned_message_id,
    c.username AS channel_username,
    p.role AS member_role,
    p.banned_until AS member_banned_until,
    p.join_pts AS member_join_pts,
    cs.pts AS channel_pts,
    top.local_id AS top_local_id,
    top.from_id AS top_from_id,
    top.date AS top_date,
    top.message AS top_message,
    top.edit_date AS top_edit_date,
    top.random_id AS top_random_id,
    top.file_id AS top_file_id,
    top.reply_to_msg_id AS top_reply_to_msg_id
FROM channels c
JOIN channel_participants p ON p.channel_id = c.id
JOIN channel_state cs ON cs.channel_id = c.id
JOIN LATERAL (
    SELECT cm.local_id, cm.from_id, cm.date, cm.message, cm.edit_date,
           cm.random_id, cm.file_id, cm.reply_to_msg_id
    FROM channel_messages cm
    WHERE cm.channel_id = c.id AND cm.deleted = false
    ORDER BY cm.local_id DESC
    LIMIT 1
) top ON true
WHERE c.id = ANY(sqlc.arg(channel_ids)::bigint[])
  AND p.user_id = sqlc.arg(owner_id)::bigint
  AND (p.banned_until IS NULL OR p.banned_until <= now());

-- name: UnreadCountForOwner :one
SELECT COALESCE(SUM(unread_count), 0)::bigint
FROM dialogs
WHERE owner_id = sqlc.arg(owner_id)::bigint;
