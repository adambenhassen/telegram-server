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
    c.default_banned_rights AS channel_default_banned_rights,
    p.role AS member_role,
    p.banned_until AS member_banned_until,
    p.join_pts AS member_join_pts,
    COALESCE(read_state.read_max_id, 0)::bigint AS read_inbox_max_id,
    unread.unread_count,
    unread.entitled::boolean AS summary_entitled,
    unread.status_exists::boolean AS summary_status_exists,
    unread.summary_version::smallint AS summary_version,
    unread.summary_ready::boolean AS summary_ready,
    unread.total_live::bigint AS summary_total_live,
    unread.author_live::bigint AS summary_author_live,
    cs.pts AS channel_pts,
    top.local_id AS top_local_id,
    top.from_id AS top_from_id,
    top.date AS top_date,
    top.message AS top_message,
    top.edit_date AS top_edit_date,
    top.random_id AS top_random_id,
    top.file_id AS top_file_id,
    top.reply_to_msg_id AS top_reply_to_msg_id,
    top.action_type AS top_action_type
FROM channels c
JOIN channel_participants p ON p.channel_id = c.id
JOIN channel_state cs ON cs.channel_id = c.id
LEFT JOIN channel_read_state read_state
  ON read_state.channel_id = c.id AND read_state.user_id = p.user_id
CROSS JOIN LATERAL (
    SELECT LEAST(GREATEST(summary.total_live - summary.author_live, 0), 1000)::int AS unread_count,
           summary.entitled::boolean AS entitled,
           summary.status_exists::boolean AS status_exists,
           summary.summary_version::smallint AS summary_version,
           summary.summary_ready::boolean AS summary_ready,
           summary.total_live::bigint AS total_live,
           summary.author_live::bigint AS author_live
    FROM channel_post_unread_suffix_counts(
        c.id,
        p.user_id,
        COALESCE(read_state.read_max_id, 0)
    ) AS summary(entitled, status_exists, summary_version, summary_ready, total_live, author_live)
) AS unread
JOIN LATERAL (
    SELECT cm.local_id, cm.from_id, cm.date, cm.message, cm.edit_date,
           cm.random_id, cm.file_id, cm.reply_to_msg_id, cm.action_type
    FROM channel_messages cm
    WHERE cm.channel_id = c.id AND cm.deleted = false
    ORDER BY cm.local_id DESC
    LIMIT 1
) top ON true
WHERE c.id = ANY(sqlc.arg(channel_ids)::bigint[])
  AND p.user_id = sqlc.arg(owner_id)::bigint
  AND (p.banned_until IS NULL OR p.banned_until <= now());

-- name: UnreadCountForOwner :one
WITH channel_unread AS MATERIALIZED (
    SELECT summary.entitled::boolean AS entitled,
           summary.status_exists::boolean AS status_exists,
           summary.summary_version::smallint AS summary_version,
           summary.summary_ready::boolean AS summary_ready,
           summary.total_live::bigint AS total_live,
           summary.author_live::bigint AS author_live,
           LEAST(GREATEST(summary.total_live - summary.author_live, 0), 1000)::bigint AS unread_count
    FROM channel_participants AS participant
    LEFT JOIN channel_read_state AS read_state
      ON read_state.channel_id = participant.channel_id
     AND read_state.user_id = participant.user_id
    CROSS JOIN LATERAL channel_post_unread_suffix_counts(
        participant.channel_id,
        participant.user_id,
        COALESCE(read_state.read_max_id, 0)
    ) AS summary(entitled, status_exists, summary_version, summary_ready, total_live, author_live)
    WHERE participant.user_id = sqlc.arg(owner_id)::bigint
      AND (participant.banned_until IS NULL OR participant.banned_until <= now())
)
SELECT ((
           SELECT COALESCE(SUM(dialog.unread_count), 0)
           FROM dialogs AS dialog
           WHERE dialog.owner_id = sqlc.arg(owner_id)::bigint
       ) + COALESCE((SELECT SUM(channel_unread.unread_count) FROM channel_unread), 0)::bigint)::bigint AS unread_count,
       (SELECT COUNT(*)::bigint
        FROM channel_unread
        WHERE NOT channel_unread.entitled
           OR NOT channel_unread.status_exists
           OR channel_unread.summary_version <> 1
           OR NOT channel_unread.summary_ready) AS unavailable_channel_count,
       (SELECT COUNT(*)::bigint
        FROM channel_unread
        WHERE channel_unread.total_live < channel_unread.author_live) AS corrupt_channel_count;

-- BasicUnreadCountForOwner is used by getDifference, which must preserve
-- ordinary dialog unread state without depending on channel summary readiness.
-- name: BasicUnreadCountForOwner :one
SELECT COALESCE(SUM(dialog.unread_count), 0)::bigint AS unread_count
FROM dialogs AS dialog
WHERE dialog.owner_id = sqlc.arg(owner_id)::bigint;
