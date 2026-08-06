-- name: InsertChannel :one
INSERT INTO channels (title, about, creator_id, megagroup) VALUES ($1, $2, $3, $4)
RETURNING *;

-- name: InsertChannelState :exec
INSERT INTO channel_state (channel_id) VALUES ($1);

-- name: InsertChannelParticipant :exec
INSERT INTO channel_participants (channel_id, user_id, role, join_pts) VALUES ($1, $2, $3, $4);

-- name: ChannelByID :one
SELECT * FROM channels WHERE id = $1;

-- ChannelStateForUpdate takes the channel_state row lock that serialises
-- admission to one channel: the pts a joiner records and the participant count
-- the caps are decided on are both read under it. See the lock-order comment at
-- the top of channels.go.
-- name: ChannelStateForUpdate :one
SELECT * FROM channel_state WHERE channel_id = $1 FOR UPDATE;

-- LockChannel takes the channels row lock that serialises the rights mutations:
-- the caller's and the target's participant rows are read under it and the write
-- lands under it, so a demotion cannot interleave with the promotion it revokes.
-- See the lock-order comment at the top of channels.go.
-- name: LockChannel :one
SELECT * FROM channels WHERE id = $1 FOR UPDATE;

-- name: ChannelParticipants :many
SELECT * FROM channel_participants WHERE channel_id = $1 ORDER BY user_id;

-- name: ChannelParticipantByUser :one
SELECT * FROM channel_participants WHERE channel_id = $1 AND user_id = $2;

-- name: IsChannelMember :one
SELECT EXISTS(SELECT 1 FROM channel_participants WHERE channel_id = $1 AND user_id = $2);

-- name: CountChannelParticipants :one
SELECT count(*) FROM channel_participants WHERE channel_id = $1;

-- CountChannelsForUser backs the per-account cap. It counts memberships, so a
-- channel the account was removed from stops counting against it.
-- name: CountChannelsForUser :one
SELECT count(*) FROM channel_participants WHERE user_id = $1;

-- InsertChannelParticipantIfAbsent reports 0 rows when the user already holds a
-- row, which is what makes a repeated join a no-op instead of an error — and in
-- particular never lowers an existing join_pts or clears an existing ban.
-- name: InsertChannelParticipantIfAbsent :execrows
INSERT INTO channel_participants (channel_id, user_id, role, join_pts) VALUES ($1, $2, $3, $4)
ON CONFLICT (channel_id, user_id) DO NOTHING;

-- UpdateChannelParticipantRole and UpdateChannelParticipantBan only ever update:
-- neither may create a participant row, so 0 rows affected means the target is
-- not a member and the caller rejects.
-- name: UpdateChannelParticipantRole :execrows
UPDATE channel_participants SET role = $3 WHERE channel_id = $1 AND user_id = $2;

-- banned_until takes SQL NULL to unban and 'infinity' for a permanent ban, so
-- the parameter is a pgtype.Timestamptz — the same type the column decodes into.
-- name: UpdateChannelParticipantBan :execrows
UPDATE channel_participants SET banned_until = $3 WHERE channel_id = $1 AND user_id = $2;

-- name: DeleteChannelParticipant :execrows
DELETE FROM channel_participants WHERE channel_id = $1 AND user_id = $2;

-- name: InsertChannelInvite :exec
INSERT INTO channel_invites (hash, channel_id, creator_id) VALUES ($1, $2, $3);

-- ChannelInviteByHash is the ONLY way into a channel. It is keyed on the hash
-- alone and takes no channel id, so the dense channels.id space is not an
-- admission input and cannot be walked. An unknown hash and an unusable one are
-- one rejection upstream — see JoinChannelByInvite. Revoked invites are
-- excluded: a revoked hash must refuse admission the same way an unknown one
-- does.
-- name: ChannelInviteByHash :one
SELECT * FROM channel_invites WHERE hash = $1 AND revoked_at IS NULL;

-- ChannelInviteByHashForUpdate is the locked variant used by JoinChannelByInvite.
-- The row lock serialises admission against revocation: a concurrent
-- RevokeChannelInvite UPDATE blocks until the join commits or rolls back. The
-- preview path (ChannelByInvite) keeps the unlocked ChannelInviteByHash.
-- name: ChannelInviteByHashForUpdate :one
SELECT * FROM channel_invites WHERE hash = $1 AND revoked_at IS NULL FOR UPDATE;

-- name: RevokeChannelInvite :execrows
UPDATE channel_invites SET revoked_at = COALESCE(revoked_at, now()) WHERE hash = $1 AND channel_id = $2;

-- name: ChannelsForUser :many
SELECT c.* FROM channels c
JOIN channel_participants p ON p.channel_id = c.id
WHERE p.user_id = $1
ORDER BY c.id;

-- SetChannelPinnedMessage sets or clears the pinned message id on a channel.
-- The pinned_message_id is the local_id of the pinned channel post. NULL clears
-- the pin.
-- name: SetChannelPinnedMessage :one
UPDATE channels SET pinned_message_id = $2, version = version + 1 WHERE id = $1 RETURNING *;

-- GetChannelPinnedMessage reads the current pinned message id for a channel.
-- name: GetChannelPinnedMessage :one
SELECT pinned_message_id FROM channels WHERE id = $1;

-- ChannelDialogsForUser returns every channel the user belongs to alongside the
-- channel's pts and the newest non-deleted post (the "top message" for the
-- dialog list). Channels with no posts or whose newest post is deleted appear
-- with top_local_id = 0 so the caller can skip them. LEFT JOIN is deliberate:
-- the 100-channel cap applies to the candidate set (all memberships), not to
-- the filtered set, so an empty channel still counts against the cap.
-- COALESCE guards against NULL from the lateral join; local_id >= 1 so 0 is
-- a safe sentinel for "no row".
-- name: ChannelDialogsForUser :many
SELECT
    c.id AS channel_id,
    c.title,
    c.about,
    c.creator_id,
    c.megagroup,
    c.version,
    c.date AS channel_date,
    cs.pts,
    cs.next_local_id,
    cs.date AS state_date,
    COALESCE(top.local_id, 0)  AS top_local_id,
    COALESCE(top.from_id, 0)   AS top_from_id,
    top.date                   AS top_date,
    COALESCE(top.message, '')  AS top_message,
    top.edit_date              AS top_edit_date,
    COALESCE(top.deleted, false) AS top_deleted,
    COALESCE(top.random_id, 0) AS top_random_id,
    top.file_id                AS top_file_id
FROM channels c
JOIN channel_participants p ON p.channel_id = c.id
JOIN channel_state cs ON cs.channel_id = c.id
LEFT JOIN LATERAL (
    SELECT cm.*
    FROM channel_messages cm
    WHERE cm.channel_id = c.id AND cm.deleted = false
    ORDER BY cm.local_id DESC
    LIMIT 1
) top ON true
WHERE p.user_id = $1
ORDER BY c.id;
