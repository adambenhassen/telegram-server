-- InsertChannel takes the id from the caller rather than from channels_id_seq.
-- A new channel's id is a random draw over a sparse range (newChannelID in
-- channels.go): a dense id set discloses, through its gaps, how many private
-- channels exist and where in creation order each one sits. A repeated draw
-- arrives here as a channels_pkey unique violation, which the caller answers by
-- redrawing — never by letting the sequence supply the id.
-- name: InsertChannel :one
INSERT INTO channels (id, title, about, creator_id, megagroup) VALUES ($1, $2, $3, $4, $5)
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

-- ChannelParticipantsForViewer answers "which of these channels is this caller
-- in" in one query, for a caller-supplied set bounded by a page. It returns the
-- whole participant row rather than a boolean so the ban stays ChannelMember's
-- decision: this feeds a rendering choice, not a row set that a LIMIT then cuts,
-- so there is no reason to spell the predicate a second time here.
-- name: ChannelParticipantsForViewer :many
SELECT * FROM channel_participants
WHERE user_id = sqlc.arg(viewer_id)::bigint
  AND channel_id = ANY(sqlc.arg(channel_ids)::bigint[]);

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
-- alone and takes no channel id, so a channel id is not an admission input. An
-- unknown hash and an unusable one are one rejection upstream — see
-- JoinChannelByInvite. Revoked invites are excluded: a revoked hash must refuse
-- admission the same way an unknown one does.
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
    SELECT cm.channel_id, cm.local_id, cm.from_id, cm.date, cm.message, cm.edit_date, cm.deleted, cm.random_id, cm.file_id
    FROM channel_messages cm
    WHERE cm.channel_id = c.id AND cm.deleted = false
    ORDER BY cm.local_id DESC
    LIMIT 1
) top ON true
WHERE p.user_id = $1
ORDER BY c.id;

-- SetChannelUsername writes the denormalized handle copy and, with it,
-- recomputes publicly_discoverable from the usernames table.
--
-- The two live in one statement because there is one moment at which both are
-- true: every writer of a channel handle — SetChannelUsername and
-- ClaimChannelUsername, claim path and release path alike — mutates the
-- usernames row and then calls this, inside the transaction that does both. A
-- marker maintained at each of those call sites instead would be four places to
-- keep in step; here there is one, and it reads the authoritative table rather
-- than the argument, so it is a recomputation and not a second copy of the same
-- assertion.
--
-- A future writer that changes usernames without coming through here leaves the
-- marker stale, and that is survivable by construction rather than by luck:
-- publicly_discoverable decides cost only, the pre-LIMIT JOIN usernames in
-- SearchPublicChannels decides disclosure, and a stale marker in the permissive
-- direction costs a wasted candidate row that the join drops. See the migration
-- header for the full asymmetry.
-- name: SetChannelUsername :execrows
UPDATE channels c
SET username = sqlc.arg(username),
    publicly_discoverable = EXISTS (
        SELECT 1 FROM usernames un
        WHERE un.owner_type = 'channel' AND un.owner_id = c.id
    )
WHERE c.id = sqlc.arg(id);

-- SearchPublicChannels finds the channels any account may discover by title or
-- by handle: exactly those owning a row in usernames.
--
-- The publicness predicate lives in the WHERE and therefore runs before LIMIT.
-- That position is the whole security property. A private channel matching the
-- tsquery must never occupy a row inside LIMIT that is then dropped in Go: the
-- caller reads the shortfall in the returned row count as "a private channel
-- matching my query exists", and probing LIMIT positions turns titles into an
-- enumeration oracle over channels that are supposed to be invisible.
--
-- The query takes no viewer, deliberately. Results is public discovery and is
-- identical for every caller, so nothing in it can vary with the caller's own
-- membership or ban state and be read back as information.
--
-- Publicness is decided by the usernames row rather than the denormalized
-- channels.username column. The two are written in one transaction today
-- (ClaimChannelUsername), but only the usernames row is authoritative, and a
-- future writer that releases a handle without clearing the copy must not leave
-- the channel discoverable. The handle returned to the caller comes from the
-- same row for the same reason: the username shown is the one that admitted the
-- channel to this result, never the copy.
--
-- publicly_discoverable in the title disjunct is not a second publicness test
-- and must never be read as one. The join above stays the only thing deciding
-- what this query returns; the marker decides what it costs. It is there so the
-- candidate set holds public channels only: a private title matching the token
-- was previously scanned, probed against usernames and discarded, so the time
-- an empty answer took counted the private channels sharing a guessed word.
-- With the marker in the predicate the partial index over public titles is the
-- only index that can answer it, so the cost of this arm tracks the public
-- matches — which the response discloses anyway — and nothing else.
--
-- The match is a union of two single-relation subqueries rather than one OR
-- spanning channels and usernames, and that shape is load-bearing rather than
-- stylistic. An OR across two relations cannot be answered from either index:
-- the planner scans every channel, joins every channel handle, and evaluates
-- the disjunction as a join filter, so a query matching nothing costs the whole
-- table (162.8 ms at 200k channels, against 1.99 ms here) and an authenticated
-- account picks that worst case for free by sending a nonsense token. Each
-- disjunct here restricts on its own relation, so the title arm is a bitmap
-- scan of channels_public_title_tsv_idx and the handle arm a lookup on the
-- usernames primary key.
-- name: SearchPublicChannels :many
SELECT c.id, c.title, c.about, c.creator_id, c.megagroup, c.version, c.date,
       c.pinned_message_id, un.handle,
       (SELECT count(*) FROM channel_participants cp WHERE cp.channel_id = c.id) AS participants_count
FROM channels c
JOIN usernames un ON un.owner_type = 'channel' AND un.owner_id = c.id
WHERE c.id IN (
    SELECT tc.id FROM channels tc
    WHERE tc.publicly_discoverable
      AND tc.title_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
    UNION
    SELECT hu.owner_id FROM usernames hu WHERE hu.owner_type = 'channel' AND hu.handle = sqlc.arg(handle)
)
ORDER BY c.id
LIMIT sqlc.arg(lim)::int;

-- SearchMemberChannels finds the channels the caller belongs to by title or by
-- handle, public and private alike. Membership is the join, so a channel the
-- caller never joined can never appear here whatever its title.
--
-- The ban predicate is written here rather than left to ChannelMember.Banned
-- for the reason FileForDownload records: it decides the row set that LIMIT
-- then cuts, so a retained participant row of a banned member would otherwise
-- displace a channel the caller may actually see. It must stay spelled the same
-- way as Banned — NULL is not banned, "may act" is banned_until IS NULL OR
-- banned_until <= now().
--
-- The match is the same union of single-relation subqueries SearchPublicChannels
-- uses, for the same planner reason, but the title disjunct here is scoped by
-- channel_ids instead of by publicness. This arm cannot take the publicness
-- marker — a member has to be able to find their own private channel by title,
-- which is the whole point of the arm — so the bound it gets instead is the
-- caller's own membership, spelled inside the statement.
--
-- channel_ids is a scope, never the authorization, exactly as in
-- SearchGlobalPage: it carries the caller's own unbanned channel ids, read
-- separately off the index on channel_participants (user_id), and the join to
-- channel_participants above is what decides who may see what. A stale or wrong
-- id list can only narrow this arm, never widen it past the membership the join
-- re-checks in the same statement.
--
-- It is written as an explicit id set rather than left to the planner because
-- the bound has to be structural. Without it the candidate set is every channel
-- whose title matches, private ones included, filtered down to the caller's
-- afterwards — so the cost of an empty answer counts the private channels
-- sharing the token, which is the oracle M16 closed. A plan that avoids that by
-- preference rather than by construction is one statistics change away from
-- returning to it, and "index-driven" does not rule it out: a bitmap scan of a
-- whole-table title index followed by a membership filter is index-driven and
-- is precisely the leaking shape. With the id set in the predicate, and no
-- index over every title left to scan, the plan is a primary-key scan over at
-- most the per-account channel cap (channels.go, defaultMaxChannelsPerUser),
-- whatever the corpus behind it looks like.
--
-- The handle disjunct stays unscoped, and needs no scope: it is a primary-key
-- lookup on usernames yielding at most one row, so it costs the same whatever
-- the corpus holds, and a private channel has no row there to find. A channel
-- it names that the caller does not belong to is dropped by the join above.
--
-- No participant count here, unlike the public arm: a member is rendered by
-- channelToTL, which carries no participants_count field, so counting would be
-- a per-row aggregate over channel_participants whose result is discarded.
-- name: SearchMemberChannels :many
SELECT c.id, c.title, c.about, c.creator_id, c.megagroup, c.version, c.date,
       c.pinned_message_id, un.handle, p.role
FROM channels c
JOIN channel_participants p ON p.channel_id = c.id AND p.user_id = sqlc.arg(viewer_id)::bigint
LEFT JOIN usernames un ON un.owner_type = 'channel' AND un.owner_id = c.id
WHERE c.id IN (
    SELECT tc.id FROM channels tc
    WHERE tc.id = ANY(sqlc.arg(channel_ids)::bigint[])
      AND tc.title_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
    UNION
    SELECT hu.owner_id FROM usernames hu WHERE hu.owner_type = 'channel' AND hu.handle = sqlc.arg(handle)
)
  AND (p.banned_until IS NULL OR p.banned_until <= now())
ORDER BY c.id
LIMIT sqlc.arg(lim)::int;
