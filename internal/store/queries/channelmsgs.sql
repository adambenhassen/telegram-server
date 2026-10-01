-- name: EnsureChannelState :exec
INSERT INTO channel_state (channel_id) VALUES ($1) ON CONFLICT (channel_id) DO NOTHING;

-- name: GetChannelState :one
SELECT * FROM channel_state WHERE channel_id = $1;

-- LockChannelState takes the channel_state row lock ahead of the dedup read, so
-- two concurrent posts carrying the same random_id cannot both miss it. It is
-- the same row the bump below locks, taken one statement earlier.
-- name: LockChannelState :one
SELECT * FROM channel_state WHERE channel_id = $1 FOR UPDATE;

-- BumpChannelState allocates the next local_id and advances pts in one step. It
-- returns the new pts and the local_id just consumed (pre-increment value).
-- name: BumpChannelState :one
UPDATE channel_state
SET pts = pts + 1, next_local_id = next_local_id + 1, date = now()
WHERE channel_id = $1
RETURNING pts, (next_local_id - 1)::bigint AS local_id;

-- ChannelEventsWindow returns events in (from_pts, to_pts] ordered, capped by
-- lim. The upper bound pins the read to a pts snapshot so the difference never
-- advertises a pts past an event it omitted.
-- name: ChannelEventsWindow :many
SELECT * FROM channel_events
WHERE channel_id = sqlc.arg(channel_id) AND pts > sqlc.arg(from_pts) AND pts <= sqlc.arg(to_pts)
ORDER BY pts
LIMIT sqlc.arg(lim)::int;

-- NewChannelPostPts is NewMessagePts for a channel's log, and carries the same
-- contract: one row per post, type = 1 spelled as a literal for the partial
-- index.
-- name: NewChannelPostPts :one
SELECT pts FROM channel_events
WHERE channel_id = $1 AND local_id = $2 AND type = 1;

-- name: InsertChannelEvent :exec
INSERT INTO channel_events (channel_id, pts, type, local_id) VALUES ($1, $2, $3, $4);

-- name: InsertChannelMessage :exec
INSERT INTO channel_messages (channel_id, local_id, from_id, message, random_id, file_id, reply_to_msg_id)
VALUES ($1, $2, $3, $4, $5, $6, $7);

-- ChannelPostExistsActive returns the local_id of an existing, non-deleted post
-- in channelID. Used to validate reply_to_msg_id inside the post transaction.
--
-- LOCK INVARIANT: callers must hold the channel_state row lock (LockChannelState)
-- before calling this. A future channel-post delete path must acquire the same
-- lock before writing deleted = true, or the snapshot read here can race it and
-- accept a reply reference to a post that is deleted by the time we commit.
-- name: ChannelPostExistsActive :one
SELECT local_id FROM channel_messages
WHERE channel_id = $1 AND local_id = $2 AND deleted = false;

-- name: ChannelMessageByLocal :one
SELECT channel_id, local_id, from_id, date, message, edit_date, deleted, random_id, file_id, reply_to_msg_id
FROM channel_messages WHERE channel_id = $1 AND local_id = $2;

-- name: ChannelMessageByRandomID :one
SELECT channel_id, local_id, from_id, date, message, edit_date, deleted, random_id, file_id, reply_to_msg_id
FROM channel_messages WHERE channel_id = $1 AND random_id = $2 AND random_id <> 0;

-- name: ChannelMessagesByLocalIDs :many
SELECT channel_id, local_id, from_id, date, message, edit_date, deleted, random_id, file_id, reply_to_msg_id
FROM channel_messages
WHERE channel_id = $1 AND local_id = ANY(sqlc.arg(local_ids)::bigint[]);

-- name: ChannelHistoryPage :many
SELECT channel_id, local_id, from_id, date, message, edit_date, deleted, random_id, file_id, reply_to_msg_id
FROM channel_messages
WHERE channel_id = sqlc.arg(channel_id) AND deleted = false
  AND (sqlc.arg(offset_id)::bigint = 0 OR local_id < sqlc.arg(offset_id)::bigint)
ORDER BY local_id DESC
LIMIT sqlc.arg(lim)::int;

-- SearchChannelPostsPage is ChannelHistoryPage narrowed by a full-text match.
-- It carries no caller predicate: a channel keeps one shared row per post
-- rather than one copy per member, so there is no owner column to filter on and
-- membership is the caller's whole gate, checked before this runs.
-- message_tsv is index-backed (GIN), so the match is not a sequential scan.
-- name: SearchChannelPostsPage :many
SELECT channel_id, local_id, from_id, date, message, edit_date, deleted, random_id, file_id, reply_to_msg_id
FROM channel_messages
WHERE channel_id = sqlc.arg(channel_id) AND deleted = false
  AND message_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
  AND (sqlc.arg(offset_id)::bigint = 0 OR local_id < sqlc.arg(offset_id)::bigint)
ORDER BY local_id DESC
LIMIT sqlc.arg(lim)::int;

-- SearchPinnedChannelPostForMember returns the active pinned post only while
-- the viewer still has an unbanned participant row for the channel. Posts are
-- shared, so unlike chat pins the channel local_id is already the wire id.
-- name: SearchPinnedChannelPostForMember :many
SELECT post.channel_id, post.local_id, post.from_id, post.date, post.message,
       post.edit_date, post.deleted, post.random_id, post.file_id, post.reply_to_msg_id
FROM channels c
JOIN channel_participants participant
  ON participant.channel_id = c.id
 AND participant.user_id = sqlc.arg(owner_id)::bigint
 AND (participant.banned_until IS NULL OR participant.banned_until <= now())
JOIN channel_messages post
  ON post.channel_id = c.id
 AND post.local_id = c.pinned_message_id
 AND post.deleted = false
WHERE c.id = sqlc.arg(channel_id)::bigint
  AND c.pinned_message_id IS NOT NULL
  AND (sqlc.arg(query)::text = '' OR post.message_tsv @@ plainto_tsquery('simple', sqlc.arg(query)))
  AND (sqlc.arg(offset_id)::bigint = 0 OR post.local_id < sqlc.arg(offset_id)::bigint)
ORDER BY post.local_id DESC
LIMIT sqlc.arg(lim)::int;
