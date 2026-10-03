-- name: ChannelPostSummarySchemaInstalled :one
SELECT to_regclass('public.channel_read_state') IS NOT NULL
   AND to_regclass('public.channel_post_summary_state') IS NOT NULL
   AND to_regclass('public.channel_post_summaries') IS NOT NULL
   AND to_regprocedure('public.channel_post_unread_suffix_counts(bigint,bigint,bigint)') IS NOT NULL;

-- name: ChannelPostSummaryReadinessByChannel :one
SELECT version, ready
FROM channel_post_summary_state
WHERE channel_id = $1;

-- name: ChannelPostSummaryUnavailableChannels :one
SELECT count(*)::bigint
FROM channels AS channel
LEFT JOIN channel_post_summary_state AS summary_state
  ON summary_state.channel_id = channel.id
WHERE summary_state.channel_id IS NULL
   OR summary_state.version <> 1
   OR summary_state.ready = false;

-- name: LockChannelStateForPostSummaryInitialization :one
SELECT channel_id
FROM channel_state
WHERE channel_id = $1
FOR UPDATE;

-- Call only after taking channel_state FOR UPDATE. Readiness becomes false only
-- in the explicit maintenance initializer, never on source mutation paths.
-- name: MarkChannelPostSummariesNotReady :exec
INSERT INTO channel_post_summary_state (channel_id, version, ready)
VALUES ($1, 1, false)
ON CONFLICT (channel_id)
DO UPDATE SET version = 1, ready = false;

-- name: InitializeChannelPostSummaries :exec
SELECT initialize_channel_post_summaries($1);

-- Membership and readiness share this statement snapshot with the two exact
-- suffix sums. Author scope comes only from the joined participant row.
-- Each suffix has at most 63 keys, so the two correlated PK lookups are bounded
-- at 126 equality probes and do not visit channel_messages.
-- name: ChannelPostUnreadSuffixCounts :one
SELECT summary.entitled::boolean AS entitled,
       summary.status_exists::boolean AS status_exists,
       summary.summary_version::smallint AS version,
       summary.summary_ready::boolean AS ready,
       summary.total_live::bigint AS total_live,
       summary.author_live::bigint AS author_live
FROM channel_post_unread_suffix_counts(
    sqlc.arg(channel_id)::bigint,
    sqlc.arg(viewer_id)::bigint,
    sqlc.arg(marker)::bigint
) AS summary(entitled, status_exists, summary_version, summary_ready, total_live, author_live);

-- name: ChannelReadMarkerForMember :one
SELECT COALESCE(read_state.read_max_id, 0)::bigint AS read_max_id
FROM channel_participants AS participant
LEFT JOIN channel_read_state AS read_state
  ON read_state.channel_id = participant.channel_id
 AND read_state.user_id = participant.user_id
WHERE participant.channel_id = sqlc.arg(channel_id)::bigint
  AND participant.user_id = sqlc.arg(user_id)::bigint
  AND (participant.banned_until IS NULL OR participant.banned_until <= now());

-- Call only after taking channels and channel_state locks in that order.
-- name: AdvanceChannelReadMarker :exec
INSERT INTO channel_read_state (channel_id, user_id, read_max_id)
SELECT participant.channel_id, participant.user_id, sqlc.arg(read_max_id)::bigint
FROM channel_participants AS participant
WHERE participant.channel_id = sqlc.arg(channel_id)::bigint
  AND participant.user_id = sqlc.arg(user_id)::bigint
  AND (participant.banned_until IS NULL OR participant.banned_until <= now())
ON CONFLICT (channel_id, user_id)
DO UPDATE SET read_max_id = GREATEST(channel_read_state.read_max_id, EXCLUDED.read_max_id)
WHERE EXCLUDED.read_max_id > channel_read_state.read_max_id;
