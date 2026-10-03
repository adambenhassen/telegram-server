-- Reuse the fixed-depth summary lookup for every explicit unread surface.
-- Each suffix is decomposed into at most 63 exact (channel, scope, author,
-- depth, prefix) keys; no caller walks channel_messages to compute a count.
CREATE FUNCTION channel_post_unread_suffix_counts(
    p_channel_id BIGINT,
    p_viewer_id BIGINT,
    p_marker BIGINT
)
RETURNS TABLE (
    entitled BOOLEAN,
    status_exists BOOLEAN,
    summary_version SMALLINT,
    summary_ready BOOLEAN,
    total_live BIGINT,
    author_live BIGINT
)
LANGUAGE SQL
STABLE
AS $function$
WITH member AS MATERIALIZED (
    SELECT participant.user_id
    FROM channel_participants AS participant
    WHERE participant.channel_id = p_channel_id
      AND participant.user_id = p_viewer_id
      AND (participant.banned_until IS NULL OR participant.banned_until <= now())
), summary_state AS MATERIALIZED (
    SELECT state.version, state.ready
    FROM channel_post_summary_state AS state
    WHERE state.channel_id = p_channel_id
), suffix_keys AS MATERIALIZED (
    SELECT 0::SMALLINT AS depth, 0::BIGINT AS prefix
    WHERE p_marker = 0
    UNION ALL
    SELECT bits.depth::SMALLINT AS depth,
           (((p_marker >> (64 - bits.depth)) << 1) | 1)::BIGINT AS prefix
    FROM generate_series(1, 63) AS bits(depth)
    WHERE p_marker > 0
      AND ((p_marker >> (63 - bits.depth)) & 1) = 0
)
SELECT EXISTS (SELECT 1 FROM member) AS entitled,
       EXISTS (SELECT 1 FROM summary_state) AS status_exists,
       COALESCE((SELECT state.version FROM summary_state AS state), 0)::SMALLINT AS summary_version,
       COALESCE((SELECT state.ready FROM summary_state AS state), FALSE)::BOOLEAN AS summary_ready,
       COALESCE((
           SELECT sum((
               SELECT summary.live_count
               FROM channel_post_summaries AS summary
               WHERE summary.channel_id = p_channel_id
                 AND summary.scope_kind = 0
                 AND summary.author_id = 0
                 AND summary.depth = suffix_key.depth
                 AND summary.prefix = suffix_key.prefix
           ))::BIGINT
           FROM suffix_keys AS suffix_key
           WHERE EXISTS (SELECT 1 FROM member)
             AND EXISTS (SELECT 1 FROM summary_state WHERE version = 1 AND ready)
       ), 0)::BIGINT AS total_live,
       COALESCE((
           SELECT sum((
               SELECT summary.live_count
               FROM channel_post_summaries AS summary
               WHERE summary.channel_id = p_channel_id
                 AND summary.scope_kind = 1
                 AND summary.author_id = member.user_id
                 AND summary.depth = suffix_key.depth
                 AND summary.prefix = suffix_key.prefix
           ))::BIGINT
           FROM member
           CROSS JOIN suffix_keys AS suffix_key
           WHERE EXISTS (SELECT 1 FROM summary_state WHERE version = 1 AND ready)
       ), 0)::BIGINT AS author_live;
$function$;
