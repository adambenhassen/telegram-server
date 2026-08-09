-- SearchGlobalPage returns one ordered page of cross-dialog search keys: which
-- peer each hit belongs to and its id in that peer's id space. It names rows
-- rather than returning them because the two arms read tables with different
-- shapes; the callers hydrate each arm through its own query.
--
-- The two arms are the two authorization predicates, and neither may be relaxed
-- to make the composition uniform. Owned rows (peer_type 1 = user, 2 = chat) are
-- filtered by owner_id: a chat keeps one copy per member, so ownership already
-- is the membership check. Channel posts keep one shared row per post, so they
-- carry the membership predicate explicitly, as an EXISTS over
-- channel_participants including the banned_until clause — a plain join on
-- membership would keep serving posts to a banned member.
--
-- Ordering is newest-first by (rate, peer, id), where rate is the message date
-- truncated to whole seconds — the same value offset_rate carries on the wire.
-- Truncating in the sort key rather than only in the emitted cursor is what
-- makes paging exact: a sub-second ordering the cursor cannot express would skip
-- every row that shares its second with the last row of a page. peer_type and
-- peer_id break ties across peers, whose local_id spaces are not comparable, and
-- local_id breaks the rest. The keyset is one row comparison against that whole
-- tuple, so a page resumes exactly where the previous one stopped.
--
-- Each arm carries the page limit of its own, so total work is bounded by
-- 2 * lim rows regardless of how many messages match.
-- name: SearchGlobalPage :many
WITH owned AS (
    SELECT FLOOR(EXTRACT(EPOCH FROM date))::bigint AS rate,
           peer_type                               AS peer_type,
           peer_id                                 AS peer_id,
           local_id                                AS msg_id
    FROM messages
    WHERE owner_id = sqlc.arg(owner_id)
      AND peer_type IN (1, 2)
      AND deleted = false
      AND message_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
      AND (sqlc.arg(has_cursor)::bool = false
           OR (FLOOR(EXTRACT(EPOCH FROM date))::bigint, peer_type, peer_id, local_id)
              < (sqlc.arg(offset_rate)::bigint, sqlc.arg(offset_peer_type)::smallint,
                 sqlc.arg(offset_peer_id)::bigint, sqlc.arg(offset_id)::bigint))
    ORDER BY rate DESC, peer_type DESC, peer_id DESC, msg_id DESC
    LIMIT sqlc.arg(lim)::int
), posts AS (
    SELECT FLOOR(EXTRACT(EPOCH FROM cm.date))::bigint AS rate,
           3::smallint                                AS peer_type,
           cm.channel_id                              AS peer_id,
           cm.local_id                                AS msg_id
    FROM channel_messages cm
    WHERE cm.deleted = false
      AND cm.message_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
      AND EXISTS (
          SELECT 1 FROM channel_participants cp
          WHERE cp.channel_id = cm.channel_id
            AND cp.user_id = sqlc.arg(owner_id)
            AND (cp.banned_until IS NULL OR cp.banned_until <= now())
      )
      AND (sqlc.arg(has_cursor)::bool = false
           OR (FLOOR(EXTRACT(EPOCH FROM cm.date))::bigint, 3::smallint, cm.channel_id, cm.local_id)
              < (sqlc.arg(offset_rate)::bigint, sqlc.arg(offset_peer_type)::smallint,
                 sqlc.arg(offset_peer_id)::bigint, sqlc.arg(offset_id)::bigint))
    ORDER BY rate DESC, peer_type DESC, peer_id DESC, msg_id DESC
    LIMIT sqlc.arg(lim)::int
)
SELECT rate, peer_type, peer_id, msg_id
FROM (SELECT * FROM owned UNION ALL SELECT * FROM posts) hits
ORDER BY rate DESC, peer_type DESC, peer_id DESC, msg_id DESC
LIMIT sqlc.arg(lim)::int;

-- OwnedMessagesByLocalIDs hydrates the owned arm of a search page. The owner_id
-- predicate is repeated here rather than trusted from the page query: this is
-- the query that reads message bodies, so it carries the predicate that decides
-- who may read them.
-- name: OwnedMessagesByLocalIDs :many
SELECT * FROM messages
WHERE owner_id = sqlc.arg(owner_id)
  AND local_id = ANY(sqlc.arg(local_ids)::bigint[])
  AND deleted = false;

