-- SearchGlobalPage returns one ordered page of cross-dialog search keys: which
-- peer each hit belongs to, its id in that peer's id space, and its date. It
-- names rows rather than returning them because the two arms read tables with
-- different shapes; the callers hydrate each arm through its own query.
--
-- The two arms are the two authorization predicates, and neither may be relaxed
-- to make the composition uniform. Owned rows (peer_type 1 = user, 2 = chat) are
-- filtered by owner_id: a chat keeps one copy per member, so ownership already
-- is the membership check. Channel posts keep one shared row per post, so they
-- carry the membership predicate explicitly, as an EXISTS over
-- channel_participants including the banned_until clause — a plain join on
-- membership would keep serving posts to a banned member.
--
-- channel_ids is a scope, never the authorization. It carries the caller's own
-- unbanned channel ids, read separately off the index on channel_participants
-- (user_id), and exists so the planner cannot choose a plan that reads the whole
-- channel_messages table: with only the EXISTS to go on it may satisfy the
-- semi-join after the fact, and at a frequent term — plainto_tsquery('simple')
-- strips no stopwords, so the caller picks the frequency — that plan is a
-- parallel sequential scan of every post on the server. The EXISTS stays and
-- stays authoritative: a stale or wrong id list can only narrow this arm, never
-- widen it past the membership the EXISTS re-checks in the same statement.
--
-- Ordering is newest-first by (date, peer, id), on the full stored timestamp
-- rather than on the whole second offset_rate carries. That is what keeps a page
-- sequence stable: a message arriving after the sequence started always has a
-- date past the cursor row's, so it can only surface at the newest end of a
-- fresh search, never slip in mid-sequence with a lower peer tuple in the same
-- second. peer_type and peer_id break ties across peers, whose local_id spaces
-- are not comparable, and local_id breaks the rest.
--
-- The keyset arrives as a tie window rather than a single value: rows older than
-- tie_lo are strictly behind the cursor, rows within [tie_lo, tie_hi] are ties
-- broken by the peer tuple, rows past tie_hi are ahead of it. The caller sets
-- both ends to the cursor row's own timestamp when it can read that row, which
-- is the exact keyset; when the cursor names a row that does not exist it widens
-- the window to the whole second offset_rate names, which is all a made-up
-- cursor can be resolved to.
--
-- Each arm carries a limit of its own, so at most 2 * lim rows reach the merge
-- and the hydration behind it. That bounds the reply and everything downstream
-- of this query. It does not bound the match: each arm still top-N sorts every
-- row its predicate matches, so the worst case is the caller's own corpus —
-- their messages, and the posts of the channels they belong to — and never the
-- whole server's.
-- name: SearchGlobalPage :many
WITH owned AS (
    SELECT date      AS date,
           peer_type AS peer_type,
           peer_id   AS peer_id,
           local_id  AS msg_id
    FROM messages
    WHERE owner_id = sqlc.arg(owner_id)
      AND peer_type IN (1, 2)
      AND deleted = false
      AND message_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
      AND (sqlc.arg(has_cursor)::bool = false
           OR date < sqlc.arg(tie_lo)::timestamptz
           OR (date <= sqlc.arg(tie_hi)::timestamptz
               AND (peer_type, peer_id, local_id)
                   < (sqlc.arg(offset_peer_type)::smallint, sqlc.arg(offset_peer_id)::bigint,
                      sqlc.arg(offset_id)::bigint)))
    ORDER BY date DESC, peer_type DESC, peer_id DESC, msg_id DESC
    LIMIT sqlc.arg(lim)::int
), posts AS (
    SELECT cm.date        AS date,
           3::smallint    AS peer_type,
           cm.channel_id  AS peer_id,
           cm.local_id    AS msg_id
    FROM channel_messages cm
    WHERE cm.channel_id = ANY(sqlc.arg(channel_ids)::bigint[])
      AND cm.deleted = false
      AND cm.message_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
      AND EXISTS (
          SELECT 1 FROM channel_participants cp
          WHERE cp.channel_id = cm.channel_id
            AND cp.user_id = sqlc.arg(owner_id)
            AND (cp.banned_until IS NULL OR cp.banned_until <= now())
      )
      AND (sqlc.arg(has_cursor)::bool = false
           OR cm.date < sqlc.arg(tie_lo)::timestamptz
           OR (cm.date <= sqlc.arg(tie_hi)::timestamptz
               AND (3::smallint, cm.channel_id, cm.local_id)
                   < (sqlc.arg(offset_peer_type)::smallint, sqlc.arg(offset_peer_id)::bigint,
                      sqlc.arg(offset_id)::bigint)))
    ORDER BY date DESC, peer_type DESC, peer_id DESC, msg_id DESC
    LIMIT sqlc.arg(lim)::int
)
SELECT date, peer_type, peer_id, msg_id
FROM (SELECT * FROM owned UNION ALL SELECT * FROM posts) hits
ORDER BY date DESC, peer_type DESC, peer_id DESC, msg_id DESC
LIMIT sqlc.arg(lim)::int;

-- MemberChannelIDs lists the channels the caller is currently an unbanned member
-- of. It backs the scope in SearchGlobalPage and nothing else may treat it as an
-- authorization: it is read outside the search statement, so a membership can
-- end between this and the page query, which is exactly why the page query
-- re-checks membership itself.
-- name: MemberChannelIDs :many
SELECT channel_id FROM channel_participants
WHERE user_id = sqlc.arg(user_id)
  AND (banned_until IS NULL OR banned_until <= now())
ORDER BY channel_id;

-- OwnedMessageDate reads the timestamp of one of the caller's own rows, deleted
-- rows included: this resolves a pagination cursor, and a sequence whose last
-- served row was deleted mid-sequence must still resume exactly where it left
-- off rather than fall back to a whole-second window.
-- name: OwnedMessageDate :one
SELECT date FROM messages WHERE owner_id = $1 AND local_id = $2;

-- ChannelPostDate is OwnedMessageDate for a channel cursor. The caller's right
-- to hold this cursor is settled before it runs — a channel cursor is refused
-- unless the caller is an unbanned member — so this reads no more than the
-- search itself would serve.
-- name: ChannelPostDate :one
SELECT date FROM channel_messages WHERE channel_id = $1 AND local_id = $2;

-- OwnedMessagesByLocalIDs hydrates the owned arm of a search page. The owner_id
-- predicate is repeated here rather than trusted from the page query: this is
-- the query that reads message bodies, so it carries the predicate that decides
-- who may read them.
-- name: OwnedMessagesByLocalIDs :many
SELECT * FROM messages
WHERE owner_id = sqlc.arg(owner_id)
  AND local_id = ANY(sqlc.arg(local_ids)::bigint[])
  AND deleted = false;
