-- Every query that loads a user reads the handle from the usernames row, never
-- from the denormalized users.username column. The two are written in one
-- transaction today, but only the usernames row is authoritative: it is what
-- admits an account to resolveUsername, and a writer that released a handle
-- without clearing the copy must not leave any RPC still reporting it. The join
-- is a nested loop on usernames_owner_idx (owner_type, owner_id), 0.11 ms for a
-- user lookup against 200k handles, so the hot per-peer read pays an index
-- probe rather than the copy's zero.

-- name: CreateUser :one
WITH upserted AS (
    INSERT INTO users (phone) VALUES ($1)
    ON CONFLICT (phone) WHERE phone IS NOT NULL DO UPDATE SET phone = EXCLUDED.phone
    RETURNING id, phone, first_name, last_name, created_at, is_online, last_seen_at
)
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       un.handle AS username
FROM upserted u
LEFT JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id;

-- name: CreateUsernameUser :one
INSERT INTO users (phone, login_mode, first_name, last_name)
VALUES (NULL, 'username', $1, $2)
RETURNING id, phone, first_name, last_name, created_at, is_online, last_seen_at;

-- name: UserByPhone :one
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       un.handle AS username
FROM users u
LEFT JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE u.phone = $1;

-- name: UserByID :one
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       un.handle AS username
FROM users u
LEFT JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE u.id = $1;

-- name: UsersByID :many
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       un.handle AS username
FROM users u
LEFT JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE u.id = ANY(sqlc.arg(ids)::bigint[]);

-- name: EntitledUserIDs :many
-- Returns the subset of ids the viewer is entitled to see live. An id is
-- entitled iff any of the four live edges holds:
--   1. the id is the viewer themself;
--   2. the two share a 1:1 dialog row (peer_type = 1);
--   3. both are current participants of some chat;
--   4. both are current members of some channel, and neither is banned at now().
-- The channel edge requires the VIEWER's own row to be unbanned as well: a
-- banned viewer is not a current member, so the channel admits nothing for
-- them. The ban predicate is the same one ChannelMember.Banned uses in Go:
-- banned_until IS NULL OR banned_until <= now().
SELECT id FROM (
    SELECT sqlc.arg(ids)::bigint[] AS id_arr
) seed, UNNEST(seed.id_arr) AS id
WHERE id = sqlc.arg(viewer_id)::bigint
   OR EXISTS (
       SELECT 1 FROM dialogs d
       WHERE d.peer_type = 1
         AND ((d.owner_id = sqlc.arg(viewer_id)::bigint AND d.peer_id = id)
           OR (d.owner_id = id AND d.peer_id = sqlc.arg(viewer_id)::bigint))
   )
   OR EXISTS (
       SELECT 1 FROM chat_participants p1
       JOIN chat_participants p2 ON p1.chat_id = p2.chat_id
       WHERE p1.user_id = sqlc.arg(viewer_id)::bigint
         AND p2.user_id = id
   )
   OR EXISTS (
       SELECT 1 FROM channel_participants v
       JOIN channel_participants m ON v.channel_id = m.channel_id
       WHERE v.user_id = sqlc.arg(viewer_id)::bigint
         AND m.user_id = id
         AND (v.banned_until IS NULL OR v.banned_until <= now())
         AND (m.banned_until IS NULL OR m.banned_until <= now())
   );

-- name: SetUserStatus :execrows
UPDATE users SET is_online = $2, last_seen_at = now() WHERE id = $1;

-- name: UpdateUserProfile :execrows
-- A nil first_name or last_name leaves that column unchanged. An empty string
-- is a real write: signup accepts an empty display name, so the update path
-- must too.
UPDATE users
SET first_name = COALESCE(sqlc.narg('first_name'), first_name),
    last_name = COALESCE(sqlc.narg('last_name'), last_name)
WHERE id = sqlc.arg('id');

-- name: SetUsername :execrows
UPDATE users SET username = $2 WHERE id = $1;

-- name: GetUserLoginMode :one
SELECT login_mode FROM users WHERE id = sqlc.arg(id);

-- name: SearchContactsByName :many
-- Search users by name within the caller's existing dialogs.
-- Only users with whom the caller has exchanged messages (has a dialog row) are returned.
-- The single owner_id arm is sufficient: sending a message writes two dialog rows
-- in one transaction (caller-owned and peer-owned), so the caller always has a
-- row where owner_id = caller. The second arm (peer_id = caller) was removed
-- because it authorizes off rows the caller does not own — a future history-deletion
-- path could delete the peer-owned row without deleting the caller-owned one,
-- making the second arm incorrectly permissive. It also costs ~618 ms vs 0.10 ms
-- (seq-scan vs index-only) at 200k users.
--
-- ORDER BY u.id is what makes the page deterministic. MyResults unions this arm
-- with the caller's matching channels under ONE limit budget, so rows past the
-- budget are dropped; without a total order the set that survives truncation is
-- whatever the plan happened to emit, and two identical searches disagree. It
-- is the same order the channel arms use.
--
-- The candidate set is computed in a MATERIALIZED CTE, and the fence is the
-- whole shape rather than a formatting choice. With both predicates in the
-- WHERE clause, ORDER BY u.id LIMIT lets the planner walk users in id order and
-- apply the tsquery as a filter, betting the page fills early. It takes that
-- bet once the caller's dialog set is too large to be the selective side of the
-- join — at 200k users the walk showed up for a caller with 20k dialog partners
-- and not for one with 1k — and then loses it on every query that does not
-- match densely: the walk reaches the end of the table, so a token nobody
-- carries costs the whole user set (52.7 ms, against 0.28 ms here). This is not
-- a worst case any account can reach by sending a nonsense token, the way the
-- public channel arm's was; a caller with few dialogs was never on that plan.
-- It is one a caller with many dialogs paid on nearly every search they ran,
-- the ones that matched included. Writing the match as u.id IN (SELECT ...) the
-- way the channel arms do is not enough on its own — the planner flattens that
-- subquery into a semi-join and takes the same ordered scan (52.9 ms, measured)
-- — so the set is fenced with AS MATERIALIZED, which no plan can flatten. Both
-- predicates sit inside the fence so the planner still drives from whichever
-- side is cheaper: the caller's dialog index when the dialog set is small,
-- users_name_tsv_idx when it is not.
--
-- What that costs is the early exit, the same trade the channel arms made. A
-- token nearly every user carries is now answered from the whole candidate set
-- rather than from the first rows of the id scan, so a caller holding a dialog
-- with every user goes 0.31 ms -> 413 ms on one. It is not confined to the
-- callers the fence rescues, and not to the densest tokens either: a caller
-- with 1k partners, who was never on the walk and so had nothing rescued, pays
-- it from 1% match density upward — 5.14 -> 5.32 ms at 1%, 6.95 -> 16.6 ms at
-- 10%, 0.39 -> 8.63 ms at 100% — bounded around 17 ms there, and that is where
-- a latency report would come from. Smaller dialog sets stay flat: at 50
-- partners every density measured lands under 1.6 ms.
WITH matched AS MATERIALIZED (
    SELECT tu.id
    FROM users tu
    WHERE tu.name_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
      AND EXISTS (
          SELECT 1 FROM dialogs d
          WHERE d.owner_id = sqlc.arg(owner_id)::bigint
            AND d.peer_id = tu.id
            AND d.peer_type = 1
      )
)
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       un.handle AS username
FROM users u
LEFT JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id
WHERE u.id IN (SELECT id FROM matched)
ORDER BY u.id
LIMIT sqlc.arg(lim)::int;


