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
    ON CONFLICT (phone) DO UPDATE SET phone = EXCLUDED.phone
    RETURNING id, phone, first_name, last_name, created_at, is_online, last_seen_at
)
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at,
       un.handle AS username
FROM upserted u
LEFT JOIN usernames un ON un.owner_type = 'user' AND un.owner_id = u.id;

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

-- name: SetUserStatus :execrows
UPDATE users SET is_online = $2, last_seen_at = now() WHERE id = $1;

-- name: SetUsername :execrows
UPDATE users SET username = $2 WHERE id = $1;

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
-- apply the tsquery as a filter, betting the page fills early. The bet loses on
-- every query that does not match most of the caller's dialog partners: the
-- walk reaches the end of the table, so a token nobody carries costs the whole
-- user set (52.7 ms at 200k users, against 0.28 ms here) and any authenticated
-- account picks that worst case for free by sending a nonsense token. Writing
-- the match as u.id IN (SELECT ...) the way the channel arms do is not enough
-- on its own — the planner flattens that subquery into a semi-join and takes
-- the same ordered scan (52.9 ms, measured) — so the set is fenced with AS
-- MATERIALIZED, which no plan can flatten. Both predicates sit inside the fence
-- so the planner still drives from whichever side is cheaper: the caller's
-- dialog index when the dialog set is small, users_name_tsv_idx when it is not.
--
-- What that costs is the early exit, the same trade the channel arms made. A
-- token nearly every user carries is now answered from the whole candidate set
-- rather than from the first rows of the id scan, so a caller holding a dialog
-- with every user goes 0.31 ms -> 413 ms on one. That case needs a token the
-- corpus shares; the case it replaces needed only a token nobody has.
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


