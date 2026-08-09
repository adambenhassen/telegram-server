-- name: CreateUser :one
INSERT INTO users (phone) VALUES ($1)
ON CONFLICT (phone) DO UPDATE SET phone = EXCLUDED.phone
RETURNING *;

-- name: UserByPhone :one
SELECT * FROM users WHERE phone = $1;

-- name: UserByID :one
SELECT * FROM users WHERE id = $1;

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
SELECT u.id, u.phone, u.first_name, u.last_name, u.created_at, u.is_online, u.last_seen_at, u.username
FROM users u
WHERE u.name_tsv @@ plainto_tsquery('simple', sqlc.arg(query))
  AND EXISTS (
      SELECT 1 FROM dialogs d
      WHERE d.owner_id = sqlc.arg(owner_id)::bigint
        AND d.peer_id = u.id
        AND d.peer_type = 1
  )
ORDER BY u.id
LIMIT sqlc.arg(lim)::int;


