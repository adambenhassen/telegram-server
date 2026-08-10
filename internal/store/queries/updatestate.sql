-- name: EnsureUpdateState :exec
INSERT INTO update_state (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING;

-- name: GetState :one
SELECT * FROM update_state WHERE user_id = $1;

-- BumpState allocates the next local_id and advances pts in one step. It returns
-- the new pts and the local_id just consumed (pre-increment value).
-- name: BumpState :one
UPDATE update_state
SET pts = pts + 1, next_local_id = next_local_id + 1, date = now()
WHERE user_id = $1
RETURNING pts, (next_local_id - 1)::bigint AS local_id;

-- BumpPtsOnly advances pts without consuming a local_id (edit/delete/read).
-- name: BumpPtsOnly :one
UPDATE update_state SET pts = pts + 1, date = now() WHERE user_id = $1 RETURNING pts;

-- name: EventsSince :many
SELECT * FROM message_events
WHERE owner_id = $1 AND pts > $2
ORDER BY pts;

-- EventsWindow returns events in (from_pts, to_pts] ordered, capped by lim. The
-- upper bound pins the read to a pts snapshot so the difference never advertises
-- a pts past an event it omitted.
-- name: EventsWindow :many
SELECT * FROM message_events
WHERE owner_id = sqlc.arg(owner_id) AND pts > sqlc.arg(from_pts) AND pts <= sqlc.arg(to_pts)
ORDER BY pts
LIMIT sqlc.arg(lim)::int;

-- NewMessagePts returns the pts at which owner's local_id entered the log, for
-- a resend that has to name the pts its stored message already occupies rather
-- than the owner's current one. Exactly one row can match: a message is written
-- with its new-message event in one transaction and neither is ever rewritten.
--
-- type = 1 (new message) is a literal, not a parameter, because the partial
-- index serving this read is predicated on it and a parameter cannot be proven
-- against that predicate.
-- name: NewMessagePts :one
SELECT pts FROM message_events
WHERE owner_id = $1 AND local_id = $2 AND type = 1;

-- name: InsertEvent :exec
INSERT INTO message_events (owner_id, pts, type, local_id) VALUES ($1, $2, $3, $4);
