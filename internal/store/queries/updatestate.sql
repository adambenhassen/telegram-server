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
RETURNING pts, next_local_id - 1 AS local_id;

-- BumpPtsOnly advances pts without consuming a local_id (edit/delete/read).
-- name: BumpPtsOnly :one
UPDATE update_state SET pts = pts + 1, date = now() WHERE user_id = $1 RETURNING pts;

-- name: EventsSince :many
SELECT * FROM message_events
WHERE owner_id = $1 AND pts > $2
ORDER BY pts;

-- name: InsertEvent :exec
INSERT INTO message_events (owner_id, pts, type, local_id) VALUES ($1, $2, $3, $4);
