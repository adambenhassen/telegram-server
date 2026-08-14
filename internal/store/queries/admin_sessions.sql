-- name: InsertAdminSession :exec
-- Insert a new admin session row. Returns an error if a session with the
-- same hash already exists (unique primary key).
INSERT INTO admin_sessions (session_hash, token_fingerprint, expires_at, last_activity)
VALUES ($1, $2, $3, $4);

-- name: GetAdminSession :one
-- Look up a session by its SHA-256 hash. Returns the row's fingerprint,
-- expiry, and last-activity timestamp.
SELECT token_fingerprint, expires_at, last_activity
FROM admin_sessions
WHERE session_hash = $1;

-- name: UpdateAdminSessionActivity :execrows
-- Touch the last_activity timestamp for an existing session. Uses GREATEST
-- so concurrent requests committing out of order never move the clock
-- backwards and trigger a spurious idle-timeout. Returns rows affected
-- (0 means the session was deleted between lookup and update).
UPDATE admin_sessions
SET last_activity = GREATEST(last_activity, $2)
WHERE session_hash = $1;

-- name: DeleteAdminSession :execrows
-- Delete a single admin session row by its hash. Returns rows affected
-- (0 means the session was already gone).
DELETE FROM admin_sessions
WHERE session_hash = $1;

-- name: SweepExpiredAdminSessions :execrows
-- Delete session rows whose absolute expiry deadline has passed.
DELETE FROM admin_sessions
WHERE expires_at < now();
