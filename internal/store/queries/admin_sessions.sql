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

-- name: UpdateAdminSessionActivity :exec
-- Touch the last_activity timestamp for an existing session.
UPDATE admin_sessions
SET last_activity = $2
WHERE session_hash = $1;

-- name: SweepExpiredAdminSessions :execrows
-- Delete session rows whose absolute expiry deadline has passed.
DELETE FROM admin_sessions
WHERE expires_at < now();
