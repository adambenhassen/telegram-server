-- Admin session storage for requireAdmin middleware.
--
-- Each row represents one active admin session. The session id itself is never
-- stored in plaintext — only its SHA-256 hash, so a database leak does not
-- expose session tokens. The token_fingerprint is a SHA-256 of TG_ADMIN_TOKEN_HASH
-- at session-creation time; changing the env var invalidates all existing sessions
-- on the next request.
CREATE TABLE admin_sessions (
    session_hash   BYTEA        NOT NULL PRIMARY KEY,
    token_fingerprint BYTEA     NOT NULL,
    expires_at     TIMESTAMPTZ  NOT NULL,
    last_activity  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Index for the sweep query that deletes expired rows.
CREATE INDEX admin_sessions_expires_at_idx ON admin_sessions (expires_at);
