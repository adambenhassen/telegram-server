-- Per-IP failure counter for auth.signIn.
--
-- auth.signIn is unauthenticated, so it has no account to key a limit on.
-- The subject is the network the connection came from (IPv4 /32 or IPv6 /64),
-- stored as CIDR. A failed verification from one IP does not affect another
-- IP's budget — the attacker's own network is what gets rate-limited.
--
-- This is a fixed-window counter, one row per key, mirroring send_code_ip_calls.
CREATE TABLE sign_in_fail_calls (
    ip_key       CIDR         NOT NULL PRIMARY KEY,
    token_count  INT          NOT NULL,
    window_start TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ  NOT NULL
);

-- Index for the sweep query that deletes expired rows.
CREATE INDEX sign_in_fail_calls_expires_at_idx ON sign_in_fail_calls (expires_at);
