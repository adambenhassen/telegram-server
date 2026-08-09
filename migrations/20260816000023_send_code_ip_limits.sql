-- M15 per-IP control on auth.sendCode.
--
-- auth.sendCode is unauthenticated, so it has no account to key a limit on and
-- the existing rate_limits table (subject_id BIGINT) cannot hold its subject.
-- The subject here is the network the connection came from: an IPv4 host (/32)
-- or the /64 an IPv6 host rotates addresses inside for free. Stored as CIDR so
-- Postgres owns the canonical form rather than a Go-side string spelling.
--
-- Two counters share that one key, and both are bounded by the quota rather
-- than by request count: this is an unauthenticated write path, so nothing here
-- may grow a row per call.

-- send_code_ip_calls is a fixed-window counter, one row per key regardless of
-- how many calls land in the window. token_count is reset and bumped by an
-- upsert.
--
-- expires_at is the window: it is written when the window opens and is the only
-- thing that says when it closes, for the upsert and for the sweep alike.
-- Neither recomputes the boundary from window_start plus the configured
-- duration, because the configured duration can change while a row is live and
-- the two readers would then disagree about the same row. window_start is kept
-- for diagnosis — with expires_at it shows the duration a live window opened
-- under, which is what a config change makes differ from the current setting.
CREATE TABLE send_code_ip_calls (
    ip_key       CIDR         NOT NULL PRIMARY KEY,
    token_count  INT          NOT NULL,
    window_start TIMESTAMPTZ  NOT NULL DEFAULT now(),
    expires_at   TIMESTAMPTZ  NOT NULL
);

-- Index for the sweep query that deletes expired rows.
CREATE INDEX send_code_ip_calls_expires_at_idx ON send_code_ip_calls (expires_at);

-- send_code_ip_phones tracks which phone numbers a key has already requested a
-- code for, so distinct numbers can be counted. One row per (key, number), and
-- no row is inserted once the key is already at its quota, so rows per key are
-- bounded by the quota itself.
--
-- These rows join a network to a phone number, which is personal data: the row
-- carries its own expiry and is deleted by prune-on-write plus the sweep, so
-- retention is the limit window and nothing beyond it.
CREATE TABLE send_code_ip_phones (
    ip_key     CIDR        NOT NULL,
    phone      TEXT        NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (ip_key, phone)
);

-- Index for the sweep query that deletes expired rows.
CREATE INDEX send_code_ip_phones_expires_at_idx ON send_code_ip_phones (expires_at);
