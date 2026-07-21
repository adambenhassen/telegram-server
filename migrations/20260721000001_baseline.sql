-- Baseline schema: re-expresses the Milestone 1 schema (users + phone_codes).
-- Atlas tracks which migrations have been applied, so no IF NOT EXISTS guards.

CREATE TABLE users (
    id         BIGSERIAL PRIMARY KEY,
    phone      TEXT NOT NULL UNIQUE,
    first_name TEXT NOT NULL DEFAULT '',
    last_name  TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE phone_codes (
    phone      TEXT PRIMARY KEY,
    code_hash  TEXT NOT NULL,
    code       TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
