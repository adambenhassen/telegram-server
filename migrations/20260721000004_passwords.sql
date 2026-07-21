-- Two-factor "cloud password" (SRP) state.
--
-- user_passwords is 1:1 with users; an absent row means the account has no 2FA.
-- The verifier is password-equivalent material and is encrypted at rest by the
-- store's keycrypt cipher, exactly like auth key values. p and g are fixed
-- Telegram protocol constants and are not stored.
CREATE TABLE user_passwords (
    user_id        BIGINT PRIMARY KEY REFERENCES users (id),
    salt1          BYTEA  NOT NULL,
    salt2          BYTEA  NOT NULL,
    verifier       BYTEA  NOT NULL,
    hint           TEXT   NOT NULL DEFAULT '',
    recovery_email TEXT   NULL,
    has_recovery   BOOL   NOT NULL DEFAULT false,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- pending_user_id is the half-authorized state between auth.signIn and
-- auth.checkPassword when the account has 2FA: signIn sets it (never user_id),
-- checkPassword promotes it to user_id on a valid SRP proof. It never grants
-- access on its own.
ALTER TABLE auth_keys ADD COLUMN pending_user_id BIGINT NULL REFERENCES users (id);
