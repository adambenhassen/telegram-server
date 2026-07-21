-- Persistent MTProto auth keys. user_id is NULL until the key is bound to an
-- account during login; the FK keeps bound keys pointing at a real user.

CREATE TABLE auth_keys (
    id           BIGINT PRIMARY KEY,
    key_value    BYTEA  NOT NULL,
    user_id      BIGINT NULL REFERENCES users (id),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX auth_keys_user_id_idx ON auth_keys (user_id);
