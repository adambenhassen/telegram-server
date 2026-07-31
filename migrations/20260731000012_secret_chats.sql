-- MAIN-137: secret chats schema.
--
-- Two new tables (secret_chats, encrypted_events) plus qts column on update_state.
-- All in one migration so schema is always consistent.

CREATE TABLE secret_chats (
    id              INT PRIMARY KEY,
    admin_id        BIGINT NOT NULL REFERENCES users(id),
    participant_id  BIGINT NOT NULL REFERENCES users(id),
    state           TEXT   NOT NULL CHECK (state IN ('requested', 'active', 'discarded')),
    g_a_hash        BYTEA,
    g_a             BYTEA,
    g_a_or_b        BYTEA,
    key_fingerprint BIGINT,
    date            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Server-allocated id sequence (int32 range per TL spec).
CREATE SEQUENCE secret_chats_id_seq START WITH 1 INCREMENT BY 1;

-- Per-account listing and cap query on outstanding requests.
CREATE INDEX secret_chats_admin_state_idx ON secret_chats (admin_id, state);
CREATE INDEX secret_chats_participant_state_idx ON secret_chats (participant_id, state);

CREATE TABLE encrypted_events (
    owner_id  BIGINT NOT NULL REFERENCES users(id),
    qts       BIGINT NOT NULL,
    chat_id   INT    NOT NULL REFERENCES secret_chats(id),
    random_id BIGINT NOT NULL,
    bytes     BYTEA  NOT NULL,
    date      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (owner_id, qts),
    UNIQUE (owner_id, random_id)
);

ALTER SEQUENCE secret_chats_id_seq OWNED BY secret_chats.id;

ALTER TABLE update_state ADD COLUMN qts BIGINT NOT NULL DEFAULT 0;
