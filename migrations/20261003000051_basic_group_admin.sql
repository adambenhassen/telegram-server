ALTER TABLE chat_participants
    ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE chat_admin_events (
    id       BIGSERIAL PRIMARY KEY,
    chat_id  BIGINT  NOT NULL REFERENCES chats (id),
    user_id  BIGINT  NOT NULL REFERENCES users (id),
    is_admin BOOLEAN NOT NULL,
    version  INT     NOT NULL
);
