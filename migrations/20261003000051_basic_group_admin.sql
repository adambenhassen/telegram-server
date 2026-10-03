ALTER TABLE chat_participants
    ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE chat_admin_events (
    id       BIGSERIAL PRIMARY KEY,
    chat_id  BIGINT  NOT NULL REFERENCES chats (id),
    user_id  BIGINT  NOT NULL REFERENCES users (id),
    is_admin BOOLEAN NOT NULL,
    version  INT     NOT NULL
);

-- Each event records the membership snapshot that was entitled to receive it.
-- The difference path uses these durable, non-pts markers to replay current
-- admin state after a transient live notification is missed.
CREATE TABLE chat_admin_event_recipients (
    event_id BIGINT NOT NULL REFERENCES chat_admin_events (id),
    owner_id BIGINT NOT NULL REFERENCES users (id),
    PRIMARY KEY (event_id, owner_id)
);

CREATE INDEX chat_admin_event_recipients_owner_event_idx
    ON chat_admin_event_recipients (owner_id, event_id);
