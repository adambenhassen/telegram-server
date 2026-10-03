ALTER TABLE chat_participants
    ADD COLUMN is_admin BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE chat_admin_events (
    id       BIGSERIAL PRIMARY KEY,
    chat_id  BIGINT  NOT NULL REFERENCES chats (id),
    user_id  BIGINT  NOT NULL REFERENCES users (id),
    is_admin BOOLEAN NOT NULL,
    version  INT     NOT NULL
);

-- Keep one current live recipient per member/chat/target. Pull markers are
-- stored separately so one connection's poll cannot suppress fanout to others.
CREATE TABLE chat_admin_event_recipients (
    owner_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    target_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL REFERENCES chat_admin_events (id) ON DELETE CASCADE,
    PRIMARY KEY (owner_id, chat_id, target_id),
    FOREIGN KEY (chat_id, owner_id)
        REFERENCES chat_participants (chat_id, user_id) ON DELETE CASCADE,
    FOREIGN KEY (chat_id, target_id)
        REFERENCES chat_participants (chat_id, user_id) ON DELETE CASCADE
);

CREATE INDEX chat_admin_event_recipients_event_owner_idx
    ON chat_admin_event_recipients (event_id, owner_id);
CREATE INDEX chat_admin_event_recipients_chat_target_idx
    ON chat_admin_event_recipients (chat_id, target_id);

-- Keep only the latest pending pull version per member/chat/target. A
-- successful getDifference consumes the matching event ID; membership
-- cascades clear the row when either party leaves.
CREATE TABLE chat_admin_state_markers (
    owner_id BIGINT NOT NULL,
    chat_id BIGINT NOT NULL,
    target_id BIGINT NOT NULL,
    event_id BIGINT NOT NULL REFERENCES chat_admin_events (id) ON DELETE CASCADE,
    PRIMARY KEY (owner_id, chat_id, target_id),
    FOREIGN KEY (chat_id, owner_id)
        REFERENCES chat_participants (chat_id, user_id) ON DELETE CASCADE,
    FOREIGN KEY (chat_id, target_id)
        REFERENCES chat_participants (chat_id, user_id) ON DELETE CASCADE
);

CREATE INDEX chat_admin_state_markers_owner_event_idx
    ON chat_admin_state_markers (owner_id, event_id);
CREATE INDEX chat_admin_state_markers_chat_target_idx
    ON chat_admin_state_markers (chat_id, target_id);
