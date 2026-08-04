-- M9 secret chats: encrypted message events plus qts watermark in update_state.
--
-- encrypted_events stores incoming secret-chat messages per recipient. Unlike
-- message_events, these carry a raw encrypted blob and a random_id (the sender's
-- dedup token). The qts column in update_state tracks the highest qts the
-- recipient has acknowledged via messages.receivedQueue.
--
-- receivedQueue deletes rows with qts <= clamped_max_qts and returns their
-- random_id values. Deletion avoids unbounded row growth and matches the
-- Telegram semantics of receivedQueue as an acknowledgement watermark.

-- encrypted_chats maps a secret-chat id to its two participants. Both sides share
-- the same chat id and access_hash, so either participant can look up the other.
CREATE TABLE encrypted_chats (
    id           INT PRIMARY KEY,
    access_hash  BIGINT NOT NULL,
    user1_id     BIGINT NOT NULL REFERENCES users (id),
    user2_id     BIGINT NOT NULL REFERENCES users (id),
    UNIQUE (user1_id, user2_id)
);

ALTER TABLE update_state ADD COLUMN qts BIGINT NOT NULL DEFAULT 0;

CREATE TABLE encrypted_events (
    owner_id  BIGINT NOT NULL REFERENCES users (id),
    qts       BIGINT NOT NULL,
    random_id BIGINT NOT NULL,
    data      BYTEA  NOT NULL,
    PRIMARY KEY (owner_id, qts)
);
