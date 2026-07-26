-- M6 group chats: chat peer type, membership, and the fan-out linkage.

CREATE TABLE chats (
    id          BIGSERIAL PRIMARY KEY,
    title       TEXT   NOT NULL,
    creator_id  BIGINT NOT NULL REFERENCES users (id),
    version     INT    NOT NULL DEFAULT 1,  -- tg.Chat.Version; bumps on membership or title change
    date        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE chat_participants (
    chat_id    BIGINT NOT NULL REFERENCES chats (id),
    user_id    BIGINT NOT NULL REFERENCES users (id),
    inviter_id BIGINT NOT NULL,
    date       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chat_id, user_id)
);

-- "which chats is this user in" drives every fan-out and the dialog list.
CREATE INDEX chat_participants_user_idx ON chat_participants (user_id);

-- peer_type discriminates the peer_id namespace: 1 = user, 2 = chat. Chat ids and
-- user ids come from different sequences and CAN collide numerically, so peer_id
-- alone is ambiguous and every predicate on it must carry peer_type.
--
-- fanout_id links every per-member copy of one chat message (the N-way
-- generalisation of peer_local_id, which stays as-is for 1:1). 0 = not a chat message.
--
-- action_type: 0 plain text, 1 chat_create, 2 chat_add_user, 3 chat_delete_user,
-- 4 chat_edit_title. For 1 and 4 the title lives in the existing `message` column.
ALTER TABLE messages
    ADD COLUMN peer_type      SMALLINT NOT NULL DEFAULT 1,
    ADD COLUMN fanout_id      BIGINT   NOT NULL DEFAULT 0,
    ADD COLUMN action_type    SMALLINT NOT NULL DEFAULT 0,
    ADD COLUMN action_user_id BIGINT   NOT NULL DEFAULT 0;

CREATE SEQUENCE message_fanout_seq;

CREATE INDEX messages_fanout_idx ON messages (fanout_id) WHERE fanout_id <> 0;

DROP INDEX messages_owner_peer_idx;
CREATE INDEX messages_owner_peer_idx ON messages (owner_id, peer_type, peer_id, local_id);

ALTER TABLE dialogs ADD COLUMN peer_type SMALLINT NOT NULL DEFAULT 1;
ALTER TABLE dialogs DROP CONSTRAINT dialogs_pkey;
ALTER TABLE dialogs ADD PRIMARY KEY (owner_id, peer_type, peer_id);
