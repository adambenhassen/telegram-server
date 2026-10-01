-- Private dialog filters are owner-scoped settings. The state row owns ordering
-- and the durable change marker, so it remains after the last folder is deleted.
CREATE TABLE user_dialog_filter_state (
    owner_id   BIGINT PRIMARY KEY REFERENCES users (id),
    order_ids  SMALLINT[] NOT NULL DEFAULT ARRAY[0]::SMALLINT[],
    changed_at TIMESTAMPTZ NULL
);

CREATE TABLE user_dialog_filters (
    owner_id         BIGINT NOT NULL REFERENCES users (id),
    filter_id        SMALLINT NOT NULL CHECK (filter_id BETWEEN 2 AND 255),
    title            TEXT NOT NULL,
    emoticon         TEXT NOT NULL DEFAULT '',
    color            SMALLINT NULL CHECK (color BETWEEN 0 AND 6),
    contacts         BOOLEAN NOT NULL DEFAULT false,
    non_contacts     BOOLEAN NOT NULL DEFAULT false,
    groups           BOOLEAN NOT NULL DEFAULT false,
    broadcasts       BOOLEAN NOT NULL DEFAULT false,
    bots             BOOLEAN NOT NULL DEFAULT false,
    exclude_muted    BOOLEAN NOT NULL DEFAULT false,
    exclude_read     BOOLEAN NOT NULL DEFAULT false,
    exclude_archived BOOLEAN NOT NULL DEFAULT false,
    title_noanimate  BOOLEAN NOT NULL DEFAULT false,
    PRIMARY KEY (owner_id, filter_id)
);

CREATE TABLE user_dialog_filter_entities (
    owner_id    BIGINT NOT NULL,
    filter_id   SMALLINT NOT NULL,
    entity_position SMALLINT NOT NULL CHECK (entity_position BETWEEN 0 AND 11),
    entity_offset   INTEGER NOT NULL CHECK (entity_offset >= 0),
    length      INTEGER NOT NULL CHECK (length > 0),
    document_id BIGINT NOT NULL CHECK (document_id > 0),
    PRIMARY KEY (owner_id, filter_id, entity_position),
    FOREIGN KEY (owner_id, filter_id)
        REFERENCES user_dialog_filters (owner_id, filter_id) ON DELETE CASCADE
);

CREATE TABLE user_dialog_filter_peers (
    owner_id   BIGINT NOT NULL,
    filter_id  SMALLINT NOT NULL,
    list_type  SMALLINT NOT NULL CHECK (list_type BETWEEN 1 AND 3),
    peer_type  SMALLINT NOT NULL CHECK (peer_type BETWEEN 1 AND 3),
    peer_id    BIGINT NOT NULL,
    peer_position SMALLINT NOT NULL CHECK (peer_position BETWEEN 0 AND 99),
    PRIMARY KEY (owner_id, filter_id, list_type, peer_position),
    UNIQUE (owner_id, filter_id, list_type, peer_type, peer_id),
    FOREIGN KEY (owner_id, filter_id)
        REFERENCES user_dialog_filters (owner_id, filter_id) ON DELETE CASCADE
);

CREATE INDEX user_dialog_filter_peers_peer_idx
    ON user_dialog_filter_peers (peer_type, peer_id, owner_id);
