-- A poll is canonical across the per-owner copies of its message. Votes point
-- to that one row, not to each copy, so replacement and counts share one state.
CREATE TABLE polls (
    id                  BIGINT PRIMARY KEY CHECK (id > 0),
    creator_id          BIGINT NOT NULL REFERENCES users (id),
    random_id           BIGINT NOT NULL DEFAULT 0,
    source_local_id     BIGINT NOT NULL,
    question            BYTEA NOT NULL CHECK (octet_length(question) BETWEEN 1 AND 4096),
    public_voters       BOOL NOT NULL DEFAULT false,
    multiple_choice     BOOL NOT NULL DEFAULT false,
    quiz                BOOL NOT NULL DEFAULT false,
    shuffle_answers     BOOL NOT NULL DEFAULT false,
    revoting_disabled   BOOL NOT NULL DEFAULT false,
    closed              BOOL NOT NULL DEFAULT false,
    close_date          TIMESTAMPTZ,
    solution            BYTEA,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (NOT quiz OR revoting_disabled)
);

-- The source message's random_id is the create retry key. Zero keeps the
-- existing messaging convention that a zero random_id disables deduplication.
CREATE UNIQUE INDEX polls_creator_random_id_idx
    ON polls (creator_id, random_id) WHERE random_id <> 0;

CREATE TABLE poll_options (
    poll_id     BIGINT NOT NULL REFERENCES polls (id) ON DELETE CASCADE,
    option      BYTEA NOT NULL CHECK (octet_length(option) BETWEEN 1 AND 100),
    text        BYTEA NOT NULL CHECK (octet_length(text) BETWEEN 1 AND 4096),
    correct     BOOL NOT NULL DEFAULT false,
    position    SMALLINT NOT NULL CHECK (position BETWEEN 0 AND 9),
    PRIMARY KEY (poll_id, option),
    UNIQUE (poll_id, position)
);

CREATE TABLE poll_votes (
    poll_id       BIGINT NOT NULL REFERENCES polls (id) ON DELETE CASCADE,
    voter_id      BIGINT NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    first_voted_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (poll_id, voter_id)
);

CREATE TABLE poll_vote_options (
    poll_id   BIGINT NOT NULL,
    voter_id  BIGINT NOT NULL,
    option    BYTEA NOT NULL,
    PRIMARY KEY (poll_id, voter_id, option),
    FOREIGN KEY (poll_id, voter_id)
        REFERENCES poll_votes (poll_id, voter_id) ON DELETE CASCADE,
    FOREIGN KEY (poll_id, option)
        REFERENCES poll_options (poll_id, option) ON DELETE CASCADE
);

-- One row for every stored message copy. The shared poll_id preserves one
-- identity across sender and recipient copies and across a chat fan-out.
CREATE TABLE poll_message_copies (
    owner_id  BIGINT NOT NULL,
    local_id  BIGINT NOT NULL,
    poll_id   BIGINT NOT NULL REFERENCES polls (id) ON DELETE CASCADE,
    PRIMARY KEY (owner_id, local_id),
    FOREIGN KEY (owner_id, local_id)
        REFERENCES messages (owner_id, local_id) ON DELETE CASCADE
);

CREATE INDEX poll_message_copies_poll_idx ON poll_message_copies (poll_id);

-- Soft-deleting a copy removes only that copy's poll reference. Keep the poll
-- and its votes while any other owner's retained message copy still exists.
CREATE FUNCTION poll_message_copy_cleanup()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    removed_poll_id BIGINT;
BEGIN
    IF NOT OLD.deleted AND NEW.deleted THEN
        SELECT poll_id INTO removed_poll_id
          FROM poll_message_copies
         WHERE owner_id = NEW.owner_id AND local_id = NEW.local_id;

        IF removed_poll_id IS NOT NULL THEN
            -- Every cleanup locks the canonical row before removing a copy
            -- reference. Concurrent last-copy deletes then observe each
            -- other's committed removal in sequence instead of both keeping
            -- the poll because the other reference is still uncommitted.
            PERFORM 1 FROM polls WHERE id = removed_poll_id FOR UPDATE;

            DELETE FROM poll_message_copies
             WHERE owner_id = NEW.owner_id
               AND local_id = NEW.local_id
               AND poll_id = removed_poll_id;

            DELETE FROM polls p
             WHERE p.id = removed_poll_id
               AND NOT EXISTS (
                   SELECT 1 FROM poll_message_copies c WHERE c.poll_id = p.id
               );
        END IF;
    END IF;
    RETURN NEW;
END;
$$;

CREATE TRIGGER messages_poll_copy_cleanup
AFTER UPDATE OF deleted ON messages
FOR EACH ROW
EXECUTE FUNCTION poll_message_copy_cleanup();
