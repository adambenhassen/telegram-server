-- Channel service messages share the channel_messages id and pts streams with
-- posts. Existing writes remain ordinary messages through the zero default.
ALTER TABLE channel_messages
    ADD COLUMN action_type SMALLINT NOT NULL DEFAULT 0;
