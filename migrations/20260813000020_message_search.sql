-- Add full-text search support to messages table.
-- search_vector is a tsvector derived from the message text, used by messages.search.
-- The trigger keeps it in sync on INSERT and UPDATE.

ALTER TABLE messages ADD COLUMN search_vector tsvector GENERATED ALWAYS AS (to_tsvector('english', message)) STORED;

CREATE INDEX messages_search_vector_idx ON messages USING gin (search_vector);
