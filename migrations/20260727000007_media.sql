-- M5 media & files: uploaded file metadata, in-flight upload parts, and the
-- messages.file_id linkage that is also the download authorization gate.

CREATE TABLE files (
    id          BIGSERIAL PRIMARY KEY,
    uploader_id BIGINT NOT NULL REFERENCES users (id),
    -- 64 random bits from crypto/rand, drawn per row and never 0. Deliberately
    -- NOT the peer access_hash placeholder (access_hash == user_id), which is
    -- satisfiable by construction; this one is not derived from anything.
    access_hash BIGINT NOT NULL,
    size        BIGINT NOT NULL,
    mime_type   TEXT   NOT NULL,
    file_name   TEXT   NOT NULL,
    -- The blob body has been written. A row is created before the bytes are
    -- stored, so only stored = true rows are downloadable: a crashed assembly
    -- leaves an unreachable row rather than a file id that serves garbage.
    stored      BOOL   NOT NULL DEFAULT false,
    date        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- "how many bytes has this account uploaded" — the per-account storage cap
-- sums size over this index at assembly time.
CREATE INDEX files_uploader_idx ON files (uploader_id);

-- In-flight upload parts, held in Postgres rather than the blob store so an
-- abandoned upload is a DELETE and never a filesystem reconciliation.
--
-- file_id is CLIENT-chosen (upload.saveFilePart names it) and is deliberately
-- NOT a foreign key to files.id: the two id spaces are unrelated. The user_id
-- in the primary key is what makes it safe — without it, an account could name
-- another account's in-flight file_id and write its own bytes into that
-- upload, which would then be assembled and sent under the victim's identity.
CREATE TABLE upload_parts (
    user_id    BIGINT NOT NULL REFERENCES users (id),
    file_id    BIGINT NOT NULL,
    part_index INT    NOT NULL,
    payload    BYTEA  NOT NULL,
    date       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, file_id, part_index)
);

-- The TTL sweeper deletes by age; it is the only delete in M5 that removes
-- bytes, and it only ever removes parts no message references.
CREATE INDEX upload_parts_date_idx ON upload_parts (date);

-- 0 = no media. A media message is an ordinary message row carrying a file id.
ALTER TABLE messages ADD COLUMN file_id BIGINT NOT NULL DEFAULT 0;

-- The download authorization gate: upload.getFile serves a file only if the
-- caller owns a non-deleted messages row naming it. Because messages is
-- per-owner (one row per entitled account, and one per member on a chat
-- fan-out), the entitled set is already enumerated on disk and this index is
-- the whole check.
CREATE INDEX messages_owner_file_idx ON messages (owner_id, file_id) WHERE file_id <> 0;
