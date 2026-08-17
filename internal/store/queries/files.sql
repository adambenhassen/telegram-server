-- name: InsertFile :one
INSERT INTO files (uploader_id, access_hash, size, mime_type, file_name)
VALUES ($1, $2, $3, $4, $5)
RETURNING *;

-- name: MarkFileStored :execrows
UPDATE files SET stored = true WHERE id = $1 AND stored = false;

-- UserStoredBytes is the per-account storage cap's input. With no blob deleter
-- in M5 nothing decrements it, so it is a lifetime quota, not a live one.
-- name: UserStoredBytes :one
SELECT coalesce(sum(size), 0)::bigint FROM files WHERE uploader_id = $1;

-- FileForDownload is the M5 download authorization gate, and it is one query on
-- purpose. Matching (id, access_hash) is wire compatibility and defence in
-- depth; the EXISTS is the boundary. messages is per-owner — one row per
-- entitled account, and one per member on a chat fan-out — so the set of
-- accounts allowed to read a file is already enumerated on disk and needs no
-- separate membership model. Channels are the exception, and the second EXISTS
-- is why: a channel post writes ONE channel_messages row and no per-member row
-- at all, so entitlement there has to be computed from membership rather than
-- read off an enumeration. banned_until is part of that computation — a banned
-- member keeps their channel_participants row, and without the predicate that
-- retained row would be an entitlement instead of a record, making a ban
-- cosmetic for exactly the content worth exfiltrating.
--
-- Consequences that follow from putting it here rather than in the handler:
-- deleteMessages soft-deletes both sides, so deleting a media message actually
-- revokes retrieval instead of being cosmetic; and an (id, access_hash) pair
-- pasted to an account that never received the file is inert.
--
-- Every rejection returns pgx.ErrNoRows, which the caller maps to ONE error.
-- files.id is dense BIGSERIAL, so distinguishing "no such file" from "wrong
-- hash" from "not yours" would turn this into an enumeration oracle over every
-- file on the server.
-- name: FileForDownload :one
SELECT f.* FROM files f
WHERE f.id = $1 AND f.access_hash = $2 AND f.stored = true
  AND (
      EXISTS (
          SELECT 1 FROM messages m
          WHERE m.owner_id = $3 AND m.file_id = f.id AND m.deleted = false
      )
      OR EXISTS (
          SELECT 1 FROM channel_messages cm
          JOIN channel_participants cp
            ON cp.channel_id = cm.channel_id AND cp.user_id = $3
          WHERE cm.file_id = f.id
            AND cm.deleted = false
            AND (cp.banned_until IS NULL OR cp.banned_until <= now())
      )
  );

-- LockFileForReference is the M17 interlock. messages.file_id has no foreign
-- key and the pool runs at READ COMMITTED, so a reference insert is invisible
-- to a concurrent reader and no re-read of the reference set can see one in
-- flight. The files row is therefore the lock object: every path that writes a
-- non-zero messages.file_id takes this inside the transaction that inserts the
-- referencing row(s), and an eraser takes the same row FOR UPDATE before
-- deleting it, so the two orders are the only two possible and each is correct.
--
-- Shared, not exclusive: concurrent references to one file are ordinary traffic
-- and must not serialize behind each other. Returning no row is the fail-closed
-- branch — the caller aborts rather than writing a reference to a file that is
-- not there to lock.
-- name: LockFileForReference :one
SELECT id FROM files WHERE id = $1 FOR SHARE;

-- name: FilesByIDs :many
SELECT * FROM files WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND stored = true;

-- name: MaxFileID :one
SELECT coalesce(max(id), 0)::bigint FROM files;

-- MediaErasureScan classifies one bounded window of files rows: for each row it
-- reports whether anything live references it, and whether it is past the age
-- cutoff. It names nothing for deletion; it deletes nothing.
--
-- It takes no lock of any kind, and that is a requirement rather than an
-- omission. files is the terminal lock class — a reference insert takes that
-- row FOR SHARE and waits for nothing afterwards — so a scan holding one would
-- put a send, a forward or a download behind a background pass. Naming a
-- candidate does not require holding it; deciding what an eraser holds, and in
-- what order, is stage 3's design.
--
-- The window is the whole table paged by id rather than a pre-filtered
-- candidate set, because the caller has to report why each file it did NOT name
-- was held back. Filtering those out here would make that count unobservable.
--
-- The reference predicate is the union the download gate already reads:
-- non-deleted messages OR non-deleted channel_messages. channel_messages is in
-- it even though no handler can currently post channel media — omitting it
-- passes every test that can be written today and starts destroying live
-- channel media the day channel posts carry a file id.
--
-- access_hash is deliberately not selected. It is the unguessable half of a
-- download credential, and a candidate report is exactly the kind of record
-- that ends up in log aggregation.
-- name: MediaErasureScan :many
SELECT f.id, f.size, f.stored,
       (f.date < sqlc.arg(older_than)::timestamptz) AS aged,
       EXISTS (
           SELECT 1 FROM messages m
           WHERE m.file_id = f.id AND m.deleted = false
       ) AS message_ref,
       EXISTS (
           SELECT 1 FROM channel_messages cm
           WHERE cm.file_id = f.id AND cm.deleted = false
       ) AS channel_ref
FROM files f
WHERE f.id > sqlc.arg(after_id) AND f.id <= sqlc.arg(through_id)
ORDER BY f.id
LIMIT sqlc.arg(lim)::int;
