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
-- separate membership model.
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
  AND EXISTS (
      SELECT 1 FROM messages m
      WHERE m.owner_id = $3 AND m.file_id = f.id AND m.deleted = false
  );

-- name: FilesByIDs :many
SELECT * FROM files WHERE id = ANY(sqlc.arg(ids)::bigint[]) AND stored = true;
