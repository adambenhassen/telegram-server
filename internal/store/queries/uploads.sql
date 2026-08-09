-- UpsertUploadPart writes one part of an in-flight upload. Re-saving the same
-- part is legal — a client retries a failed part — so this is an upsert, and
-- the caller re-reads the sums afterwards rather than tracking a delta.
--
-- date is deliberately NOT refreshed on conflict: it is what the TTL sweep
-- measures from, so a re-save that moved it would let an account hold an
-- outstanding set alive forever by touching each part every TTL/2. The payload
-- still wins, so a retry's bytes are the ones assembled.
-- name: UpsertUploadPart :exec
INSERT INTO upload_parts (user_id, file_id, part_index, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (user_id, file_id, part_index)
DO UPDATE SET payload = excluded.payload;

-- name: FileOutstandingBytes :one
SELECT coalesce(sum(length(payload)), 0)::bigint FROM upload_parts
WHERE user_id = $1 AND file_id = $2;

-- name: UserOutstanding :one
SELECT count(*)::bigint AS parts,
       coalesce(sum(length(payload)), 0)::bigint AS total_bytes
FROM upload_parts WHERE user_id = $1;

-- UploadPartsSummary is what assembly checks contiguity with: part indexes are
-- distinct by the primary key and non-negative because partIndexOf rejects a
-- negative index, so n = total and max = total-1 together prove the set is
-- exactly {0 .. total-1} with no gaps.
-- name: UploadPartsSummary :one
SELECT count(*)::bigint AS parts,
       coalesce(max(part_index), -1)::int AS max_index,
       coalesce(sum(length(payload)), 0)::bigint AS total_bytes
FROM upload_parts WHERE user_id = $1 AND file_id = $2;

-- name: UploadPartPayload :one
SELECT payload FROM upload_parts WHERE user_id = $1 AND file_id = $2 AND part_index = $3;

-- name: DeleteUploadParts :execrows
DELETE FROM upload_parts WHERE user_id = $1 AND file_id = $2;

-- DeleteExpiredUploadParts removes at most one batch of expired parts, oldest
-- first. The bound is the point: unbounded, this is one DELETE over every
-- account's expired rows, holding row locks and writing WAL in proportion to
-- whatever accumulated while the sweep was down. The caller repeats it until a
-- pass comes back short.
--
-- SKIP LOCKED because every replica runs this sweep: a batch another replica
-- already holds is its work, and blocking on it would serialise the sweeps into
-- one and re-create the long transaction the bound removes.
-- name: DeleteExpiredUploadParts :execrows
DELETE FROM upload_parts p
USING (
    SELECT old.user_id, old.file_id, old.part_index FROM upload_parts old
    WHERE old.date < $1
    ORDER BY old.date
    LIMIT sqlc.arg(lim)::int
    FOR UPDATE SKIP LOCKED
) expired
WHERE p.user_id = expired.user_id
  AND p.file_id = expired.file_id
  AND p.part_index = expired.part_index;
