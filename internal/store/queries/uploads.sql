-- UpsertUploadPart writes one part's accounting row. The bytes live in the
-- blob store under blob_key; the row names the key and the server-measured
-- size, and nothing in the row is client-declared.
--
-- Re-saving the same part is legal — a client retries a failed part — so this
-- is an upsert, and the caller re-reads the sums afterwards rather than
-- tracking a delta.
--
-- The conflict clause is the size rule: the recorded size is never lowered
-- below the size of the object that may still exist at the part's key. A
-- re-save that records a smaller size while the larger object is still there
-- would let the outstanding-byte cap be evaded by shrinking rows, so a
-- smaller size keeps the larger one. The bytes themselves are replaced
-- unconditionally, so the assembled file reflects the retry.
--
-- date is deliberately NOT refreshed on conflict: it is what the TTL sweep
-- measures from, so a re-save that moved it would let an account hold an
-- outstanding set alive forever by touching each part every TTL/2.
-- name: UpsertUploadPart :exec
INSERT INTO upload_parts (user_id, file_id, part_index, size, blob_key)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (user_id, file_id, part_index)
DO UPDATE SET size = greatest(upload_parts.size, excluded.size),
              blob_key = excluded.blob_key;

-- FileOutstandingBytes sums the recorded sizes. The caps aggregate over rows,
-- so a retry is still not billed twice: there is no counter to increment,
-- only a SUM over rows the upsert has already deduplicated.
-- name: FileOutstandingBytes :one
SELECT coalesce(sum(size), 0)::bigint FROM upload_parts
WHERE user_id = $1 AND file_id = $2;

-- name: UserOutstanding :one
SELECT count(*)::bigint AS parts,
       coalesce(sum(size), 0)::bigint AS total_bytes
FROM upload_parts WHERE user_id = $1;

-- UploadPartsSummary is what assembly checks contiguity with: part indexes are
-- distinct by the primary key and non-negative because partIndexOf rejects a
-- negative index, so n = total and max = total-1 together prove the set is
-- exactly {0 .. total-1} with no gaps.
-- name: UploadPartsSummary :one
SELECT count(*)::bigint AS parts,
       coalesce(max(part_index), -1)::int AS max_index,
       coalesce(sum(size), 0)::bigint AS total_bytes
FROM upload_parts WHERE user_id = $1 AND file_id = $2;

-- UploadPartKey returns one part's recorded blob key and size. The key is
-- what the bytes are read back from; the size is what assembly reconciles
-- the read against.
-- name: UploadPartKey :one
SELECT blob_key, size FROM upload_parts WHERE user_id = $1 AND file_id = $2 AND part_index = $3;

-- UploadPartKeys lists the blob keys an upload's rows currently name, so the
-- assembly cleanup can delete the bytes before the rows.
-- name: UploadPartKeys :many
SELECT blob_key FROM upload_parts WHERE user_id = $1 AND file_id = $2;

-- name: DeleteUploadParts :execrows
DELETE FROM upload_parts WHERE user_id = $1 AND file_id = $2;

-- ClaimExpiredUploadParts claims at most one batch of expired parts, oldest
-- first, returning the blob keys the batch names. The bound is the point:
-- unbounded, this is one statement over every account's expired rows, holding
-- row locks and writing WAL in proportion to whatever accumulated while the
-- sweep was down. The caller repeats it until a pass comes back short.
--
-- SKIP LOCKED because every replica runs this sweep: a batch another replica
-- already holds is its work, and blocking on it would serialise the sweeps
-- into one and re-create the long transaction the bound removes.
--
-- This only claims: it returns the keys and holds no locks once committed. The
-- byte delete and the conditional row delete run afterwards, outside any
-- transaction, so a hanging storage backend cannot pin the claim's locks.
-- name: ClaimExpiredUploadParts :many
SELECT old.blob_key
FROM upload_parts old
WHERE old.date < $1
ORDER BY old.date
LIMIT sqlc.arg(lim)::int
FOR UPDATE SKIP LOCKED;

-- DeleteUploadPartsByKey drops the one row naming blob_key, if any. The
-- sweep's finalise step uses it per claimed key: the delete is conditional on
-- the row still naming the key the sweep deleted — never blind on the primary
-- key — so a re-save that committed in the window between the claim and the
-- byte delete keeps its row and its new object.
-- name: DeleteUploadPartsByKey :exec
DELETE FROM upload_parts WHERE blob_key = $1;
