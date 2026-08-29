-- UpsertUploadPart writes one part's accounting row. The bytes live in the
-- blob store under blob_key; the row names the key and the server-measured
-- size, and nothing in the row is client-declared.
--
-- Re-saving the same part is legal — a client retries a failed part — so this
-- is an upsert, and the caller re-reads the sums afterwards rather than
-- tracking a delta.
--
-- The conflict clause is the size rule: the recorded size describes the object
-- the row currently names, and nothing else. Every save draws a fresh key, so
-- the size and the key move together — a row can never claim a size the object
-- it names does not have. Holding a superseded larger size instead would
-- describe an object that is about to be deleted, and the fail-closed read at
-- assembly would then reject a perfectly well-formed upload.
--
-- The superseded key is what this returns, empty when the part is new. The
-- object it names is unreachable from any row the moment this commits, so the
-- caller deletes it: without that, a client looping saveFilePart on one part
-- grows stored bytes without bound while the row-based caps stay flat.
--
-- The pre-conflict read is a plain CTE, so it runs against the statement's
-- snapshot and sees the row as it was before the upsert. The INSERT is a
-- data-modifying CTE and executes exactly once whether or not the final SELECT
-- references it.
--
-- date is deliberately NOT refreshed on conflict: it is what the TTL sweep
-- measures from, so a re-save that moved it would let an account hold an
-- outstanding set alive forever by touching each part every TTL/2.
-- name: UpsertUploadPart :one
WITH superseded AS (
    SELECT prev.blob_key FROM upload_parts prev
    WHERE prev.user_id = $1 AND prev.file_id = $2 AND prev.part_index = $3
), upserted AS (
    INSERT INTO upload_parts (user_id, file_id, part_index, size, blob_key)
    VALUES ($1, $2, $3, $4, $5)
    ON CONFLICT (user_id, file_id, part_index)
    DO UPDATE SET size = excluded.size,
                  blob_key = excluded.blob_key
    RETURNING 1
)
SELECT coalesce((SELECT blob_key FROM superseded), '')::text AS superseded_key;

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

-- UploadPartRefs lists an upload's parts in index order, each with the key its
-- bytes are read back from and the size that read is reconciled against. It is
-- the whole of what assembly needs from Postgres: one statement instead of a
-- key lookup per part on the validation pass and a second one per part on the
-- read pass. The assembly cleanup takes its keys from here too, so the bytes
-- go before the rows.
-- name: UploadPartRefs :many
SELECT part_index, blob_key, size FROM upload_parts
WHERE user_id = $1 AND file_id = $2
ORDER BY part_index;

-- There is deliberately no delete over a whole upload. DeleteUploadPartByKey
-- below is the only statement that removes a parts row, so a row can only be
-- retired by naming it and the key whose bytes were deleted for it. A
-- convenience delete over (user_id, file_id) is what the assembly cleanup used
-- to run, and it dropped rows whose bytes it had never touched.

-- ClaimExpiredUploadParts takes at most one batch of expired parts, oldest
-- first, returning each row's primary key and the blob key it names. The bound
-- is the point: unbounded, this is one statement over every account's expired
-- rows, holding row locks and writing WAL in proportion to whatever
-- accumulated while the sweep was down. The caller repeats it until a pass
-- comes back short.
--
-- It partitions nothing between replicas, and must not be read as if it did.
-- The caller runs it in autocommit, so FOR UPDATE SKIP LOCKED holds its row
-- locks only to the end of the statement: two replicas sweeping at once can
-- and do take the same rows. That is safe rather than merely tolerated —
-- deleting an object twice is a no-op and the conditional row delete below
-- matches at most once — and it is why no durable claim column exists here.
-- SKIP LOCKED stays because it keeps a pass from blocking behind another
-- replica's in-flight statement.
-- name: ClaimExpiredUploadParts :many
SELECT old.user_id, old.file_id, old.part_index, old.blob_key
FROM upload_parts old
WHERE old.date < $1
ORDER BY old.date
LIMIT sqlc.arg(lim)::int
FOR UPDATE SKIP LOCKED;

-- DeleteUploadPartByKey drops one claimed row, and only if it still names the
-- key whose bytes the sweep deleted. Both halves matter. The primary key makes
-- it one row: the rows a deploy leaves behind all carry the same empty
-- blob_key, and a delete keyed on blob_key alone would take every one of them
-- on the first claimed key, which is the per-batch bound gone in exactly the
-- case it is needed. The blob_key condition makes it conditional: a re-save
-- that committed in the window between the claim and the byte delete has
-- renamed the row, and its row and its new object both survive.
-- name: DeleteUploadPartByKey :execrows
DELETE FROM upload_parts
WHERE user_id = $1 AND file_id = $2 AND part_index = $3 AND blob_key = $4;

-- LivePartKeys reports which of the given blob keys a parts row still names.
-- The orphan pass gates each enumeration page with this: one lookup over the
-- page's at-most-500 keys, so the live-key set is bounded by the page rather
-- than the table, and the gate reads row state at delete time rather than at
-- run start. A key absent from the result is named by no row.
-- name: LivePartKeys :many
SELECT blob_key FROM upload_parts
WHERE blob_key = ANY(sqlc.arg(keys)::text[]);
