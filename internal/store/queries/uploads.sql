-- UpsertUploadPart writes one part of an in-flight upload. Re-saving the same
-- part is legal — a client retries a failed part — so this is an upsert, and
-- the caller re-reads the sums afterwards rather than tracking a delta.
-- name: UpsertUploadPart :exec
INSERT INTO upload_parts (user_id, file_id, part_index, payload)
VALUES ($1, $2, $3, $4)
ON CONFLICT (user_id, file_id, part_index)
DO UPDATE SET payload = excluded.payload, date = now();

-- name: FileOutstandingBytes :one
SELECT coalesce(sum(length(payload)), 0)::bigint FROM upload_parts
WHERE user_id = $1 AND file_id = $2;

-- name: UserOutstandingBytes :one
SELECT coalesce(sum(length(payload)), 0)::bigint FROM upload_parts
WHERE user_id = $1;

-- UploadPartsSummary is what assembly checks contiguity with: part indexes are
-- distinct and non-negative by the primary key, so n = total and max = total-1
-- together prove the set is exactly {0 .. total-1} with no gaps.
-- name: UploadPartsSummary :one
SELECT count(*)::bigint AS parts,
       coalesce(max(part_index), -1)::int AS max_index,
       coalesce(sum(length(payload)), 0)::bigint AS total_bytes
FROM upload_parts WHERE user_id = $1 AND file_id = $2;

-- name: UploadPartPayload :one
SELECT payload FROM upload_parts WHERE user_id = $1 AND file_id = $2 AND part_index = $3;

-- name: DeleteUploadParts :execrows
DELETE FROM upload_parts WHERE user_id = $1 AND file_id = $2;

-- name: DeleteExpiredUploadParts :execrows
DELETE FROM upload_parts WHERE date < $1;
