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

-- AllocatedFileIDCeiling is the highest id the files sequence has ever
-- handed out, read from the sequence itself rather than from the rows.
--
-- The two differ once a row is deleted: max(id) falls below the erased ids
-- while the sequence does not, and the sequence is the bound a pass over the
-- blob store's disk needs. A committed row delete whose unlink was lost
-- leaves its blob at the top of the id space, and a bound that has shrunk
-- below it puts that blob above the snapshot, where nothing names it until a
-- later upload allocates past it. The sequence is an upper bound on every id
-- allocated before the read and never falls below an id whose row was once
-- committed, which is what the classification's snapshot argument requires.
--
-- pg_sequence_last_value is NULL on a sequence that never advanced, and the
-- coalesce makes the empty table the same zero the row-based query returns.
-- name: AllocatedFileIDCeiling :one
SELECT coalesce(pg_sequence_last_value('files_id_seq'), 0)::bigint;

-- ExistingFileIDs answers "which of these ids does the database still account
-- for", for a pass classifying what is on the blob store's disk.
--
-- stored is deliberately not in the predicate, unlike FilesByIDs. A row with
-- stored = false is an assembly that crashed or one running right now, and its
-- bytes are on their way to that exact key: treating it as unaccounted for
-- would name a live upload's blob, which is the one mistake this classification
-- exists to avoid. Whether an unstored row is itself reclaimable is the files
-- table's question and MediaErasureScan already answers it.
--
-- No lock and no join. It is one indexed probe per id over the primary key, so
-- a background walk of the tree never puts a send or a download behind it.
-- name: ExistingFileIDs :many
SELECT id FROM files WHERE id = ANY(sqlc.arg(ids)::bigint[]);

-- FileExistsForBlob is the final database check before an assembled blob with
-- no row may be reclaimed. It deliberately sees stored and not-stored rows:
-- the latter are live upload assemblies and are a different class only in the
-- row-driven pass. A plain existence probe does not wait on a row lock, so an
-- eraser never parks upload, send or download traffic behind this check.
-- name: FileExistsForBlob :one
SELECT EXISTS (SELECT 1 FROM files WHERE id = $1);

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
--
-- m.file_id <> 0 in the messages arm is redundant against m.file_id = f.id and
-- must not be removed. The files_id_positive CHECK constraint holds files.id
-- above 0, so no files row can carry the sentinel and the conjunct selects no
-- row differently. The constraint is what makes that true — BIGSERIAL only
-- supplies a default and an explicit insert can still name a 0 — so the two
-- travel together and neither is removable on its own. The conjunct is here
-- because it is the only qual that makes messages_file_idx usable: that index
-- is partial on file_id <> 0, and Postgres uses a partial index only where it
-- can prove the index predicate from the query's own quals — a proof that needs
-- a Const on both sides. f.id is a runtime value from the outer scan, so
-- file_id = f.id proves nothing and the index is discarded in silence:
-- measured, the SubPlan degrades to a per-row Seq Scan of messages, 3.41M
-- buffers for one 1000-row batch against 5.4k. The channel_messages arm needs
-- no counterpart because its index is partial on file_id IS NOT NULL, which a
-- strict equality does prove.
-- name: MediaErasureScan :many
SELECT f.id, f.size, f.stored,
       (f.date < sqlc.arg(older_than)::timestamptz) AS aged,
       EXISTS (
           SELECT 1 FROM messages m
           WHERE m.file_id = f.id AND m.file_id <> 0 AND m.deleted = false
       ) AS message_ref,
       EXISTS (
           SELECT 1 FROM channel_messages cm
           WHERE cm.file_id = f.id AND cm.deleted = false
       ) AS channel_ref
FROM files f
WHERE f.id > sqlc.arg(after_id) AND f.id <= sqlc.arg(through_id)
ORDER BY f.id
LIMIT sqlc.arg(lim)::int;

-- LockFileForErase takes one files row exclusively, or reports nothing when
-- another transaction holds it. It is the eraser's half of the M17 interlock:
-- LockFileForReference above takes the same row FOR SHARE on every path that
-- writes a reference, so the two modes conflict and the orders are the only
-- two possible.
--
-- It carries no predicate, and that omission is the safety argument rather than
-- an oversight. Under READ COMMITTED a statement evaluates its qual and takes
-- its row locks against one snapshot, so a `WHERE <no live reference> FOR
-- UPDATE` would decide erasability against a snapshot taken BEFORE the lock was
-- held — and a forward that committed in between is invisible to it while its
-- own share lock is already released, so nothing blocks and nothing refuses.
-- Locking in a statement of its own puts the reference predicate in a later
-- statement, whose fresh snapshot sees every reference committed before the
-- lock was taken; every reference not yet committed blocks on the lock and
-- then fails closed on the missing row. Those two cases are exhaustive.
--
-- SKIP LOCKED, not NOWAIT and not a wait: a files row is a terminal lock class
-- that a forward reaches while already holding the destination chat's row lock
-- and up to 200 per-owner advisory locks, so an eraser that queued behind a
-- contended row would hold nothing but would leave itself parked, and an
-- eraser that waited would be the one parking the chat. A row someone else
-- holds is somebody's live reference being written; it is not a candidate this
-- second, and the next sweep will read it again.
-- name: LockFileForErase :one
SELECT id, size FROM files WHERE id = $1 FOR UPDATE SKIP LOCKED;

-- ClearDeletedChannelFileRefs releases the file reference held by channel posts
-- that are already soft-deleted, and only those.
--
-- It exists because the foreign key at channel_messages.file_id is RESTRICT and
-- counts soft-deleted rows: while any channel_messages row names a file, live
-- or deleted, DELETE FROM files is refused. Measured, not assumed — a file
-- posted once to a channel and then deleted is unerasable forever without this
-- statement, and the refusal is raised by the constraint rather than by any
-- code here.
--
-- The `deleted = true` condition is what keeps the constraint a backstop rather
-- than removing it. Cascade would delete live channel posts and set-null would
-- turn a live media post into a text post; this clears only rows that are
-- already tombstones, so a post created in the race window is still named by a
-- row this statement did not touch, and the DELETE that follows aborts the
-- whole transaction on the constraint even if every predicate above it missed.
--
-- Taken after the files row and never before: files first, then
-- channel_messages, the same direction a channel media send must take once one
-- exists (lock the file row, then insert the post). The reverse order is a
-- deadlock between the eraser and channel media the day channel media ships.
-- name: ClearDeletedChannelFileRefs :execrows
UPDATE channel_messages SET file_id = NULL WHERE file_id = $1 AND deleted = true;

-- DeleteUnreferencedFile removes one files row, and only if every condition
-- that made it a candidate still holds. It is the decision; the scan that named
-- the file only proposed it.
--
-- Every condition is re-evaluated by this statement rather than trusted from
-- the earlier scan, for the reason finding 2 of the M17 threat model gives:
-- between naming a candidate and acting on it, a file can gain a reference, and
-- a SELECT that decided otherwise is a statement about a moment that has
-- passed. Under READ COMMITTED this statement takes a fresh snapshot, and the
-- caller already holds the row exclusively, so what it cannot see cannot yet
-- exist: a reference writer is either committed and visible here, or blocked on
-- the row lock and about to fail closed on the row being gone.
--
-- The age cutoff is repeated here for the same reason and not because the scan
-- forgot it. It is defence in depth either way — every media file on the server
-- is stored with zero live references for the length of one send, so the age
-- gate narrows that window but does not close it, and what actually keeps a
-- live file safe is the row lock this runs under.
--
-- stored = true keeps this statement off not-stored rows entirely: an
-- unassembled file is a crashed upload or one running right now, it has no
-- bytes at this key to unlink, and reclaiming it is a different mechanism.
-- name: DeleteUnreferencedFile :execrows
DELETE FROM files f
WHERE f.id = sqlc.arg(id)
  AND f.stored = true
  AND f.date < sqlc.arg(older_than)::timestamptz
  AND NOT EXISTS (
      SELECT 1 FROM messages m
      WHERE m.file_id = f.id AND m.deleted = false
  )
  AND NOT EXISTS (
      SELECT 1 FROM channel_messages cm
      WHERE cm.file_id = f.id AND cm.deleted = false
  );

-- DeleteUnassembledFile is the crashed-assembly half of the row-driven
-- reclaim. It repeats every condition after the exclusive row lock rather
-- than trusting the scan: a live assembly can finish, or a reference can be
-- created, in the gap. The caller unlinks the exact key only after this row
-- deletion commits.
-- name: DeleteUnassembledFile :execrows
DELETE FROM files f
WHERE f.id = sqlc.arg(id)
  AND f.stored = false
  AND f.date < sqlc.arg(older_than)::timestamptz
  AND NOT EXISTS (
      SELECT 1 FROM messages m
      WHERE m.file_id = f.id AND m.file_id <> 0 AND m.deleted = false
  )
  AND NOT EXISTS (
      SELECT 1 FROM channel_messages cm
      WHERE cm.file_id = f.id AND cm.deleted = false
  );
