-- M16: publicly_discoverable — the cost gate that stops contacts.search pricing
-- private channels.
--
-- The M15 title index made both channel arms of contacts.search cost one probe
-- per channel whose title matched the token, private channels included. The
-- response never carried a private channel, but its latency counted them: at
-- 100k private titles sharing a guessed word the empty answer took 149 ms
-- against 1.2 ms at ten, so an authenticated account could read the aggregate
-- number of private channels holding any word it liked. This migration removes
-- the shape that made that possible.
--
-- publicly_discoverable is derived state, not a new fact. It is the answer to
-- "does this channel own a usernames row", recomputed from usernames itself in
-- the same transaction that claims or releases the handle, and recomputable at
-- any time from that table alone — the UPDATE below is the same expression the
-- writer runs, restricted to one row. Nothing reads it as authority: the
-- authoritative JOIN usernames in SearchPublicChannels is untouched and still
-- sits pre-LIMIT, so this column can only decide what the query costs, never
-- what it returns. That makes its two failure directions asymmetric on purpose.
-- A row wrongly marked public enters the candidate set and is dropped by the
-- join before LIMIT sees it: time spent, no byte disclosed. A row wrongly
-- marked private drops out of title search while staying reachable by handle
-- and by every other path: an availability defect, and the direction that fails
-- closed.
--
-- The backfill is additive and reversible. It writes derived data only, and a
-- rollback is DROP COLUMN plus recreating the index below — no channel, handle
-- or membership is read or changed.

ALTER TABLE channels
    ADD COLUMN publicly_discoverable BOOLEAN NOT NULL DEFAULT false;

UPDATE channels c
SET publicly_discoverable = EXISTS (
    SELECT 1 FROM usernames un
    WHERE un.owner_type = 'channel' AND un.owner_id = c.id
);

-- The title index is rebuilt over public rows only. This replaces
-- channels_title_tsv_idx rather than joining it, and the replacement is the
-- point: while an index over every title exists, a plan that scans it and
-- filters the private rows out afterwards stays available to the planner, and
-- that plan is the leak. It is index-driven and it still costs one entry per
-- private match. Safety that depends on the planner preferring the cheaper
-- index is safety that a change in table statistics can withdraw, so the whole
-- table's titles stop being indexed and the leaking plan stops existing.
--
-- Dropping an index destroys no data and this file recreates either one, but it
-- does decide what plans the two arms can have, so both are named here in one
-- place rather than left to be inferred:
--
--   public arm — candidates come from this partial index, so its cost tracks
--   the number of PUBLIC channels matching the token, which is exactly what the
--   response already discloses.
--
--   member arm — candidates are restricted to the caller's own channel ids
--   inside the statement (channels.sql, SearchMemberChannels), so the plan is a
--   primary-key scan over at most the per-account channel cap and no title
--   index takes part at all. Its cost tracks the caller's own memberships and
--   nothing global.
CREATE INDEX channels_public_title_tsv_idx
    ON channels USING GIN (title_tsv)
    WHERE publicly_discoverable;

DROP INDEX channels_title_tsv_idx;
