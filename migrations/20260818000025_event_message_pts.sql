-- MAIN-217: index the (owner, message) direction of the two event logs.
--
-- Both logs are keyed (owner, pts) because every existing reader walks them in
-- pts order. Answering a resend asks the opposite question — which pts does this
-- already-stored message occupy — and on the primary key that is a scan of the
-- owner's whole log. The read sits ahead of the send rate limit by design (a
-- resend must never draw FLOOD_WAIT), so leaving it unindexed hands an
-- authenticated client an unmetered scan, the same reasoning M15 recorded for
-- usernames_owner_idx.
--
-- Partial on type = 1, the new-message event: a message has exactly one, and
-- edit/delete/read rows would only widen the index without ever being read
-- through it.
--
-- Additive and reversible: DROP INDEX restores the previous state exactly, and
-- no running code depends on either index existing. Both are ordinary
-- (non-concurrent) CREATE INDEX statements, matching every index in this
-- directory; each takes a SHARE lock on its table for the length of one build.

CREATE INDEX message_events_new_message_idx
    ON message_events (owner_id, local_id) WHERE type = 1;

CREATE INDEX channel_events_new_message_idx
    ON channel_events (channel_id, local_id) WHERE type = 1;
