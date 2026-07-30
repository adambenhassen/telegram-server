-- MAIN-108: invite revocation — add revoked_at to channel_invites.
--
-- Nullable timestamptz: NULL means active, non-NULL means revoked. Additive
-- only: no row is deleted, no existing row is rewritten. Re-reading a revoked
-- invite returns the same row with the timestamp populated.

ALTER TABLE channel_invites ADD COLUMN revoked_at TIMESTAMPTZ NULL;
