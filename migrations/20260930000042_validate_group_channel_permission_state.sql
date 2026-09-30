-- Run the table scans after migration 41 commits and releases its column-addition
-- locks. VALIDATE CONSTRAINT uses a lock mode compatible with ordinary reads and writes.
ALTER TABLE chats VALIDATE CONSTRAINT chats_default_banned_rights_valid;
ALTER TABLE channels VALIDATE CONSTRAINT channels_default_banned_rights_valid;
ALTER TABLE channels VALIDATE CONSTRAINT channels_slowmode_seconds_valid;
