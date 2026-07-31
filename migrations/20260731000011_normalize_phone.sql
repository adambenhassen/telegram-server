-- MAIN-127: normalize users.phone to strip leading '+'.
--
-- CreateUser now stores phone via NormalizePhone (strips '+'). This migration
-- backfills existing rows so the stored form matches the new write path.
--
-- If the UNIQUE constraint fires, it means both the original (e.g. '+1555...')
-- and normalized (e.g. '1555...') forms already exist as separate rows. That
-- indicates duplicate registrations that require manual triage before the
-- migration can run. Do not silently drop either row.

UPDATE users SET phone = substring(phone FROM 2) WHERE phone LIKE '+%';
