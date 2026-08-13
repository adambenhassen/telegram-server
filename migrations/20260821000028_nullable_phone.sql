-- M10 nullable phone: username-based accounts have no phone number.
--
-- The baseline UNIQUE constraint on users.phone becomes a partial unique index
-- so that NULL is not treated as a duplicate. Without this, CreateUser called
-- with an empty string for a username-mode account would conflict on the empty
-- string and return the first account's row.
--
-- Steps:
--  1. Drop the inline UNIQUE constraint (Postgres named it users_phone_key).
--  2. Make the column nullable.
--  3. Add a partial unique index that enforces uniqueness only for non-null
--     values.

ALTER TABLE users DROP CONSTRAINT users_phone_key;
ALTER TABLE users ALTER COLUMN phone DROP NOT NULL;
CREATE UNIQUE INDEX users_phone_unique ON users (phone) WHERE phone IS NOT NULL;
