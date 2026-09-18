-- M21: durable first-user server administration election.
--
-- The singleton is open only when it has no administrator. A migrated
-- non-empty installation is closed without promotion so existing data cannot
-- acquire authority as a side effect of this migration.
CREATE TABLE server_administration (
    singleton_id          SMALLINT NOT NULL PRIMARY KEY CHECK (singleton_id = 1),
    election_closed       BOOLEAN NOT NULL,
    administrator_user_id BIGINT UNIQUE NULL REFERENCES users(id) ON DELETE RESTRICT,
    CHECK (election_closed OR administrator_user_id IS NULL)
);

-- The deferred constraint lets the current first-user transaction close the
-- election after inserting its user, while rejecting legacy inserts that
-- commit after migration initialized an open election.
CREATE FUNCTION ensure_server_administration_closed()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM server_administration
        WHERE singleton_id = 1
          AND election_closed
    ) THEN
        RAISE EXCEPTION 'server administrator election is open';
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER users_require_closed_server_administration
AFTER INSERT ON users
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
EXECUTE FUNCTION ensure_server_administration_closed();

-- Serialize the emptiness decision against legacy user inserts. SHARE conflicts
-- with the ROW EXCLUSIVE lock acquired by INSERT and remains held through the
-- initialization insert in the migration transaction.
LOCK TABLE users IN SHARE MODE;

INSERT INTO server_administration (singleton_id, election_closed)
SELECT 1, EXISTS (SELECT 1 FROM users);
