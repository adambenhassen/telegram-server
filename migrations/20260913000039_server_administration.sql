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

-- Serialize the emptiness decision against legacy user inserts. SHARE conflicts
-- with the ROW EXCLUSIVE lock acquired by INSERT and remains held through the
-- initialization insert in the migration transaction.
LOCK TABLE users IN SHARE MODE;

INSERT INTO server_administration (singleton_id, election_closed)
SELECT 1, EXISTS (SELECT 1 FROM users);
