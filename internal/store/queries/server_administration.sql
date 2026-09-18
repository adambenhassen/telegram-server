-- name: LockServerAdministration :many
-- Lock every row while validating the singleton before an account commit.
SELECT singleton_id, election_closed, administrator_user_id
FROM server_administration
ORDER BY singleton_id
FOR UPDATE;

-- name: ElectServerAdministrator :one
-- The conditional update is the durable concurrency arbiter for the first
-- user. It must never be replaced by an application-side count or mutex.
UPDATE server_administration
SET election_closed = TRUE,
    administrator_user_id = sqlc.arg(user_id)
WHERE singleton_id = 1
  AND election_closed = FALSE
  AND administrator_user_id IS NULL
RETURNING singleton_id, election_closed, administrator_user_id;

-- name: IsServerAdministrator :one
SELECT CASE WHEN
    count(*) = 1
    AND bool_and(singleton_id = 1)
    AND bool_and(election_closed)
    AND bool_or(administrator_user_id = sqlc.arg(user_id))
    THEN TRUE ELSE FALSE END
FROM server_administration;
