-- Retire an issued row whose expiry has passed so the handle can be issued
-- again. The clock is PostgreSQL's clock, not a process timestamp.
-- name: ExpireRegistrationInvite :execrows
UPDATE registration_invites
SET state = 'expired'
WHERE handle = lower(sqlc.arg(handle))
  AND state = 'issued'
  AND expires_at <= clock_timestamp();

-- Issue returns metadata but never returns the stored digest. The secret is
-- generated and retained only by the caller that receives the result.
-- name: InsertRegistrationInvite :one
WITH issued AS (SELECT clock_timestamp() AS at)
INSERT INTO registration_invites (handle, secret_digest, issued_at, expires_at)
SELECT
    lower(sqlc.arg(handle)),
    sqlc.arg(secret_digest),
    issued.at,
    issued.at + (sqlc.arg(lifetime_us)::bigint * interval '1 microsecond')
FROM issued
RETURNING id, handle, issued_at, expires_at, state, consumed_at, revoked_at;

-- The row lock is the decision boundary shared with consume and revoke. A
-- caller holds it for its transaction, so a rollback cannot consume the row.
-- name: LiveRegistrationInviteForUpdate :one
SELECT id, handle, secret_digest, issued_at, expires_at, state, consumed_at, revoked_at
FROM registration_invites
WHERE handle = lower(sqlc.arg(handle))
  AND state = 'issued'
ORDER BY id DESC
LIMIT 1
FOR UPDATE;

-- This check runs after the handle row is locked. Keeping it as a separate
-- statement means a waiter cannot use a clock value captured before it
-- acquired the row lock to admit an invite that expired while it waited.
-- name: RegistrationInviteIsLive :one
SELECT expires_at > clock_timestamp()
FROM registration_invites
WHERE id = sqlc.arg(id)
  AND state = 'issued';

-- Guard the state transition as well as taking the row lock above: if expiry
-- passes while the caller is comparing the secret, the update refuses.
-- name: ConsumeRegistrationInvite :execrows
UPDATE registration_invites
SET state = 'consumed', consumed_at = clock_timestamp()
WHERE id = sqlc.arg(id)
  AND state = 'issued'
  AND expires_at > clock_timestamp();

-- Revoke and consume both update the same row. PostgreSQL's row-lock order
-- therefore decides the race: whichever update acquires the lock first wins.
-- name: LockRegistrationInvite :one
SELECT id
FROM registration_invites
WHERE id = sqlc.arg(id)
FOR UPDATE;

-- name: RevokeRegistrationInvite :execrows
UPDATE registration_invites
SET state = 'revoked', revoked_at = clock_timestamp()
WHERE id = sqlc.arg(id)
  AND state = 'issued'
  AND expires_at > clock_timestamp();

-- State is projected from database time so a naturally expired row is reported
-- as expired even before another writer retires it.
-- name: ListRegistrationInvites :many
SELECT id, handle, issued_at, expires_at,
       CASE
           WHEN state = 'issued' AND expires_at <= clock_timestamp() THEN 'expired'
           ELSE state
       END::text AS state,
       consumed_at, revoked_at
FROM registration_invites
ORDER BY id;
