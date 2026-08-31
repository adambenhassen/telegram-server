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
-- The MATERIALIZED CTE makes the outer database-clock check run after the
-- selected row has been locked. The synthetic row keeps absent and terminal
-- handles on the same one-row query path as an issued or expired handle.
-- name: LiveRegistrationInviteForUpdate :one
WITH locked AS MATERIALIZED (
    SELECT id, handle, secret_digest, issued_at, expires_at, state, consumed_at, revoked_at
    FROM registration_invites
    WHERE handle = lower(sqlc.arg(handle))
      AND state = 'issued'
    ORDER BY id DESC
    LIMIT 1
    FOR UPDATE
)
SELECT id, handle, secret_digest, issued_at, expires_at, state, consumed_at, revoked_at,
       true AS found,
       expires_at > clock_timestamp() AS live
FROM locked
UNION ALL
SELECT 0::bigint,
       lower(sqlc.arg(handle))::text,
       decode(repeat('00', 64), 'hex')::bytea,
       NULL::timestamptz,
       NULL::timestamptz,
       'expired'::text,
       NULL::timestamptz,
       NULL::timestamptz,
       false,
       false
WHERE NOT EXISTS (SELECT 1 FROM locked);

-- Guard the state transition as well as taking the row lock above: if expiry
-- passes while the caller is comparing the secret, the update refuses.
-- name: ConsumeRegistrationInvite :execrows
UPDATE registration_invites
SET state = 'consumed', consumed_at = clock_timestamp()
WHERE id = sqlc.arg(id)
  AND secret_digest = sqlc.arg(secret_digest)
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
