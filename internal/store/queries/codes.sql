-- name: UpsertCode :exec
-- Issues a fresh code, resetting the hardening state (attempts, consumed_at,
-- created_at) so a re-issue starts a clean single-use window.
INSERT INTO phone_codes (phone, code_hash, code, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (phone) DO UPDATE
  SET code_hash   = EXCLUDED.code_hash,
      code        = EXCLUDED.code,
      expires_at  = EXCLUDED.expires_at,
      attempts    = 0,
      consumed_at = NULL,
      created_at  = now();

-- name: GetCode :one
SELECT * FROM phone_codes WHERE phone = $1;

-- name: IncrementCodeAttempts :exec
-- Scoped to the exact issued code by its hash so a concurrent resend (new hash)
-- is never charged for a failed attempt against the old code.
UPDATE phone_codes SET attempts = attempts + 1
WHERE phone = $1 AND code_hash = $2;

-- name: ConsumeCode :execrows
-- Compare-and-swap: consume only the exact issued code, and only while it is
-- still verifiable. The terminal-state guards live in the WHERE so a code that
-- lost a race to a concurrent resend/consume/expiry affects zero rows.
UPDATE phone_codes SET consumed_at = now()
WHERE phone = $1
  AND code_hash = $2
  AND code = $3
  AND consumed_at IS NULL
  AND expires_at >= now()
  AND attempts < $4;

-- name: DeleteExpiredCodes :execrows
DELETE FROM phone_codes WHERE expires_at < now();
