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
UPDATE phone_codes SET attempts = attempts + 1 WHERE phone = $1;

-- name: ConsumeCode :exec
UPDATE phone_codes SET consumed_at = now() WHERE phone = $1;

-- name: DeleteExpiredCodes :execrows
DELETE FROM phone_codes WHERE expires_at < now();
