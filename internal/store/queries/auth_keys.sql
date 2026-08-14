-- name: SaveAuthKey :exec
INSERT INTO auth_keys (id, key_value)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE
  SET key_value = EXCLUDED.key_value,
      last_seen_at = now();

-- name: AuthKeyByID :one
SELECT ak.id, ak.key_value, ak.user_id, ak.created_at, ak.last_seen_at, ak.pending_user_id,
       u.login_mode,
       (up.user_id IS NOT NULL) AS has_password
FROM auth_keys ak
LEFT JOIN users u ON u.id = ak.user_id
LEFT JOIN user_passwords up ON up.user_id = ak.user_id
WHERE ak.id = $1;

-- name: BindAuthKeyUser :execrows
UPDATE auth_keys SET user_id = $2 WHERE id = $1;

-- name: BindAuthKeyUserForSignUp :execrows
-- Binds an auth key to a user only when the key is unbound (user_id IS NULL)
-- and not mid-2FA (pending_user_id IS NULL). Used exclusively by SignUpUsernameUser
-- so signup cannot rebind an already-authorized session. The shared BindAuthKeyUser
-- has no such guard — signIn relies on its unconditional rebinding behaviour.
UPDATE auth_keys SET user_id = $2 WHERE id = $1 AND user_id IS NULL AND pending_user_id IS NULL;

-- name: TouchAuthKey :exec
UPDATE auth_keys SET last_seen_at = now() WHERE id = $1;

-- name: DeleteAuthKey :exec
DELETE FROM auth_keys WHERE id = $1;

-- name: AuthKeysByUser :many
SELECT * FROM auth_keys WHERE user_id = $1;

-- name: SetPendingUser :execrows
UPDATE auth_keys SET user_id = NULL, pending_user_id = $2 WHERE id = $1;

-- name: PromotePendingUser :execrows
UPDATE auth_keys
SET user_id = $2, pending_user_id = NULL
WHERE id = $1 AND pending_user_id = $2;
