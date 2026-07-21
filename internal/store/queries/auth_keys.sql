-- name: SaveAuthKey :exec
INSERT INTO auth_keys (id, key_value)
VALUES ($1, $2)
ON CONFLICT (id) DO UPDATE
  SET key_value = EXCLUDED.key_value,
      last_seen_at = now();

-- name: AuthKeyByID :one
SELECT * FROM auth_keys WHERE id = $1;

-- name: BindAuthKeyUser :execrows
UPDATE auth_keys SET user_id = $2 WHERE id = $1;

-- name: DeleteAuthKey :exec
DELETE FROM auth_keys WHERE id = $1;

-- name: AuthKeysByUser :many
SELECT * FROM auth_keys WHERE user_id = $1;
