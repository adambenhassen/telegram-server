-- M20: durable, single-use registration invites.
--
-- The secret is full-entropy and is stored only as its SHA-256 digest. An
-- invite can keep its row after it stops being usable so operators can see the
-- terminal state. Expired rows are moved out of the issued state when a new
-- invite for the same handle is issued; natural expiry is derived from
-- expires_at by reads until then.
CREATE TABLE registration_invites (
    id            BIGSERIAL PRIMARY KEY,
    handle        TEXT        NOT NULL CHECK (handle <> ''),
    secret_digest BYTEA       NOT NULL CHECK (octet_length(secret_digest) = 32),
    issued_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at    TIMESTAMPTZ NOT NULL,
    state         TEXT        NOT NULL DEFAULT 'issued'
        CHECK (state IN ('issued', 'consumed', 'revoked', 'expired')),
    consumed_at   TIMESTAMPTZ NULL,
    revoked_at    TIMESTAMPTZ NULL,
    CHECK (expires_at > issued_at),
    CHECK (expires_at <= issued_at + interval '30 days'),
    CHECK (
        (state IN ('issued', 'expired') AND consumed_at IS NULL AND revoked_at IS NULL)
        OR (state = 'consumed' AND consumed_at IS NOT NULL AND revoked_at IS NULL)
        OR (state = 'revoked' AND consumed_at IS NULL AND revoked_at IS NOT NULL)
    )
);

-- PostgreSQL predicates cannot contain the clock, so issued is the indexed
-- live-candidate state. The store retires naturally expired issued rows before
-- inserting a replacement, while every admission decision also checks time.
CREATE UNIQUE INDEX registration_invites_live_handle_idx
    ON registration_invites (handle) WHERE state = 'issued';

CREATE INDEX registration_invites_handle_idx
    ON registration_invites (handle, id);
