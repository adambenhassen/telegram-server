-- Login-code hardening (Task 8 [D1]): single-use codes (consumed_at), a
-- per-code attempt cap (attempts), and a per-phone resend cooldown measured
-- from created_at.

ALTER TABLE phone_codes
    ADD COLUMN attempts    INT NOT NULL DEFAULT 0,
    ADD COLUMN consumed_at TIMESTAMPTZ NULL,
    ADD COLUMN created_at  TIMESTAMPTZ NOT NULL DEFAULT now();
