-- M11 login_mode: distinguishes phone-mode accounts from username-mode accounts.
--
-- Provisional state is not stored as a column. It is derived in AuthKeyByID:
-- provisional = login_mode = 'username' AND no row in user_passwords. This
-- eliminates drift between SetPendingUser/PromotePendingUser and any separate
-- flag — storing a verifier automatically clears provisional state.

ALTER TABLE users ADD COLUMN login_mode TEXT NOT NULL DEFAULT 'phone'
    CHECK (login_mode IN ('phone', 'username'));
