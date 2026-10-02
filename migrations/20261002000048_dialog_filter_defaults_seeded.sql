-- Track the one-time Unread folder seed independently from the folder rows so
-- deleting the folder does not cause it to be recreated on the next read.
ALTER TABLE user_dialog_filter_state
    ADD COLUMN defaults_seeded BOOLEAN NOT NULL DEFAULT false;
