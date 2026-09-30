-- Add persistent defaults for basic groups and channels, plus per-member
-- megagroup post timing. Empty rights mean unrestricted. The representation
-- deliberately excludes view_messages and expiry, and its allowlist rejects
-- reserved or unsupported Telegram rights flags.
-- Before a production apply, record the verified backup object path and restore
-- target. After an enforcing binary writes non-default state, roll forward with
-- enforcement or use an approved restore/clear procedure; an older binary that
-- ignores these columns is unsafe.

ALTER TABLE chats
    ADD COLUMN default_banned_rights TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD CONSTRAINT chats_default_banned_rights_valid
        CHECK (
            (array_ndims(default_banned_rights) IS NULL OR array_ndims(default_banned_rights) = 1)
            AND array_position(default_banned_rights, NULL::TEXT) IS NULL
            AND default_banned_rights <@ ARRAY[
                'send_messages',
                'send_media',
                'send_stickers',
                'send_gifs',
                'send_games',
                'send_inline',
                'embed_links',
                'send_polls',
                'change_info',
                'invite_users',
                'pin_messages',
                'manage_topics',
                'send_photos',
                'send_videos',
                'send_roundvideos',
                'send_audios',
                'send_voices',
                'send_docs',
                'send_plain'
            ]::TEXT[]
        ) NOT VALID;

ALTER TABLE channels
    ADD COLUMN default_banned_rights TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    ADD COLUMN slowmode_seconds SMALLINT NOT NULL DEFAULT 0,
    ADD CONSTRAINT channels_default_banned_rights_valid
        CHECK (
            (array_ndims(default_banned_rights) IS NULL OR array_ndims(default_banned_rights) = 1)
            AND array_position(default_banned_rights, NULL::TEXT) IS NULL
            AND default_banned_rights <@ ARRAY[
                'send_messages',
                'send_media',
                'send_stickers',
                'send_gifs',
                'send_games',
                'send_inline',
                'embed_links',
                'send_polls',
                'change_info',
                'invite_users',
                'pin_messages',
                'manage_topics',
                'send_photos',
                'send_videos',
                'send_roundvideos',
                'send_audios',
                'send_voices',
                'send_docs',
                'send_plain'
            ]::TEXT[]
        ) NOT VALID,
    ADD CONSTRAINT channels_slowmode_seconds_valid
        CHECK (slowmode_seconds IN (0, 10, 30, 60, 300, 900, 3600)) NOT VALID;

ALTER TABLE channel_participants
    ADD COLUMN last_post_at TIMESTAMPTZ NULL;
