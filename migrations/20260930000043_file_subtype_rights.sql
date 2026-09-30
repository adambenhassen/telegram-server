-- Preserve accepted document subtype rights with the original files row.
-- NULL means unknown, while an empty array means known generic. A nullable
-- column with no default leaves every existing file unknown and rewrites no
-- file or message data.
-- Before a production apply, record the verified backup object path and restore
-- target.

ALTER TABLE files
    ADD COLUMN subtype_rights TEXT[] NULL,
    ADD CONSTRAINT files_subtype_rights_valid
        CHECK (
            subtype_rights IS NULL OR (
                (array_ndims(subtype_rights) IS NULL OR array_ndims(subtype_rights) = 1)
                AND array_position(subtype_rights, NULL::TEXT) IS NULL
                AND subtype_rights <@ ARRAY[
                    'send_stickers',
                    'send_gifs',
                    'send_videos',
                    'send_roundvideos',
                    'send_audios',
                    'send_voices'
                ]::TEXT[]
            )
        ) NOT VALID;
