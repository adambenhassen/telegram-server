-- Add forwarding columns to messages table.
-- fwd_from_id: the user id of the original sender (nullable; 0/null = not forwarded).
-- fwd_date: the date of the original message being forwarded (nullable).
-- fwd_channel_id: the channel id when the source is a channel post (nullable).
-- fwd_channel_post: the local_id of the channel post when the source is a channel (nullable).
alter table messages
    add column fwd_from_id     bigint null,
    add column fwd_date        timestamptz null,
    add column fwd_channel_id  bigint null,
    add column fwd_channel_post integer null;
