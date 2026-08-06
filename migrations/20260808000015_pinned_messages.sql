-- Pinned messages: a nullable pinned_message_id on chats and channels.
--
-- The column stores the local_id of the pinned message. NULL means no message
-- is pinned. Only one message may be pinned at a time per chat/channel.
--
-- The local_id is the message's own local_id within that chat/channel — for
-- chats it is the per-member copy's local_id (identical across members for a
-- given fanout), for channels it is the channel_messages.local_id.

ALTER TABLE chats
    ADD COLUMN pinned_message_id INT;

ALTER TABLE channels
    ADD COLUMN pinned_message_id INT;
