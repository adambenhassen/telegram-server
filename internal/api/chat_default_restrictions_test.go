package api_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/store"
)

type chatWriteStats struct {
	messages int64
	events   int64
}

func setChatDefaultRights(t *testing.T, conn *pgx.Conn, chatID int64, rights ...string) {
	t.Helper()
	if _, err := conn.Exec(context.Background(), `UPDATE chats SET default_banned_rights = $2 WHERE id = $1`, chatID, rights); err != nil {
		t.Fatalf("set chat default rights %v: %v", rights, err)
	}
}

func basicChatWriteStats(t *testing.T, conn *pgx.Conn, chatID int64) chatWriteStats {
	t.Helper()
	ctx := context.Background()
	var got chatWriteStats
	if err := conn.QueryRow(ctx, `
		SELECT count(*) FROM messages WHERE peer_type = $1 AND peer_id = $2
	`, int16(store.PeerTypeChat), chatID).Scan(&got.messages); err != nil {
		t.Fatalf("count chat messages: %v", err)
	}
	if err := conn.QueryRow(ctx, `
		SELECT count(*)
		FROM message_events e
		JOIN messages m ON m.owner_id = e.owner_id AND m.local_id = e.local_id
		WHERE m.peer_type = $1 AND m.peer_id = $2
	`, int16(store.PeerTypeChat), chatID).Scan(&got.events); err != nil {
		t.Fatalf("count chat message events: %v", err)
	}
	return got
}

func assertChatWriteStats(t *testing.T, conn *pgx.Conn, chatID int64, want chatWriteStats) {
	t.Helper()
	if got := basicChatWriteStats(t, conn, chatID); got != want {
		t.Fatalf("chat write stats = %+v, want %+v", got, want)
	}
}

func createDirectMediaSource(
	t *testing.T,
	s *store.Store,
	conn *pgx.Conn,
	sender, recipient store.User,
	clientFileID, randomID int64,
	name, mime string,
	attributes ...tg.DocumentAttributeClass,
) (senderLocalID, recipientLocalID int, fileID int64) {
	t.Helper()
	ctx := context.Background()
	saveParts(t, s, sender.ID, clientFileID, []byte("source document bytes"))
	media := uploadedDocument(clientFileID, 1, name, mime)
	media.Attributes = attributes
	if _, err := api.SendMediaForTest(s, sender.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer: api.InputPeerUser(sender.ID, recipient.ID), Media: media, Message: "forward source", RandomID: randomID,
	}); err != nil {
		t.Fatalf("send direct media source: %v", err)
	}
	if err := conn.QueryRow(ctx, `
		SELECT local_id, file_id FROM messages
		WHERE owner_id = $1 AND peer_type = $2 AND peer_id = $3 AND random_id = $4
	`, sender.ID, int16(store.PeerTypeUser), recipient.ID, randomID).Scan(&senderLocalID, &fileID); err != nil {
		t.Fatalf("read sender media source: %v", err)
	}
	if err := conn.QueryRow(ctx, `
		SELECT local_id FROM messages
		WHERE owner_id = $1 AND peer_type = $2 AND peer_id = $3 AND file_id = $4
	`, recipient.ID, int16(store.PeerTypeUser), sender.ID, fileID).Scan(&recipientLocalID); err != nil {
		t.Fatalf("read recipient media source: %v", err)
	}
	return senderLocalID, recipientLocalID, fileID
}

func forwardUserMediaToChat(
	s *store.Store,
	user store.User,
	sourcePeerID, chatID int64,
	sourceLocalID int,
	randomID int64,
) error {
	_, err := api.ForwardMessagesForTest(s, user.ID, &tg.MessagesForwardMessagesRequest{
		FromPeer: api.InputPeerUser(user.ID, sourcePeerID),
		ID:       []int{sourceLocalID},
		ToPeer:   api.InputPeerChat(user.ID, chatID),
		RandomID: []int64{randomID},
	})
	return err
}

func TestBasicChatTextDefaultsRestrictMembersButPreserveCreatorAndRetries(t *testing.T) {
	t.Parallel()
	for _, right := range []string{"send_messages", "send_plain"} {
		t.Run(right, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

			creator := chatUser(t, s, 7101)
			member := chatUser(t, s, 7102)
			chat, err := s.CreateChat(ctx, creator.ID, "Default restrictions", []int64{member.ID})
			if err != nil {
				t.Fatalf("create chat: %v", err)
			}

			memberSend := &tg.MessagesSendMessageRequest{
				Peer: api.InputPeerChat(member.ID, chat.ID), Message: "member before restriction", RandomID: 71001,
			}
			if _, err = api.SendMessageForTest(s, member.ID, memberSend); err != nil {
				t.Fatalf("unrestricted member send: %v", err)
			}
			if _, err = api.SendMessageForTest(s, creator.ID, &tg.MessagesSendMessageRequest{
				Peer: api.InputPeerChat(creator.ID, chat.ID), Message: "creator before restriction", RandomID: 71002,
			}); err != nil {
				t.Fatalf("unrestricted creator send: %v", err)
			}
			if _, err = api.SendMessageForTest(s, creator.ID, &tg.MessagesSendMessageRequest{
				Peer: api.InputPeerUser(creator.ID, member.ID), Message: "forward source", RandomID: 71003,
			}); err != nil {
				t.Fatalf("create forward source: %v", err)
			}
			var sourceID int
			if err = conn.QueryRow(ctx, `
				SELECT local_id FROM messages
				WHERE owner_id = $1 AND peer_type = $2 AND peer_id = $3
				ORDER BY local_id DESC LIMIT 1
			`, member.ID, int16(store.PeerTypeUser), creator.ID).Scan(&sourceID); err != nil {
				t.Fatalf("read member source message: %v", err)
			}

			setChatDefaultRights(t, conn, chat.ID, right)
			before := basicChatWriteStats(t, conn, chat.ID)

			if _, err = api.SendMessageForTest(s, member.ID, memberSend); err != nil {
				t.Fatalf("committed random-id retry after restriction: %v", err)
			}
			assertChatWriteStats(t, conn, chat.ID, before)

			_, err = api.SendMessageForTest(s, member.ID, &tg.MessagesSendMessageRequest{
				Peer: api.InputPeerChat(member.ID, chat.ID), Message: "blocked member send", RandomID: 71004,
			})
			wantRPC(t, err, "CHAT_WRITE_FORBIDDEN")
			assertChatWriteStats(t, conn, chat.ID, before)

			_, err = api.ForwardMessagesForTest(s, member.ID, &tg.MessagesForwardMessagesRequest{
				FromPeer: api.InputPeerUser(member.ID, creator.ID),
				ID:       []int{sourceID},
				ToPeer:   api.InputPeerChat(member.ID, chat.ID),
				RandomID: []int64{71005},
			})
			wantRPC(t, err, "CHAT_WRITE_FORBIDDEN")
			assertChatWriteStats(t, conn, chat.ID, before)

			if _, err = api.SendMessageForTest(s, creator.ID, &tg.MessagesSendMessageRequest{
				Peer: api.InputPeerChat(creator.ID, chat.ID), Message: "creator remains allowed", RandomID: 71006,
			}); err != nil {
				t.Fatalf("restricted creator send: %v", err)
			}
			want := chatWriteStats{messages: before.messages + 2, events: before.events + 2}
			assertChatWriteStats(t, conn, chat.ID, want)
		})
	}
}

func TestBasicChatDocumentDefaultsRestrictMemberSendAndForward(t *testing.T) {
	t.Parallel()
	for _, right := range []string{"send_messages", "send_media", "send_docs"} {
		t.Run(right, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

			creator := chatUser(t, s, 7201)
			member := chatUser(t, s, 7202)
			chat, err := s.CreateChat(ctx, creator.ID, "Document restrictions", []int64{member.ID})
			if err != nil {
				t.Fatalf("create chat: %v", err)
			}

			// Give the member a document message they can attempt to forward.
			saveParts(t, s, creator.ID, 72011, []byte("source document"))
			if _, err = api.SendMediaForTest(s, creator.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
				Peer: api.InputPeerUser(creator.ID, member.ID), Media: uploadedDocument(72011, 1, "source.txt", "text/plain"),
				Message: "source", RandomID: 72012,
			}); err != nil {
				t.Fatalf("send forward source: %v", err)
			}
			var sourceID int
			if err = conn.QueryRow(ctx, `
				SELECT local_id FROM messages
				WHERE owner_id = $1 AND peer_type = $2 AND peer_id = $3
				ORDER BY local_id DESC LIMIT 1
			`, member.ID, int16(store.PeerTypeUser), creator.ID).Scan(&sourceID); err != nil {
				t.Fatalf("read member source document: %v", err)
			}

			setChatDefaultRights(t, conn, chat.ID, right)
			before := basicChatWriteStats(t, conn, chat.ID)
			saveParts(t, s, member.ID, 72021, []byte("blocked document"))
			_, err = api.SendMediaForTest(s, member.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
				Peer: api.InputPeerChat(member.ID, chat.ID), Media: uploadedDocument(72021, 1, "blocked.txt", "text/plain"),
				Message: "blocked", RandomID: 72022,
			})
			wantRPC(t, err, "CHAT_WRITE_FORBIDDEN")
			assertChatWriteStats(t, conn, chat.ID, before)
			if n, _, _, err := s.UploadPartsSummary(ctx, member.ID, 72021); err != nil || n != 1 {
				t.Fatalf("denied document upload parts = %d, err=%v, want one unconsumed part", n, err)
			}
			var memberFiles int64
			if err = conn.QueryRow(ctx, `SELECT count(*) FROM files WHERE uploader_id = $1`, member.ID).Scan(&memberFiles); err != nil {
				t.Fatalf("count member files: %v", err)
			}
			if memberFiles != 0 {
				t.Fatalf("denied document created %d file rows, want none", memberFiles)
			}

			_, err = api.ForwardMessagesForTest(s, member.ID, &tg.MessagesForwardMessagesRequest{
				FromPeer: api.InputPeerUser(member.ID, creator.ID),
				ID:       []int{sourceID},
				ToPeer:   api.InputPeerChat(member.ID, chat.ID),
				RandomID: []int64{72023},
			})
			wantRPC(t, err, "CHAT_WRITE_FORBIDDEN")
			assertChatWriteStats(t, conn, chat.ID, before)

			// The creator remains exempt from all default restrictions.
			saveParts(t, s, creator.ID, 72031, []byte("creator document"))
			if _, err = api.SendMediaForTest(s, creator.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
				Peer: api.InputPeerChat(creator.ID, chat.ID), Media: uploadedDocument(72031, 1, "creator.txt", "text/plain"),
				Message: "creator", RandomID: 72032,
			}); err != nil {
				t.Fatalf("restricted creator document send: %v", err)
			}
			want := chatWriteStats{messages: before.messages + 2, events: before.events + 2}
			assertChatWriteStats(t, conn, chat.ID, want)
		})
	}
}

func TestBasicChatDocumentRetryRemainsAvailableAfterRestriction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	creator := chatUser(t, s, 7251)
	member := chatUser(t, s, 7252)
	chat, err := s.CreateChat(ctx, creator.ID, "Document retry", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	const clientFileID = 72511
	saveParts(t, s, member.ID, clientFileID, []byte("original document"))
	req := &tg.MessagesSendMediaRequest{
		Peer: api.InputPeerChat(member.ID, chat.ID), Media: uploadedDocument(clientFileID, 1, "retry.txt", "text/plain"),
		Message: "original", RandomID: 72512,
	}
	if _, err = api.SendMediaForTest(s, member.ID, newBlobs(t), api.TestMaxUserStorageBytes, req); err != nil {
		t.Fatalf("initial document send: %v", err)
	}
	before := basicChatWriteStats(t, conn, chat.ID)
	setChatDefaultRights(t, conn, chat.ID, "send_media", "send_docs")

	if _, err = api.SendMediaForTest(s, member.ID, newBlobs(t), api.TestMaxUserStorageBytes, req); err != nil {
		t.Fatalf("committed random-id retry after restriction: %v", err)
	}
	assertChatWriteStats(t, conn, chat.ID, before)
}

func TestBasicChatDocumentSubtypeDefaultsRestrictMemberSend(t *testing.T) {
	t.Parallel()
	cases := []struct {
		right string
		attr  tg.DocumentAttributeClass
	}{
		{right: "send_stickers", attr: &tg.DocumentAttributeSticker{Stickerset: &tg.InputStickerSetEmpty{}}},
		{right: "send_gifs", attr: &tg.DocumentAttributeAnimated{}},
		{right: "send_videos", attr: &tg.DocumentAttributeVideo{}},
		{right: "send_roundvideos", attr: roundVideoAttribute()},
		{right: "send_audios", attr: &tg.DocumentAttributeAudio{}},
		{right: "send_voices", attr: voiceAttribute()},
	}
	for i, tc := range cases {
		t.Run(tc.right, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

			creator := chatUser(t, s, 7401+i*2)
			member := chatUser(t, s, 7402+i*2)
			chat, err := s.CreateChat(ctx, creator.ID, "Document subtype restriction", []int64{member.ID})
			if err != nil {
				t.Fatalf("create chat: %v", err)
			}
			setChatDefaultRights(t, conn, chat.ID, tc.right)
			before := basicChatWriteStats(t, conn, chat.ID)
			fileID := int64(74010 + i)
			saveParts(t, s, member.ID, fileID, []byte("document subtype"))
			media := uploadedDocument(fileID, 1, "subtype.bin", "application/octet-stream")
			media.Attributes = []tg.DocumentAttributeClass{tc.attr}

			_, err = api.SendMediaForTest(s, member.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
				Peer: api.InputPeerChat(member.ID, chat.ID), Media: media, Message: "subtype", RandomID: 74020 + int64(i),
			})
			wantRPC(t, err, "CHAT_WRITE_FORBIDDEN")
			assertChatWriteStats(t, conn, chat.ID, before)
			if n, _, _, err := s.UploadPartsSummary(ctx, member.ID, fileID); err != nil || n != 1 {
				t.Fatalf("denied subtype upload parts = %d, err=%v, want one unconsumed part", n, err)
			}
			var files int64
			if err = conn.QueryRow(ctx, `SELECT count(*) FROM files WHERE uploader_id = $1`, member.ID).Scan(&files); err != nil {
				t.Fatalf("count member files: %v", err)
			}
			if files != 0 {
				t.Fatalf("denied subtype created %d file rows, want none", files)
			}
		})
	}
}

func roundVideoAttribute() tg.DocumentAttributeClass {
	attr := &tg.DocumentAttributeVideo{}
	attr.SetRoundMessage(true)
	return attr
}

func voiceAttribute() tg.DocumentAttributeClass {
	attr := &tg.DocumentAttributeAudio{}
	attr.SetVoice(true)
	return attr
}

func TestBasicChatForwardedDocumentSubtypeRestrictions(t *testing.T) {
	t.Parallel()
	cases := []struct {
		right string
		attr  tg.DocumentAttributeClass
	}{
		{right: "send_stickers", attr: &tg.DocumentAttributeSticker{Stickerset: &tg.InputStickerSetEmpty{}}},
		{right: "send_gifs", attr: &tg.DocumentAttributeAnimated{}},
		{right: "send_videos", attr: &tg.DocumentAttributeVideo{}},
		{right: "send_roundvideos", attr: roundVideoAttribute()},
		{right: "send_audios", attr: &tg.DocumentAttributeAudio{}},
		{right: "send_voices", attr: voiceAttribute()},
	}
	for i, tc := range cases {
		t.Run(tc.right, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

			creator := chatUser(t, s, 7601+i*2)
			member := chatUser(t, s, 7602+i*2)
			chat, err := s.CreateChat(ctx, creator.ID, "Forwarded subtype restriction", []int64{member.ID})
			if err != nil {
				t.Fatalf("create chat: %v", err)
			}
			memberSourceID, creatorSourceID, fileID := createDirectMediaSource(
				t, s, conn, member, creator, int64(76100+i), int64(76110+i),
				"source.bin", "application/octet-stream", tc.attr,
			)
			files, err := s.FilesByIDs(ctx, []int64{fileID})
			if err != nil {
				t.Fatalf("load source file subtype: %v", err)
			}
			if got := files[fileID].SubtypeRights; len(got) != 1 || got[0] != tc.right {
				t.Fatalf("stored subtype rights = %v, want [%s]", got, tc.right)
			}

			before := basicChatWriteStats(t, conn, chat.ID)
			memberPts := apiPts(t, s, member.ID)
			creatorPts := apiPts(t, s, creator.ID)
			retryID := int64(76200 + i)
			if tc.right == "send_stickers" {
				if err = forwardUserMediaToChat(s, member, creator.ID, chat.ID, memberSourceID, retryID); err != nil {
					t.Fatalf("initial unrestricted forward: %v", err)
				}
				before = basicChatWriteStats(t, conn, chat.ID)
				memberPts = apiPts(t, s, member.ID)
				creatorPts = apiPts(t, s, creator.ID)
			}
			setChatDefaultRights(t, conn, chat.ID, tc.right)
			if tc.right == "send_stickers" {
				if err = forwardUserMediaToChat(s, member, creator.ID, chat.ID, memberSourceID, retryID); err != nil {
					t.Fatalf("committed forward retry after restriction: %v", err)
				}
				assertChatWriteStats(t, conn, chat.ID, before)
				if got := apiPts(t, s, member.ID); got != memberPts {
					t.Fatalf("member pts after committed retry = %d, want %d", got, memberPts)
				}
				if got := apiPts(t, s, creator.ID); got != creatorPts {
					t.Fatalf("creator pts after committed retry = %d, want %d", got, creatorPts)
				}
			}

			wantRPC(t, forwardUserMediaToChat(s, member, creator.ID, chat.ID, memberSourceID, int64(76300+i)), "CHAT_WRITE_FORBIDDEN")
			assertChatWriteStats(t, conn, chat.ID, before)
			if got := apiPts(t, s, member.ID); got != memberPts {
				t.Fatalf("member pts after denied forward = %d, want %d", got, memberPts)
			}
			if got := apiPts(t, s, creator.ID); got != creatorPts {
				t.Fatalf("creator pts after denied forward = %d, want %d", got, creatorPts)
			}

			// The creator remains exempt for every document subtype.
			if err = forwardUserMediaToChat(s, creator, member.ID, chat.ID, creatorSourceID, int64(76400+i)); err != nil {
				t.Fatalf("restricted creator forward: %v", err)
			}
			assertChatWriteStats(t, conn, chat.ID, chatWriteStats{
				messages: before.messages + 2,
				events:   before.events + 2,
			})
		})
	}
}

func TestBasicChatGenericAndUnknownSubtypeForwardPolicy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	creator := chatUser(t, s, 7801)
	member := chatUser(t, s, 7802)
	chat, err := s.CreateChat(ctx, creator.ID, "Unknown subtype forward policy", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	genericSourceID, _, genericFileID := createDirectMediaSource(
		t, s, conn, member, creator, 78101, 78111, "generic.bin", "application/octet-stream",
	)
	unknownSourceID, _, unknownFileID := createDirectMediaSource(
		t, s, conn, member, creator, 78102, 78112, "sticker.webp", "image/webp", &tg.DocumentAttributeHasStickers{},
	)
	legacySourceID, _, legacyFileID := createDirectMediaSource(
		t, s, conn, member, creator, 78103, 78113, "legacy.bin", "application/octet-stream",
	)
	if _, err = conn.Exec(ctx, `UPDATE files SET subtype_rights = NULL WHERE id = $1`, legacyFileID); err != nil {
		t.Fatalf("mark legacy subtype unknown: %v", err)
	}

	files, err := s.FilesByIDs(ctx, []int64{genericFileID, unknownFileID, legacyFileID})
	if err != nil {
		t.Fatalf("load source file subtypes: %v", err)
	}
	if got := files[genericFileID].SubtypeRights; got == nil || len(got) != 0 {
		t.Fatalf("generic subtype rights = %v, want known empty set", got)
	}
	if got := files[unknownFileID].SubtypeRights; got != nil {
		t.Fatalf("ambiguous subtype rights = %v, want unknown", got)
	}
	if got := files[legacyFileID].SubtypeRights; got != nil {
		t.Fatalf("legacy subtype rights = %v, want unknown", got)
	}

	setChatDefaultRights(t, conn, chat.ID, "send_stickers")
	if err = forwardUserMediaToChat(s, member, creator.ID, chat.ID, genericSourceID, 78201); err != nil {
		t.Fatalf("known generic forward under send_stickers restriction: %v", err)
	}

	assertUnknownDenied := func(sourceID int, randomID int64) {
		t.Helper()
		before := basicChatWriteStats(t, conn, chat.ID)
		memberPts := apiPts(t, s, member.ID)
		creatorPts := apiPts(t, s, creator.ID)
		wantRPC(t, forwardUserMediaToChat(s, member, creator.ID, chat.ID, sourceID, randomID), "CHAT_WRITE_FORBIDDEN")
		assertChatWriteStats(t, conn, chat.ID, before)
		if got := apiPts(t, s, member.ID); got != memberPts {
			t.Fatalf("member pts after denied unknown forward = %d, want %d", got, memberPts)
		}
		if got := apiPts(t, s, creator.ID); got != creatorPts {
			t.Fatalf("creator pts after denied unknown forward = %d, want %d", got, creatorPts)
		}
	}

	for i, right := range []string{"send_stickers", "send_gifs", "send_videos", "send_roundvideos", "send_audios", "send_voices"} {
		setChatDefaultRights(t, conn, chat.ID, right)
		assertUnknownDenied(unknownSourceID, int64(78300+i))
	}

	if _, err = conn.Exec(ctx, `UPDATE chats SET default_banned_rights = '{}' WHERE id = $1`, chat.ID); err != nil {
		t.Fatalf("clear subtype restrictions: %v", err)
	}
	before := basicChatWriteStats(t, conn, chat.ID)
	if err = forwardUserMediaToChat(s, member, creator.ID, chat.ID, unknownSourceID, 78401); err != nil {
		t.Fatalf("ambiguous unknown forward without subtype restrictions: %v", err)
	}
	assertChatWriteStats(t, conn, chat.ID, chatWriteStats{messages: before.messages + 2, events: before.events + 2})

	setChatDefaultRights(t, conn, chat.ID, "send_videos")
	assertUnknownDenied(legacySourceID, 78402)
	if _, err = conn.Exec(ctx, `UPDATE chats SET default_banned_rights = '{}' WHERE id = $1`, chat.ID); err != nil {
		t.Fatalf("clear legacy subtype restriction: %v", err)
	}
	before = basicChatWriteStats(t, conn, chat.ID)
	if err = forwardUserMediaToChat(s, member, creator.ID, chat.ID, legacySourceID, 78403); err != nil {
		t.Fatalf("legacy unknown forward without subtype restrictions: %v", err)
	}
	assertChatWriteStats(t, conn, chat.ID, chatWriteStats{messages: before.messages + 2, events: before.events + 2})
}

func TestBasicChatUnavailableMediaRightsStayUnavailable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		right string
		media tg.InputMediaClass
	}{
		{right: "send_photos", media: &tg.InputMediaUploadedPhoto{File: &tg.InputFile{ID: 75001, Parts: 1, Name: "photo.png"}}},
		{right: "send_polls", media: &tg.InputMediaPoll{}},
		{right: "send_games", media: &tg.InputMediaGame{ID: &tg.InputGameID{ID: 1, AccessHash: 1}}},
	}
	for _, tc := range cases {
		t.Run(tc.right, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			conn, err := pgx.Connect(ctx, dsn)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

			creator := chatUser(t, s, 7501)
			member := chatUser(t, s, 7502)
			chat, err := s.CreateChat(ctx, creator.ID, "Unavailable media", []int64{member.ID})
			if err != nil {
				t.Fatalf("create chat: %v", err)
			}
			setChatDefaultRights(t, conn, chat.ID, tc.right)
			before := basicChatWriteStats(t, conn, chat.ID)
			_, err = api.SendMediaForTest(s, member.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
				Peer: api.InputPeerChat(member.ID, chat.ID), Media: tc.media, Message: "unavailable", RandomID: 75003,
			})
			wantRPC(t, err, "MEDIA_INVALID")
			assertChatWriteStats(t, conn, chat.ID, before)
		})
	}
}

func TestBasicChatInlineSendingRemainsUnavailable(t *testing.T) {
	t.Parallel()
	var request bin.Buffer
	if err := (&tg.MessagesSendInlineBotResultRequest{
		Peer: api.InputPeerChat(1, 1), RandomID: 75101, QueryID: 1, ID: "result",
	}).Encode(&request); err != nil {
		t.Fatalf("encode inline send: %v", err)
	}
	err := api.UnhandledForTest(slog.New(slog.DiscardHandler), unhandledConn(), &request)
	wantRPC(t, err, "INPUT_METHOD_INVALID")
}

func TestBasicChatTopicManagementRemainsUnavailable(t *testing.T) {
	t.Parallel()
	var request bin.Buffer
	if err := (&tg.MessagesCreateForumTopicRequest{
		Peer: api.InputPeerChat(1, 1), Title: "topic", RandomID: 75201,
	}).Encode(&request); err != nil {
		t.Fatalf("encode topic create: %v", err)
	}
	err := api.UnhandledForTest(slog.New(slog.DiscardHandler), unhandledConn(), &request)
	wantRPC(t, err, "INPUT_METHOD_INVALID")
}

func TestBasicChatEmbedLinksRestrictionHasNoPreviewAction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	creator := chatUser(t, s, 7521)
	member := chatUser(t, s, 7522)
	chat, err := s.CreateChat(ctx, creator.ID, "No previews", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	setChatDefaultRights(t, conn, chat.ID, "embed_links")
	enc, err := api.SendMessageForTest(s, member.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerChat(member.ID, chat.ID), Message: "https://example.invalid/page", RandomID: 75203,
	})
	if err != nil {
		t.Fatalf("text send with embed_links restriction: %v", err)
	}
	updates, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result = %T, want *tg.Updates", enc)
	}
	for _, update := range updates.Updates {
		newMessage, ok := update.(*tg.UpdateNewMessage)
		if !ok {
			continue
		}
		message, ok := newMessage.Message.(*tg.Message)
		if !ok {
			t.Fatalf("message = %T, want *tg.Message", newMessage.Message)
		}
		if _, ok = message.Media.(*tg.MessageMediaWebPage); ok {
			t.Fatal("basic chat generated a webpage preview")
		}
		return
	}
	t.Fatal("send result omitted updateNewMessage")
}

func TestBasicChatControlDefaultsRemainCreatorOnly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	creator := chatUser(t, s, 7531)
	member := chatUser(t, s, 7532)
	chat, err := s.CreateChat(ctx, creator.ID, "Creator controls", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	if _, err = api.SendMessageForTest(s, creator.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerChat(creator.ID, chat.ID), Message: "pin target", RandomID: 75303,
	}); err != nil {
		t.Fatalf("create pin target: %v", err)
	}

	setChatDefaultRights(t, conn, chat.ID, "change_info")
	_, err = api.EditChatTitleForTest(s, member.ID, &tg.MessagesEditChatTitleRequest{
		ChatID: chat.ID, Title: "member title",
	})
	wantRPC(t, err, "PEER_ID_INVALID")
	if _, err = api.EditChatTitleForTest(s, creator.ID, &tg.MessagesEditChatTitleRequest{
		ChatID: chat.ID, Title: "creator title",
	}); err != nil {
		t.Fatalf("creator title change under default restriction: %v", err)
	}

	setChatDefaultRights(t, conn, chat.ID, "pin_messages")
	_, err = api.UpdatePinnedMessageForTest(s, member.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: api.InputPeerChat(member.ID, chat.ID), ID: 1,
	})
	wantRPC(t, err, "CHAT_ADMIN_REQUIRED")
	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: api.InputPeerChat(creator.ID, chat.ID), ID: 1,
	}); err != nil {
		t.Fatalf("creator pin under default restriction: %v", err)
	}
}

func TestBasicChatInviteRestrictionPreservesCreatorAddAndMemberRemoval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	creator := chatUser(t, s, 7301)
	member := chatUser(t, s, 7302)
	target := chatUser(t, s, 7303)
	chat, err := s.CreateChat(ctx, creator.ID, "Invite restriction", []int64{member.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	setChatDefaultRights(t, conn, chat.ID, "invite_users", "send_messages")
	before := basicChatWriteStats(t, conn, chat.ID)

	_, err = api.AddChatUserForTest(s, member.ID, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID, UserID: api.InputUser(member.ID, target.ID),
	})
	wantRPC(t, err, "CHAT_WRITE_FORBIDDEN")
	if got := apiParticipants(t, s, chat.ID); len(got) != 2 {
		t.Fatalf("participants after denied invite = %v, want creator and member", got)
	}
	assertChatWriteStats(t, conn, chat.ID, before)

	// send_messages restricts client posts, while a committed member removal
	// still writes its service announcement to both the remaining and removed user.
	if _, err = api.DeleteChatUserForTest(s, member.ID, &tg.MessagesDeleteChatUserRequest{
		ChatID: chat.ID, UserID: api.InputUser(member.ID, member.ID),
	}); err != nil {
		t.Fatalf("member self-removal: %v", err)
	}
	afterRemoval := chatWriteStats{messages: before.messages + 2, events: before.events + 2}
	assertChatWriteStats(t, conn, chat.ID, afterRemoval)
	if got := apiParticipants(t, s, chat.ID); len(got) != 1 || got[0] != creator.ID {
		t.Fatalf("participants after member leave = %v, want creator only", got)
	}

	if _, err = api.AddChatUserForTest(s, creator.ID, &tg.MessagesAddChatUserRequest{
		ChatID: chat.ID, UserID: api.InputUser(creator.ID, target.ID),
	}); err != nil {
		t.Fatalf("creator add under invite restriction: %v", err)
	}
}
