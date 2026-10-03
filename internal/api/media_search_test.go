package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/store"
)

func searchSharedMedia(
	s *store.Store, userID int64, peer tg.InputPeerClass, query string,
	filter tg.MessagesFilterClass, offsetID, limit int,
) (bin.Encoder, error) {
	return api.SearchForTest(s, userID, &tg.MessagesSearchRequest{
		Peer: peer, Q: query, Filter: filter, OffsetID: offsetID, Limit: limit,
	})
}

func sharedMediaSlice(t *testing.T, enc bin.Encoder) *tg.MessagesMessagesSlice {
	t.Helper()
	res, ok := enc.(*tg.MessagesMessagesSlice)
	if !ok {
		t.Fatalf("search result = %T, want *tg.MessagesMessagesSlice", enc)
	}
	return res
}

func sharedMediaMessage(t *testing.T, message tg.MessageClass) *tg.Message {
	t.Helper()
	res, ok := message.(*tg.Message)
	if !ok {
		t.Fatalf("search message = %T, want *tg.Message", message)
	}
	return res
}

func sendSearchDocument(
	t *testing.T, s *store.Store, senderID int64, peer tg.InputPeerClass,
	fileID int64, fileName, caption string, randomID int64,
) {
	t.Helper()
	saveParts(t, s, senderID, fileID, []byte("shared-media-search document"))
	enc, err := api.SendMediaForTest(s, senderID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer: peer, Media: uploadedDocument(fileID, 1, fileName, "application/octet-stream"),
		Message: caption, RandomID: randomID,
	})
	if err != nil {
		t.Fatalf("send document %q: %v", caption, err)
	}
	documentOf(t, enc)
}

func TestSearchSharedMediaFiltersAndCountsDialogMessages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	viewer, peer := createSearchUsers(t, ctx, s)
	peerForViewer := api.InputPeerUser(viewer.ID, peer.ID)

	sendSearchDocument(t, s, peer.ID, api.InputPeerUser(peer.ID, viewer.ID), 107401, "contract.pdf", "contract attachment", 107401)
	if _, err := api.SendMessageForTest(s, peer.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(peer.ID, viewer.ID), Message: "review contract at https://example.test/contract", RandomID: 107402,
	}); err != nil {
		t.Fatalf("send URL message: %v", err)
	}
	if _, err := api.SendMessageForTest(s, peer.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(peer.ID, viewer.ID), Message: "contract discussion", RandomID: 107403,
	}); err != nil {
		t.Fatalf("send plain text message: %v", err)
	}
	sendSearchDocument(t, s, viewer.ID, peerForViewer, 107404, "export.csv", "quarterly export", 107404)

	enc, err := searchSharedMedia(s, viewer.ID, peerForViewer, "", &tg.InputMessagesFilterDocument{}, 0, 100)
	if err != nil {
		t.Fatalf("search documents: %v", err)
	}
	result := sharedMediaSlice(t, enc)
	if result.Count != 2 || len(result.Messages) != 2 {
		t.Fatalf("document search count=%d messages=%d, want 2", result.Count, len(result.Messages))
	}
	for i, wantText := range []string{"quarterly export", "contract attachment"} {
		message, ok := result.Messages[i].(*tg.Message)
		if !ok || message.Message != wantText {
			t.Fatalf("document result %d = %T %+v, want %q", i, result.Messages[i], result.Messages[i], wantText)
		}
		media, ok := message.Media.(*tg.MessageMediaDocument)
		if !ok {
			t.Fatalf("document result %q media = %T, want document", wantText, message.Media)
		}
		if _, ok := media.Document.(*tg.Document); !ok {
			t.Fatalf("document result %q payload = %T, want *tg.Document", wantText, media.Document)
		}
		if i > 0 {
			previous := sharedMediaMessage(t, result.Messages[i-1])
			if previous.ID <= message.ID {
				t.Fatalf("document results are not newest-first: previous id %d, current id %d", previous.ID, message.ID)
			}
		}
	}

	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "", &tg.InputMessagesFilterDocument{}, 0, 0)
	if err != nil {
		t.Fatalf("count documents: %v", err)
	}
	result = sharedMediaSlice(t, enc)
	if result.Count != 2 || len(result.Messages) != 0 {
		t.Fatalf("count-only document search count=%d messages=%d, want 2 and no messages", result.Count, len(result.Messages))
	}

	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "", &tg.InputMessagesFilterDocument{}, 0, 1)
	if err != nil {
		t.Fatalf("search first document page: %v", err)
	}
	newestDocumentID := resultID(t, sharedMediaSlice(t, enc))
	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "", &tg.InputMessagesFilterDocument{}, newestDocumentID, 1)
	if err != nil {
		t.Fatalf("page documents: %v", err)
	}
	result = sharedMediaSlice(t, enc)
	if result.Count != 2 || len(result.Messages) != 1 {
		t.Fatalf("second document page count=%d messages=%d, want 2 and one", result.Count, len(result.Messages))
	}
	if got := sharedMediaMessage(t, result.Messages[0]).Message; got != "contract attachment" {
		t.Fatalf("second document page message = %q, want contract attachment", got)
	}

	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "contract", &tg.InputMessagesFilterDocument{}, 0, 100)
	if err != nil {
		t.Fatalf("search documents by keyword: %v", err)
	}
	result = sharedMediaSlice(t, enc)
	if result.Count != 1 || len(result.Messages) != 1 || sharedMediaMessage(t, result.Messages[0]).Message != "contract attachment" {
		t.Fatalf("keyword document search count=%d messages=%v, want only contract attachment", result.Count, result.Messages)
	}

	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "", &tg.InputMessagesFilterURL{}, 0, 100)
	if err != nil {
		t.Fatalf("search URLs: %v", err)
	}
	result = sharedMediaSlice(t, enc)
	if result.Count != 1 || len(result.Messages) != 1 || sharedMediaMessage(t, result.Messages[0]).Message != "review contract at https://example.test/contract" {
		t.Fatalf("URL search count=%d messages=%v, want the one link message", result.Count, result.Messages)
	}

	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "contract", &tg.InputMessagesFilterURL{}, 0, 100)
	if err != nil {
		t.Fatalf("search URLs by keyword: %v", err)
	}
	result = sharedMediaSlice(t, enc)
	if result.Count != 1 || len(result.Messages) != 1 {
		t.Fatalf("keyword URL search count=%d messages=%d, want 1", result.Count, len(result.Messages))
	}
	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "absent", &tg.InputMessagesFilterURL{}, 0, 100)
	if err != nil {
		t.Fatalf("search URLs with no keyword match: %v", err)
	}
	result = sharedMediaSlice(t, enc)
	if result.Count != 0 || len(result.Messages) != 0 {
		t.Fatalf("empty URL search count=%d messages=%d, want 0", result.Count, len(result.Messages))
	}

	enc, err = searchSharedMedia(s, viewer.ID, peerForViewer, "", &tg.InputMessagesFilterPhotos{}, 0, 100)
	if err != nil {
		t.Fatalf("search photos with no representable photo messages: %v", err)
	}
	result = sharedMediaSlice(t, enc)
	if result.Count != 0 || len(result.Messages) != 0 {
		t.Fatalf("photo search count=%d messages=%d, want an empty result", result.Count, len(result.Messages))
	}

	_, err = searchSharedMedia(s, viewer.ID, &tg.InputPeerUser{
		UserID: peer.ID, AccessHash: api.DeriveUserHash(viewer.ID+1, peer.ID),
	}, "", &tg.InputMessagesFilterDocument{}, 0, 100)
	rpcError(t, err, "PEER_ID_INVALID")

	_, err = searchSharedMedia(s, viewer.ID, peerForViewer, "", &tg.InputMessagesFilterEmpty{}, 0, 100)
	rpcError(t, err, "SEARCH_QUERY_EMPTY")
}

func TestSearchSharedMediaUsesViewerOwnedChatCopies(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551297111", "+15551297112")
	creator, viewer := users[0], users[1]
	if _, err := api.SendMessageForTest(s, viewer.ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerSelf{}, Message: "advance viewer message IDs", RandomID: 107411,
	}); err != nil {
		t.Fatalf("advance viewer message IDs: %v", err)
	}
	sendSearchDocument(t, s, creator.ID, &tg.InputPeerChat{ChatID: chat.ID}, 107412, "group.pdf", "group attachment", 107412)

	viewerHistory, err := s.History(ctx, viewer.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil {
		t.Fatalf("read viewer-owned chat copy: %v", err)
	}
	viewerCopy := storedMessageByText(t, viewerHistory, "group attachment")
	creatorHistory, err := s.History(ctx, creator.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil {
		t.Fatalf("read creator-owned chat copy: %v", err)
	}
	creatorCopy := storedMessageByText(t, creatorHistory, "group attachment")
	if creatorCopy.LocalID == viewerCopy.LocalID {
		t.Fatalf("creator and viewer local IDs both equal %d, want distinct ID spaces", creatorCopy.LocalID)
	}

	enc, err := searchSharedMedia(s, viewer.ID, &tg.InputPeerChat{ChatID: chat.ID}, "", &tg.InputMessagesFilterDocument{}, 0, 100)
	if err != nil {
		t.Fatalf("search group documents: %v", err)
	}
	result := sharedMediaSlice(t, enc)
	if result.Count != 1 || len(result.Messages) != 1 {
		t.Fatalf("group document search count=%d messages=%d, want one viewer-owned copy", result.Count, len(result.Messages))
	}
	message, ok := result.Messages[0].(*tg.Message)
	if !ok || int64(message.ID) != viewerCopy.LocalID {
		t.Fatalf("group search message = %T %+v, want viewer local ID %d", result.Messages[0], result.Messages[0], viewerCopy.LocalID)
	}
}

func TestSearchSharedMediaChannelCountsAndRejectsUnauthorizedViewers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551297121")
	if err != nil {
		t.Fatalf("create channel creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551297122")
	if err != nil {
		t.Fatalf("create channel member: %v", err)
	}
	banned, err := s.CreateUser(ctx, "+15551297123")
	if err != nil {
		t.Fatalf("create banned viewer: %v", err)
	}
	departed, err := s.CreateUser(ctx, "+15551297124")
	if err != nil {
		t.Fatalf("create departed viewer: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551297125")
	if err != nil {
		t.Fatalf("create public non-member: %v", err)
	}
	quotaViewer, err := s.CreateUser(ctx, "+15551297126")
	if err != nil {
		t.Fatalf("create quota probe: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Shared Media", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if err := s.EditChannelUsername(ctx, ch.ID, creator.ID, "sharedmediasearch"); err != nil {
		t.Fatalf("make channel public: %v", err)
	}
	for _, user := range []store.User{member, banned, departed} {
		joinChannelByInvite(t, s, ch, user.ID)
	}
	if _, err := sendToChannel(t, s, creator.ID, ch.ID, "channel link https://example.test/channel", 107421); err != nil {
		t.Fatalf("send channel link: %v", err)
	}
	if err := s.SetChannelBan(ctx, ch.ID, creator.ID, banned.ID, nil, true); err != nil {
		t.Fatalf("ban channel viewer: %v", err)
	}
	if left, err := s.LeaveChannel(ctx, ch.ID, departed.ID); err != nil || !left {
		t.Fatalf("remove departed viewer: left=%v err=%v", left, err)
	}

	peer := channelPeer(member.ID, ch.ID)
	enc, err := searchSharedMedia(s, member.ID, peer, "", &tg.InputMessagesFilterURL{}, 0, 100)
	if err != nil {
		t.Fatalf("search channel URLs: %v", err)
	}
	result, ok := enc.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("channel URL result = %T, want *tg.MessagesChannelMessages", enc)
	}
	if result.Count != 1 || len(result.Messages) != 1 {
		t.Fatalf("channel URL result = %T count/messages %d/%d, want one", enc, result.Count, len(result.Messages))
	}
	message, ok := result.Messages[0].(*tg.Message)
	if !ok || message.Message != "channel link https://example.test/channel" {
		t.Fatalf("channel URL message = %T %+v, want the channel link", result.Messages[0], result.Messages[0])
	}

	enc, err = searchSharedMedia(s, member.ID, peer, "", &tg.InputMessagesFilterURL{}, 0, 0)
	if err != nil {
		t.Fatalf("count channel URLs: %v", err)
	}
	result, ok = enc.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("channel URL count-only result = %T, want *tg.MessagesChannelMessages", enc)
	}
	if result.Count != 1 || len(result.Messages) != 0 {
		t.Fatalf("channel URL count-only result = %T count/messages %d/%d, want 1/0", enc, result.Count, len(result.Messages))
	}
	enc, err = searchSharedMedia(s, member.ID, peer, "", &tg.InputMessagesFilterDocument{}, 0, 100)
	if err != nil {
		t.Fatalf("search channel documents with no representable matches: %v", err)
	}
	result, ok = enc.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("channel document result = %T, want *tg.MessagesChannelMessages", enc)
	}
	if result.Count != 0 || len(result.Messages) != 0 {
		t.Fatalf("channel document search count/messages = %d/%d, want 0/0", result.Count, len(result.Messages))
	}

	for _, viewer := range []store.User{banned, departed, outsider} {
		_, err := searchSharedMedia(s, viewer.ID, channelPeer(viewer.ID, ch.ID), "", &tg.InputMessagesFilterURL{}, 0, 100)
		rpcError(t, err, "PEER_ID_INVALID")
	}

	quota := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	probe := func() error {
		_, err := api.SearchForTestWithLimits(s, quotaViewer.ID, quota, &tg.MessagesSearchRequest{
			Peer: channelPeer(quotaViewer.ID, ch.ID), Q: "", Filter: &tg.InputMessagesFilterURL{}, Limit: 100,
		})
		return err
	}
	rpcError(t, probe(), "PEER_ID_INVALID")
	if err := probe(); !isFloodWait(err) {
		t.Fatalf("second public non-member search error = %v, want FLOOD_WAIT after first request was charged", err)
	}
}

func createSearchUsers(t *testing.T, ctx context.Context, s *store.Store) (store.User, store.User) {
	t.Helper()
	viewer, err := s.CreateUser(ctx, "+15551297101")
	if err != nil {
		t.Fatalf("create search viewer: %v", err)
	}
	peer, err := s.CreateUser(ctx, "+15551297102")
	if err != nil {
		t.Fatalf("create search peer: %v", err)
	}
	return viewer, peer
}

func resultID(t *testing.T, result *tg.MessagesMessagesSlice) int {
	t.Helper()
	if len(result.Messages) != 1 {
		t.Fatalf("search returned %d messages, want one", len(result.Messages))
	}
	message, ok := result.Messages[0].(*tg.Message)
	if !ok {
		t.Fatalf("search message = %T, want *tg.Message", result.Messages[0])
	}
	return message.ID
}
