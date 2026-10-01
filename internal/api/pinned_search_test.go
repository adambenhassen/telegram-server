package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func searchPinned(s *store.Store, userID int64, peer tg.InputPeerClass, offsetID, limit int) (bin.Encoder, error) {
	return searchPinnedQuery(s, userID, peer, "", offsetID, limit)
}

func searchPinnedQuery(s *store.Store, userID int64, peer tg.InputPeerClass, query string, offsetID, limit int) (bin.Encoder, error) {
	return api.SearchForTest(s, userID, &tg.MessagesSearchRequest{
		Peer: peer, Q: query, Filter: &tg.InputMessagesFilterPinned{}, OffsetID: offsetID, Limit: limit,
	})
}

func pinnedDialogMessages(t *testing.T, enc bin.Encoder) *tg.MessagesMessages {
	t.Helper()
	res, ok := enc.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("search result = %T, want *tg.MessagesMessages", enc)
	}
	return res
}

func pinnedChannelMessages(t *testing.T, enc bin.Encoder) *tg.MessagesChannelMessages {
	t.Helper()
	res, ok := enc.(*tg.MessagesChannelMessages)
	if !ok {
		t.Fatalf("search result = %T, want *tg.MessagesChannelMessages", enc)
	}
	return res
}

func storedMessageByText(t *testing.T, messages []store.Message, text string) store.Message {
	t.Helper()
	for _, message := range messages {
		if message.Text == text {
			return message
		}
	}
	t.Fatalf("stored messages = %v, want %q", messages, text)
	return store.Message{}
}

func findMessageByText(t *testing.T, messages []tg.MessageClass, text string) *tg.Message {
	t.Helper()
	for _, message := range messages {
		if msg, ok := message.(*tg.Message); ok && msg.Message == text {
			return msg
		}
	}
	t.Fatalf("messages = %v, want %q", messages, text)
	return nil
}

func assertPinnedSearchRejectsStaleMember(t *testing.T, enc bin.Encoder, err error) {
	t.Helper()
	if err == nil {
		switch result := enc.(type) {
		case *tg.MessagesMessages:
			if len(result.Messages) > 0 {
				t.Fatalf("stale member received pinned message(s): %v, want PEER_ID_INVALID", result.Messages)
			}
		case *tg.MessagesChannelMessages:
			if len(result.Messages) > 0 {
				t.Fatalf("stale member received pinned channel message(s): %v, want PEER_ID_INVALID", result.Messages)
			}
		}
		t.Fatalf("stale member search returned %T without an error, want PEER_ID_INVALID", enc)
	}
	rpcError(t, err, "PEER_ID_INVALID")
}

func TestSearchPinnedOneToOnePeerReturnsEmptyAndValidatesViewerHash(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	viewer, err := s.CreateUser(ctx, "+15551298001")
	if err != nil {
		t.Fatalf("create viewer: %v", err)
	}
	peer, err := s.CreateUser(ctx, "+15551298002")
	if err != nil {
		t.Fatalf("create peer: %v", err)
	}
	if _, err := api.SendMessageForTest(s, peer.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(peer.ID, viewer.ID), Message: "ordinary direct message", RandomID: 98001,
	}); err != nil {
		t.Fatalf("seed direct message: %v", err)
	}

	enc, err := searchPinned(s, viewer.ID, api.InputPeerUser(viewer.ID, peer.ID), 0, 100)
	if err != nil {
		t.Fatalf("search pinned 1:1 peer: %v", err)
	}
	res := pinnedDialogMessages(t, enc)
	if len(res.Messages) != 0 {
		t.Fatalf("search result has %d messages, want empty", len(res.Messages))
	}

	_, err = searchPinned(s, viewer.ID, &tg.InputPeerUser{
		UserID: peer.ID, AccessHash: api.DeriveUserHash(viewer.ID+1, peer.ID),
	}, 0, 100)
	rpcError(t, err, "PEER_ID_INVALID")

	_, err = api.SearchForTest(s, viewer.ID, &tg.MessagesSearchRequest{
		Peer: api.InputPeerUser(viewer.ID, peer.ID), Q: "", Filter: &tg.InputMessagesFilterEmpty{},
	})
	rpcError(t, err, "SEARCH_QUERY_EMPTY")

	_, err = api.SearchForTest(s, viewer.ID, &tg.MessagesSearchRequest{
		Peer: api.InputPeerUser(viewer.ID, peer.ID), Q: "", Filter: &tg.InputMessagesFilterPhotos{},
	})
	rpcError(t, err, "INPUT_FILTER_INVALID")
}

func TestSearchPinnedBasicGroupUsesViewerCopyAndTracksCurrentPin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551298011", "+15551298012")
	creator, viewer := users[0], users[1]
	outsider, err := s.CreateUser(ctx, "+15551298013")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}

	if _, err := api.SendMessageForTest(s, viewer.ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerSelf{}, Message: "viewer id seed", RandomID: 98101,
	}); err != nil {
		t.Fatalf("seed viewer id space: %v", err)
	}
	if _, err := api.SendMessageForTest(s, creator.ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, Message: "pinned group text", RandomID: 98102,
	}); err != nil {
		t.Fatalf("send pinned group message: %v", err)
	}
	if _, err := api.SendMessageForTest(s, viewer.ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, Message: "unpinned group text", RandomID: 98103,
	}); err != nil {
		t.Fatalf("send unpinned group message: %v", err)
	}

	creatorHistory, err := s.History(ctx, creator.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil {
		t.Fatalf("creator history: %v", err)
	}
	creatorPin := storedMessageByText(t, creatorHistory, "pinned group text")
	viewerHistory, err := s.History(ctx, viewer.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil {
		t.Fatalf("viewer history: %v", err)
	}
	viewerPin := storedMessageByText(t, viewerHistory, "pinned group text")
	if creatorPin.LocalID == viewerPin.LocalID {
		t.Fatalf("fixture pin IDs both equal %d; viewer copy must use a distinct local ID", creatorPin.LocalID)
	}

	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, ID: int(creatorPin.LocalID),
	}); err != nil {
		t.Fatalf("pin group message: %v", err)
	}

	enc, err := searchPinned(s, viewer.ID, &tg.InputPeerChat{ChatID: chat.ID}, 0, 100)
	if err != nil {
		t.Fatalf("search pinned group: %v", err)
	}
	res := pinnedDialogMessages(t, enc)
	if len(res.Messages) != 1 {
		t.Fatalf("search result = %T with %d messages, want one *tg.MessagesMessages item", enc, len(res.Messages))
	}
	msg := findMessageByText(t, res.Messages, "pinned group text")
	if int64(msg.ID) != viewerPin.LocalID {
		t.Errorf("viewer message id = %d, want viewer-owned copy %d", msg.ID, viewerPin.LocalID)
	}
	if peer, ok := msg.PeerID.(*tg.PeerChat); !ok || peer.ChatID != chat.ID {
		t.Errorf("message peer = %T/%v, want chat %d", msg.PeerID, msg.PeerID, chat.ID)
	}

	enc, err = searchPinned(s, viewer.ID, &tg.InputPeerChat{ChatID: chat.ID}, int(viewerPin.LocalID), 100)
	if err != nil {
		t.Fatalf("search pinned group at offset: %v", err)
	}
	if got := len(pinnedDialogMessages(t, enc).Messages); got != 0 {
		t.Errorf("offset at pinned viewer ID returned %d messages, want 0", got)
	}
	enc, err = searchPinned(s, viewer.ID, &tg.InputPeerChat{ChatID: chat.ID}, int(viewerPin.LocalID+1), 100)
	if err != nil {
		t.Fatalf("search pinned group before pin ID: %v", err)
	}
	if got := len(pinnedDialogMessages(t, enc).Messages); got != 1 {
		t.Errorf("offset above pinned viewer ID returned %d messages, want 1", got)
	}

	_, err = searchPinned(s, outsider.ID, &tg.InputPeerChat{ChatID: chat.ID}, 0, 100)
	rpcError(t, err, "PEER_ID_INVALID")

	if _, err = s.DeleteMessages(ctx, viewer.ID, []int64{viewerPin.LocalID}, false); err != nil {
		t.Fatalf("delete viewer-owned pinned copy: %v", err)
	}
	enc, err = searchPinned(s, viewer.ID, &tg.InputPeerChat{ChatID: chat.ID}, 0, 100)
	if err != nil {
		t.Fatalf("search deleted viewer copy: %v", err)
	}
	if got := len(pinnedDialogMessages(t, enc).Messages); got != 0 {
		t.Errorf("deleted viewer copy returned %d messages, want 0", got)
	}
	enc, err = searchPinned(s, creator.ID, &tg.InputPeerChat{ChatID: chat.ID}, 0, 100)
	if err != nil {
		t.Fatalf("creator search after viewer deletion: %v", err)
	}
	if got := len(pinnedDialogMessages(t, enc).Messages); got != 1 {
		t.Errorf("viewer-local deletion changed creator result count to %d, want 1", got)
	}

	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, Unpin: true,
	}); err != nil {
		t.Fatalf("unpin group message: %v", err)
	}
	enc, err = searchPinned(s, creator.ID, &tg.InputPeerChat{ChatID: chat.ID}, 0, 100)
	if err != nil {
		t.Fatalf("search unpinned group: %v", err)
	}
	if got := len(pinnedDialogMessages(t, enc).Messages); got != 0 {
		t.Errorf("unpinned group search returned %d messages, want 0", got)
	}
}

func TestSearchPinnedChannelReturnsPostAndRejectsUnauthorizedViewers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551298021")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551298022")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551298023")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Pinned", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "pinned channel text", 98201); err != nil {
		t.Fatalf("send pinned channel post: %v", err)
	}
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "unpinned channel text", 98202); err != nil {
		t.Fatalf("send unpinned channel post: %v", err)
	}
	posts, err := s.SearchChannelPosts(ctx, ch.ID, "pinned", 0, 10)
	if err != nil || len(posts) != 1 {
		t.Fatalf("find channel pin target: got %d posts, err %v", len(posts), err)
	}
	pinned := posts[0]
	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: channelPeer(creator.ID, ch.ID), ID: int(pinned.LocalID),
	}); err != nil {
		t.Fatalf("pin channel post: %v", err)
	}

	peer := channelPeer(member.ID, ch.ID)
	enc, err := searchPinned(s, member.ID, peer, 0, 100)
	if err != nil {
		t.Fatalf("search pinned channel: %v", err)
	}
	channelResult := pinnedChannelMessages(t, enc)
	if channelResult.Count != 1 || len(channelResult.Messages) != 1 {
		t.Fatalf("channel result = %T count=%d messages=%d, want one post", enc, channelResult.Count, len(channelResult.Messages))
	}
	msg, ok := channelResult.Messages[0].(*tg.Message)
	if !ok || msg.Message != "pinned channel text" || int64(msg.ID) != pinned.LocalID {
		t.Fatalf("channel message = %T %+v, want pin %d with its text", channelResult.Messages[0], channelResult.Messages[0], pinned.LocalID)
	}

	enc, err = searchPinned(s, member.ID, peer, int(pinned.LocalID), 100)
	if err != nil {
		t.Fatalf("search channel at pin offset: %v", err)
	}
	if got := pinnedChannelMessages(t, enc).Count; got != 0 {
		t.Errorf("offset at channel pin count = %d, want 0", got)
	}
	enc, err = searchPinned(s, member.ID, peer, int(pinned.LocalID+1), 100)
	if err != nil {
		t.Fatalf("search channel before pin offset: %v", err)
	}
	if got := pinnedChannelMessages(t, enc).Count; got != 1 {
		t.Errorf("offset above channel pin count = %d, want 1", got)
	}

	_, err = searchPinned(s, outsider.ID, channelPeer(outsider.ID, ch.ID), 0, 100)
	rpcError(t, err, "PEER_ID_INVALID")
	_, err = searchPinned(s, member.ID, &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: api.DeriveChannelHash(member.ID+1, ch.ID)}, 0, 100)
	rpcError(t, err, "PEER_ID_INVALID")

	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: channelPeer(creator.ID, ch.ID), Unpin: true,
	}); err != nil {
		t.Fatalf("unpin channel post: %v", err)
	}
	enc, err = searchPinned(s, member.ID, peer, 0, 100)
	if err != nil {
		t.Fatalf("search unpinned channel: %v", err)
	}
	if got := pinnedChannelMessages(t, enc).Count; got != 0 {
		t.Fatalf("search unpinned channel count = %d, want 0", got)
	}

	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: channelPeer(creator.ID, ch.ID), ID: int(pinned.LocalID),
	}); err != nil {
		t.Fatalf("repin channel post: %v", err)
	}
	deleteChannelPost(t, ctx, dsn, ch.ID, pinned.LocalID)
	enc, err = searchPinned(s, member.ID, peer, 0, 100)
	if err != nil {
		t.Fatalf("search deleted channel pin: %v", err)
	}
	if got := pinnedChannelMessages(t, enc).Count; got != 0 {
		t.Fatalf("search deleted channel pin count = %d, want 0", got)
	}
}

func TestSearchPinnedGroupFiltersByKeyword(t *testing.T) {
	t.Parallel()
	s, _, _, member, chat, peer, memberPinID := newPinnedSearchGroupFixture(t, "+15551298031")

	enc, err := searchPinnedQuery(s, member.ID, peer, "pinned", 0, 100)
	if err != nil {
		t.Fatalf("search matching pinned group query: %v", err)
	}
	result := pinnedDialogMessages(t, enc)
	if len(result.Messages) != 1 {
		t.Fatalf("matching group result has %d messages, want one", len(result.Messages))
	}
	message := findMessageByText(t, result.Messages, "pinned group text")
	if int64(message.ID) != memberPinID {
		t.Errorf("matching group message id = %d, want viewer id %d", message.ID, memberPinID)
	}
	if peer, ok := message.PeerID.(*tg.PeerChat); !ok || peer.ChatID != chat.ID {
		t.Errorf("matching group peer = %T/%v, want chat %d", message.PeerID, message.PeerID, chat.ID)
	}

	enc, err = searchPinnedQuery(s, member.ID, peer, "unrelatedneedle", 0, 100)
	if err != nil {
		t.Fatalf("search nonmatching pinned group query: %v", err)
	}
	if got := len(pinnedDialogMessages(t, enc).Messages); got != 0 {
		t.Errorf("nonmatching group query returned %d messages, want none", got)
	}
}

func TestSearchPinnedChannelFiltersByKeyword(t *testing.T) {
	t.Parallel()
	fixture := newPinnedSearchChannelFixture(t, "+15551298032")

	enc, err := searchPinnedQuery(fixture.store, fixture.member.ID, fixture.peer, "pinned", 0, 100)
	if err != nil {
		t.Fatalf("search matching pinned channel query: %v", err)
	}
	result := pinnedChannelMessages(t, enc)
	if result.Count != 1 || len(result.Messages) != 1 {
		t.Fatalf("matching channel result count=%d messages=%d, want one", result.Count, len(result.Messages))
	}
	message, ok := result.Messages[0].(*tg.Message)
	if !ok || message.ID != int(fixture.pinID) || message.Message != "pinned channel text" {
		t.Fatalf("matching channel message = %T %+v, want id %d and pinned channel text", result.Messages[0], result.Messages[0], fixture.pinID)
	}

	enc, err = searchPinnedQuery(fixture.store, fixture.member.ID, fixture.peer, "unrelatedneedle", 0, 100)
	if err != nil {
		t.Fatalf("search nonmatching pinned channel query: %v", err)
	}
	result = pinnedChannelMessages(t, enc)
	if result.Count != 0 || len(result.Messages) != 0 {
		t.Errorf("nonmatching channel result count=%d messages=%d, want 0", result.Count, len(result.Messages))
	}
}

func TestSearchPinnedGroupRejectsRemovedMemberUsingOriginalPeer(t *testing.T) {
	t.Parallel()
	s, ctx, creator, member, chat, peer, memberPinID := newPinnedSearchGroupFixture(t, "+15551298033")

	enc, err := searchPinned(s, member.ID, peer, 0, 100)
	if err != nil {
		t.Fatalf("authorized member pinned search: %v", err)
	}
	result := pinnedDialogMessages(t, enc)
	if len(result.Messages) != 1 {
		t.Fatalf("authorized group result has %d messages, want one", len(result.Messages))
	}
	message := findMessageByText(t, result.Messages, "pinned group text")
	if int64(message.ID) != memberPinID {
		t.Fatalf("authorized group message id = %d, want viewer id %d", message.ID, memberPinID)
	}

	if _, _, _, err = s.RemoveChatUser(ctx, chat.ID, member.ID, creator.ID); err != nil {
		t.Fatalf("remove group member: %v", err)
	}
	enc, err = searchPinned(s, member.ID, peer, 0, 100)
	assertPinnedSearchRejectsStaleMember(t, enc, err)
}

func TestSearchPinnedChannelRejectsDepartedMemberUsingOriginalPeer(t *testing.T) {
	t.Parallel()
	fixture := newPinnedSearchChannelFixture(t, "+15551298034")

	enc, err := searchPinned(fixture.store, fixture.member.ID, fixture.peer, 0, 100)
	if err != nil {
		t.Fatalf("authorized channel member pinned search: %v", err)
	}
	assertPinnedChannelMatch(t, enc, fixture.pinID)

	left, err := fixture.store.LeaveChannel(fixture.ctx, fixture.channel.ID, fixture.member.ID)
	if err != nil || !left {
		t.Fatalf("leave channel: left=%v err=%v", left, err)
	}
	enc, err = searchPinned(fixture.store, fixture.member.ID, fixture.peer, 0, 100)
	assertPinnedSearchRejectsStaleMember(t, enc, err)
}

func TestSearchPinnedChannelRejectsBannedMemberUsingOriginalPeer(t *testing.T) {
	t.Parallel()
	fixture := newPinnedSearchChannelFixture(t, "+15551298035")

	enc, err := searchPinned(fixture.store, fixture.member.ID, fixture.peer, 0, 100)
	if err != nil {
		t.Fatalf("authorized channel member pinned search: %v", err)
	}
	assertPinnedChannelMatch(t, enc, fixture.pinID)

	if err := fixture.store.SetChannelBan(fixture.ctx, fixture.channel.ID, fixture.creator.ID, fixture.member.ID, nil, true); err != nil {
		t.Fatalf("ban channel member: %v", err)
	}
	enc, err = searchPinned(fixture.store, fixture.member.ID, fixture.peer, 0, 100)
	assertPinnedSearchRejectsStaleMember(t, enc, err)
}

func newPinnedSearchGroupFixture(t *testing.T, memberPhone string) (*store.Store, context.Context, store.User, store.User, store.Chat, tg.InputPeerClass, int64) {
	t.Helper()
	ctx := context.Background()
	s := openStore(t)
	users, chat := chatWith(t, s, "+15551298030", memberPhone)
	creator, member := users[0], users[1]
	if _, err := api.SendMessageForTest(s, creator.ID, &tg.MessagesSendMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, Message: "pinned group text", RandomID: 98401,
	}); err != nil {
		t.Fatalf("send pinned group message: %v", err)
	}

	creatorHistory, err := s.History(ctx, creator.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil {
		t.Fatalf("creator group history: %v", err)
	}
	creatorPin := storedMessageByText(t, creatorHistory, "pinned group text")
	memberHistory, err := s.History(ctx, member.ID, store.PeerTypeChat, chat.ID, 0, 10)
	if err != nil {
		t.Fatalf("member group history: %v", err)
	}
	memberPin := storedMessageByText(t, memberHistory, "pinned group text")
	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: &tg.InputPeerChat{ChatID: chat.ID}, ID: int(creatorPin.LocalID),
	}); err != nil {
		t.Fatalf("pin group message: %v", err)
	}

	return s, ctx, creator, member, chat, &tg.InputPeerChat{ChatID: chat.ID}, memberPin.LocalID
}

type pinnedSearchChannelFixture struct {
	store   *store.Store
	ctx     context.Context
	creator store.User
	member  store.User
	channel store.Channel
	peer    tg.InputPeerClass
	pinID   int64
}

func newPinnedSearchChannelFixture(t *testing.T, memberPhone string) pinnedSearchChannelFixture {
	t.Helper()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551298040")
	if err != nil {
		t.Fatalf("create channel creator: %v", err)
	}
	member, err := s.CreateUser(ctx, memberPhone)
	if err != nil {
		t.Fatalf("create channel member: %v", err)
	}
	ch, err := s.CreateChannel(ctx, creator.ID, "Pinned Search", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannelByInvite(t, s, ch, member.ID)
	if _, err = sendToChannel(t, s, creator.ID, ch.ID, "pinned channel text", 98402); err != nil {
		t.Fatalf("send pinned channel post: %v", err)
	}
	posts, err := s.SearchChannelPosts(ctx, ch.ID, "pinned", 0, 10)
	if err != nil || len(posts) != 1 {
		t.Fatalf("find channel pin target: got %d posts, err %v", len(posts), err)
	}
	pinned := posts[0]
	if _, err = api.UpdatePinnedMessageForTest(s, creator.ID, &tg.MessagesUpdatePinnedMessageRequest{
		Peer: channelPeer(creator.ID, ch.ID), ID: int(pinned.LocalID),
	}); err != nil {
		t.Fatalf("pin channel post: %v", err)
	}
	return pinnedSearchChannelFixture{
		store: s, ctx: ctx, creator: creator, member: member, channel: ch,
		peer: channelPeer(member.ID, ch.ID), pinID: pinned.LocalID,
	}
}

func assertPinnedChannelMatch(t *testing.T, enc bin.Encoder, pinID int64) {
	t.Helper()
	result := pinnedChannelMessages(t, enc)
	if result.Count != 1 || len(result.Messages) != 1 {
		t.Fatalf("channel result count=%d messages=%d, want one", result.Count, len(result.Messages))
	}
	message, ok := result.Messages[0].(*tg.Message)
	if !ok || message.ID != int(pinID) || message.Message != "pinned channel text" {
		t.Fatalf("channel message = %T %+v, want id %d and pinned channel text", result.Messages[0], result.Messages[0], pinID)
	}
}
