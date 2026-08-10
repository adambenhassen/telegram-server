package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// searchGlobal runs messages.searchGlobal for the caller, from the first page.
func searchGlobal(s *store.Store, userID int64, q string, limit int) (bin.Encoder, error) {
	return api.SearchGlobalForTest(s, userID, &tg.MessagesSearchGlobalRequest{
		Q:          q,
		Filter:     &tg.InputMessagesFilterEmpty{},
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      limit,
	})
}

// globalSlice asserts the reply is the cross-peer slice form and returns it.
func globalSlice(t *testing.T, enc bin.Encoder) *tg.MessagesMessagesSlice {
	t.Helper()
	assertEncodes(t, enc)
	res, ok := enc.(*tg.MessagesMessagesSlice)
	if !ok {
		t.Fatalf("reply = %T, want *tg.MessagesMessagesSlice", enc)
	}
	if res.Count != len(res.Messages) {
		t.Errorf("count = %d, want the page length %d", res.Count, len(res.Messages))
	}
	return res
}

// globalPeers lists the peer each hit belongs to, in reply order.
func globalPeers(t *testing.T, res *tg.MessagesMessagesSlice) []tg.PeerClass {
	t.Helper()
	peers := make([]tg.PeerClass, len(res.Messages))
	for i, m := range res.Messages {
		msg, ok := m.(*tg.Message)
		if !ok {
			t.Fatalf("message %d = %T, want *tg.Message", i, m)
		}
		peers[i] = msg.PeerID
	}
	return peers
}

func globalTexts(t *testing.T, res *tg.MessagesMessagesSlice) []string {
	t.Helper()
	out := make([]string, len(res.Messages))
	for i, m := range res.Messages {
		msg, ok := m.(*tg.Message)
		if !ok {
			t.Fatalf("message %d = %T, want *tg.Message", i, m)
		}
		out[i] = msg.Message
	}
	return out
}

func hasPeer(peers []tg.PeerClass, want tg.PeerClass) bool {
	for _, p := range peers {
		if p.String() == want.String() {
			return true
		}
	}
	return false
}

// dialogSet seats one caller in a 1:1, a chat and a channel, each carrying one
// message with the searched word plus one without, and returns the peers.
type dialogSet struct {
	caller  store.User
	other   store.User
	chat    store.Chat
	channel store.Channel
}

func seedDialogSet(t *testing.T, s *store.Store, callerPhone, otherPhone string) dialogSet {
	t.Helper()
	ctx := context.Background()
	caller, err := s.CreateUser(ctx, callerPhone)
	if err != nil {
		t.Fatalf("create caller: %v", err)
	}
	other, err := s.CreateUser(ctx, otherPhone)
	if err != nil {
		t.Fatalf("create other: %v", err)
	}
	if _, err = api.SendMessageForTest(s, other.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(other.ID, caller.ID), Message: "the deadline is monday", RandomID: 8801,
	}); err != nil {
		t.Fatalf("send dm: %v", err)
	}
	if _, err = api.SendMessageForTest(s, other.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(other.ID, caller.ID), Message: "unrelated dm", RandomID: 8802,
	}); err != nil {
		t.Fatalf("send unrelated dm: %v", err)
	}
	chat, err := s.CreateChat(ctx, caller.ID, "Crew", []int64{other.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	if _, err = api.SendMessageForTest(s, other.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerChat(other.ID, chat.ID), Message: "chat deadline moved", RandomID: 8803,
	}); err != nil {
		t.Fatalf("send chat message: %v", err)
	}
	channel, err := s.CreateChannel(ctx, caller.ID, "Ops", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, err = sendToChannel(t, s, caller.ID, channel.ID, "channel deadline notice", 8804); err != nil {
		t.Fatalf("post: %v", err)
	}
	if _, err = sendToChannel(t, s, caller.ID, channel.ID, "unrelated chatter", 8805); err != nil {
		t.Fatalf("post unrelated: %v", err)
	}
	return dialogSet{caller: caller, other: other, chat: chat, channel: channel}
}

// A caller in a 1:1, a chat and a channel gets a hit from each in one reply,
// with the peers named and the users and chats a client needs to render them.
func TestSearchGlobalReturnsHitsFromEveryDialogKind(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321001", "+15551321002")

	enc, err := searchGlobal(s, d.caller.ID, "deadline", 10)
	if err != nil {
		t.Fatalf("search global: %v", err)
	}
	res := globalSlice(t, enc)
	if len(res.Messages) != 3 {
		t.Fatalf("messages = %v, want one per peer kind", globalTexts(t, res))
	}
	peers := globalPeers(t, res)
	for _, want := range []tg.PeerClass{
		&tg.PeerUser{UserID: d.other.ID},
		&tg.PeerChat{ChatID: d.chat.ID},
		&tg.PeerChannel{ChannelID: d.channel.ID},
	} {
		if !hasPeer(peers, want) {
			t.Errorf("peer %s missing from %v", want.String(), peers)
		}
	}
	if !res.Inexact {
		t.Error("slice is not marked inexact, but Count is the page and not the corpus")
	}

	// A client renders a hit in a peer it may have no dialog open on, so every
	// peer in the page has to arrive hydrated with it.
	userIDs := map[int64]bool{}
	for _, u := range res.Users {
		userIDs[u.GetID()] = true
	}
	if !userIDs[d.caller.ID] || !userIDs[d.other.ID] {
		t.Errorf("users = %v, want both the caller and the other party", userIDs)
	}
	var sawChat, sawChannel bool
	for _, c := range res.Chats {
		switch v := c.(type) {
		case *tg.Chat:
			sawChat = sawChat || v.ID == d.chat.ID
		case *tg.Channel:
			sawChannel = sawChannel || v.ID == d.channel.ID
		}
	}
	if !sawChat || !sawChannel {
		t.Errorf("chats = %+v, want the chat and the channel", res.Chats)
	}

	// next_rate is the date of the last row served, so the cursor the client
	// echoes back is derived from a row it was actually given.
	rate, ok := res.GetNextRate()
	if !ok {
		t.Fatal("no next_rate on a non-empty page")
	}
	last, ok := res.Messages[len(res.Messages)-1].(*tg.Message)
	if !ok {
		t.Fatalf("last message = %T, want *tg.Message", res.Messages[len(res.Messages)-1])
	}
	if rate != last.Date {
		t.Errorf("next_rate = %d, want the last row's date %d", rate, last.Date)
	}
}

// The channel arm never widens to the union: a second caller who is not in the
// channel never sees its post, a banned member loses it again, and a caller in
// no dialogs at all gets an empty page rather than an error.
func TestSearchGlobalNeverServesChannelsTheCallerIsNotIn(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321011", "+15551321012")

	// The other party shares the dm and the chat but never joined the channel.
	res := globalSlice(t, mustSearchGlobal(t, s, d.other.ID, "deadline"))
	if len(res.Messages) != 2 {
		t.Fatalf("non-member hits = %v, want the dm and the chat only", globalTexts(t, res))
	}
	if hasPeer(globalPeers(t, res), &tg.PeerChannel{ChannelID: d.channel.ID}) {
		t.Error("a non-member was served the channel post")
	}
	for _, c := range res.Chats {
		if ch, isChannel := c.(*tg.Channel); isChannel && ch.ID == d.channel.ID {
			t.Error("a non-member was told the channel exists through Chats")
		}
	}

	// Joining turns the channel hit on; a ban turns it back off, and nothing
	// else the caller can read goes with it.
	joinChannelByInvite(t, s, d.channel, d.other.ID)
	res = globalSlice(t, mustSearchGlobal(t, s, d.other.ID, "deadline"))
	if len(res.Messages) != 3 {
		t.Fatalf("member hits = %v, want the channel post too", globalTexts(t, res))
	}
	until := time.Now().Add(time.Hour)
	if err := s.SetChannelBan(ctx, d.channel.ID, d.caller.ID, d.other.ID, &until, false); err != nil {
		t.Fatalf("ban: %v", err)
	}
	res = globalSlice(t, mustSearchGlobal(t, s, d.other.ID, "deadline"))
	if len(res.Messages) != 2 {
		t.Fatalf("banned member hits = %v, want the channel post gone", globalTexts(t, res))
	}
	if hasPeer(globalPeers(t, res), &tg.PeerChannel{ChannelID: d.channel.ID}) {
		t.Error("a banned member was still served the channel post")
	}

	// A caller in nothing gets an empty page, not an error.
	outsider, err := s.CreateUser(ctx, "+15551321013")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	res = globalSlice(t, mustSearchGlobal(t, s, outsider.ID, "deadline"))
	if len(res.Messages) != 0 {
		t.Fatalf("outsider hits = %v, want none", globalTexts(t, res))
	}
	if _, ok := res.GetNextRate(); ok {
		t.Error("an empty page offers a rate to resume from")
	}
}

func mustSearchGlobal(t *testing.T, s *store.Store, userID int64, q string) bin.Encoder {
	t.Helper()
	enc, err := searchGlobal(s, userID, q, 10)
	if err != nil {
		t.Fatalf("search global for %d: %v", userID, err)
	}
	return enc
}

// inputPeerFor turns a peer named in a reply back into the input peer the next
// page's cursor carries, exactly as a client would.
func inputPeerFor(t *testing.T, viewerID int64, p tg.PeerClass) tg.InputPeerClass {
	t.Helper()
	switch v := p.(type) {
	case *tg.PeerUser:
		return api.InputPeerUser(viewerID, v.UserID)
	case *tg.PeerChat:
		return api.InputPeerChat(viewerID, v.ChatID)
	case *tg.PeerChannel:
		return api.InputPeerChannel(viewerID, v.ChannelID)
	default:
		t.Fatalf("peer %T has no input form", p)
		return nil
	}
}

// Paging with a limit below the total returns every match exactly once,
// newest-first, across peers whose id spaces are not comparable.
func TestSearchGlobalCursorServesEveryMatchExactlyOnce(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321021", "+15551321022")
	// More matches than one page, spread across all three peers.
	if _, err := api.SendMessageForTest(s, d.caller.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(d.caller.ID, d.other.ID), Message: "deadline reply", RandomID: 8811,
	}); err != nil {
		t.Fatalf("send dm: %v", err)
	}
	if _, err := sendToChannel(t, s, d.caller.ID, d.channel.ID, "second deadline post", 8812); err != nil {
		t.Fatalf("post: %v", err)
	}

	full := globalSlice(t, mustSearchGlobal(t, s, d.caller.ID, "deadline"))
	if len(full.Messages) != 5 {
		t.Fatalf("single page = %v, want 5 hits", globalTexts(t, full))
	}

	var paged []string
	req := &tg.MessagesSearchGlobalRequest{
		Q:          "deadline",
		Filter:     &tg.InputMessagesFilterEmpty{},
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      2,
	}
	for range len(full.Messages) + 1 {
		enc, err := api.SearchGlobalForTest(s, d.caller.ID, req)
		if err != nil {
			t.Fatalf("paged search: %v", err)
		}
		page := globalSlice(t, enc)
		if len(page.Messages) == 0 {
			break
		}
		if len(page.Messages) > 2 {
			t.Fatalf("page returned %d hits, over the limit", len(page.Messages))
		}
		paged = append(paged, globalTexts(t, page)...)
		last, ok := page.Messages[len(page.Messages)-1].(*tg.Message)
		if !ok {
			t.Fatalf("last message = %T, want *tg.Message", page.Messages[len(page.Messages)-1])
		}
		rate, ok := page.GetNextRate()
		if !ok {
			t.Fatal("non-empty page carries no next_rate")
		}
		req = &tg.MessagesSearchGlobalRequest{
			Q:          "deadline",
			Filter:     &tg.InputMessagesFilterEmpty{},
			OffsetRate: rate,
			OffsetPeer: inputPeerFor(t, d.caller.ID, last.PeerID),
			OffsetID:   last.ID,
			Limit:      2,
		}
	}

	want := globalTexts(t, full)
	if len(paged) != len(want) {
		t.Fatalf("paged %v, want the same %d hits as one page: %v", paged, len(want), want)
	}
	for i := range want {
		if paged[i] != want[i] {
			t.Fatalf("page sequence diverges at %d: got %q, want %q\npaged: %v", i, paged[i], want[i], paged)
		}
	}
}

// The cursor is client input on every page, whatever its provenance: a forged
// access_hash, a peer the caller may not read, and a membership that ended
// between pages are all refused rather than served.
func TestSearchGlobalRejectsATamperedCursorPeer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321031", "+15551321032")
	mallory, err := s.CreateUser(ctx, "+15551321033")
	if err != nil {
		t.Fatalf("create mallory: %v", err)
	}

	page := func(caller int64, peer tg.InputPeerClass) (bin.Encoder, error) {
		return api.SearchGlobalForTest(s, caller, &tg.MessagesSearchGlobalRequest{
			Q:          "deadline",
			Filter:     &tg.InputMessagesFilterEmpty{},
			OffsetRate: int(time.Now().Add(time.Hour).Unix()),
			OffsetPeer: peer,
			OffsetID:   1 << 20,
			Limit:      10,
		})
	}

	// A forged channel hash is refused before anything is read.
	if _, err = page(mallory.ID, &tg.InputPeerChannel{ChannelID: d.channel.ID, AccessHash: 999}); err == nil {
		t.Fatal("forged channel hash: expected PEER_ID_INVALID, got nil")
	} else {
		rpcError(t, err, "PEER_ID_INVALID")
	}

	// A hash this caller legitimately holds — the derivation is per viewer, and
	// a public channel hands one to non-members — still names a channel they may
	// not read, so it is refused too.
	if _, err = page(mallory.ID, api.InputPeerChannel(mallory.ID, d.channel.ID)); err == nil {
		t.Fatal("non-member cursor: expected PEER_ID_INVALID, got nil")
	} else {
		rpcError(t, err, "PEER_ID_INVALID")
	}

	// A channel id that exists and one that does not are the same refusal, so a
	// cursor is not a way to probe the dense channels.id space.
	if _, err = page(mallory.ID, api.InputPeerChannel(mallory.ID, d.channel.ID+9999)); err == nil {
		t.Fatal("nonexistent channel cursor: expected PEER_ID_INVALID, got nil")
	} else {
		rpcError(t, err, "PEER_ID_INVALID")
	}

	// A chat peer carries no hash at all, so membership is the whole gate.
	if _, err = page(mallory.ID, api.InputPeerChat(mallory.ID, d.chat.ID)); err == nil {
		t.Fatal("non-member chat cursor: expected PEER_ID_INVALID, got nil")
	} else {
		rpcError(t, err, "PEER_ID_INVALID")
	}

	// A member's own cursor works until the membership ends, and then stops the
	// next page rather than serving it: the ban is re-checked per page and is
	// never carried in the offset.
	joinChannelByInvite(t, s, d.channel, d.other.ID)
	if _, err = page(d.other.ID, api.InputPeerChannel(d.other.ID, d.channel.ID)); err != nil {
		t.Fatalf("member cursor: %v", err)
	}
	if err = s.SetChannelBan(ctx, d.channel.ID, d.caller.ID, d.other.ID, nil, true); err != nil {
		t.Fatalf("ban: %v", err)
	}
	if _, err = page(d.other.ID, api.InputPeerChannel(d.other.ID, d.channel.ID)); err == nil {
		t.Fatal("cursor after ban: expected PEER_ID_INVALID, got nil")
	} else {
		rpcError(t, err, "PEER_ID_INVALID")
	}
}

// A chat-create service row is reachable from a keyword search — createChat
// writes it with the title as its text — and it has to arrive with the member
// list its wire action carries, the way messages.search serves the same row.
// Rendering it with an empty Users is the defect this pins.
func TestSearchGlobalHydratesChatCreateServiceRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	alice, err := s.CreateUser(ctx, "+15551321071")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551321072")
	if err != nil {
		t.Fatal(err)
	}

	// The title is the service row's text, so this word is what matches it.
	if _, err = api.CreateChatForTest(s, alice.ID, &tg.MessagesCreateChatRequest{
		Title: "Falcon crew",
		Users: []tg.InputUserClass{api.InputUser(alice.ID, bob.ID)},
	}); err != nil {
		t.Fatalf("create chat: %v", err)
	}

	enc, err := searchGlobal(s, alice.ID, "falcon", 10)
	if err != nil {
		t.Fatalf("search global: %v", err)
	}
	res := globalSlice(t, enc)
	if len(res.Messages) != 1 {
		t.Fatalf("hits = %d, want the create service row", len(res.Messages))
	}
	svc, ok := res.Messages[0].(*tg.MessageService)
	if !ok {
		t.Fatalf("hit = %T, want *tg.MessageService", res.Messages[0])
	}
	action, ok := svc.Action.(*tg.MessageActionChatCreate)
	if !ok {
		t.Fatalf("action = %T, want *tg.MessageActionChatCreate", svc.Action)
	}
	if action.Title != "Falcon crew" {
		t.Errorf("action title = %q, want %q", action.Title, "Falcon crew")
	}
	// Both members, the same list messages.search returns for this row.
	if len(action.Users) != 2 {
		t.Fatalf("action users = %v, want both members", action.Users)
	}
	for _, want := range []int64{alice.ID, bob.ID} {
		var found bool
		for _, id := range action.Users {
			found = found || id == want
		}
		if !found {
			t.Errorf("action users = %v, missing %d", action.Users, want)
		}
	}
	// A client renders none of those ids without the user rows behind them.
	hydrated := map[int64]bool{}
	for _, u := range res.Users {
		hydrated[u.GetID()] = true
	}
	if !hydrated[alice.ID] || !hydrated[bob.ID] {
		t.Errorf("users = %v, want every member the action names", hydrated)
	}
}

// The two input rejections messages.search makes, made the same way here.
func TestSearchGlobalRejectsEmptyQueryAndUnsupportedFilters(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321041", "+15551321042")

	_, err := api.SearchGlobalForTest(s, d.caller.ID, &tg.MessagesSearchGlobalRequest{
		Q: "", Filter: &tg.InputMessagesFilterEmpty{}, OffsetPeer: &tg.InputPeerEmpty{},
	})
	rpcError(t, err, "SEARCH_QUERY_EMPTY")

	_, err = api.SearchGlobalForTest(s, d.caller.ID, &tg.MessagesSearchGlobalRequest{
		Q: "deadline", Filter: &tg.InputMessagesFilterPhotos{}, OffsetPeer: &tg.InputPeerEmpty{},
	})
	rpcError(t, err, "INPUT_FILTER_INVALID")

	long := make([]rune, 501)
	for i := range long {
		long[i] = 'a'
	}
	_, err = api.SearchGlobalForTest(s, d.caller.ID, &tg.MessagesSearchGlobalRequest{
		Q: string(long), Filter: &tg.InputMessagesFilterEmpty{}, OffsetPeer: &tg.InputPeerEmpty{},
	})
	rpcError(t, err, "MESSAGE_TOO_LONG")
}
