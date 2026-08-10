package api_test

import (
	"context"
	"errors"
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

	// A chat cursor is re-authorized against the predicate that served the row,
	// and the owned arm's is owner_id. A chat the caller has nothing in is
	// therefore accepted and moves the keyset, not refused: refusing it would
	// gate one arm's cursor on another arm's predicate and dead-end paging for a
	// caller who left a chat. It reaches nothing — the arm only ever reads rows
	// this caller owns — so the page comes back empty rather than carrying the
	// chat.
	enc, err := page(mallory.ID, api.InputPeerChat(mallory.ID, d.chat.ID))
	if err != nil {
		t.Fatalf("foreign chat cursor: %v", err)
	}
	if res := globalSlice(t, enc); len(res.Messages) != 0 {
		t.Fatalf("foreign chat cursor served %v", globalTexts(t, res))
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

// A caller removed from a chat keeps searching their retained copies, by
// accepted design, so the sequence that serves one has to be able to continue
// past it. Gating the chat cursor on membership dead-ends that sequence and
// strands every older match in every other peer.
func TestSearchGlobalPaginatesPastAChatTheCallerLeft(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	d := seedDialogSet(t, s, "+15551321081", "+15551321082")

	// The chat hit is newer than the 1:1 one, so a limit of 1 serves the chat
	// copy first and the dm is what the next page has to reach.
	full := globalSlice(t, mustSearchGlobal(t, s, d.caller.ID, "deadline"))
	texts := globalTexts(t, full)
	if len(texts) != 3 {
		t.Fatalf("baseline hits = %v, want 3", texts)
	}

	if _, _, _, err := s.RemoveChatUser(ctx, d.chat.ID, d.caller.ID, d.caller.ID); err != nil {
		t.Fatalf("remove from chat: %v", err)
	}

	// Page one row at a time through the whole sequence; the removal must not
	// stop it, and the retained chat copy is still the caller's own row.
	var paged []string
	req := &tg.MessagesSearchGlobalRequest{
		Q:          "deadline",
		Filter:     &tg.InputMessagesFilterEmpty{},
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      1,
	}
	for range len(texts) + 1 {
		enc, err := api.SearchGlobalForTest(s, d.caller.ID, req)
		if err != nil {
			t.Fatalf("page after removal: %v", err)
		}
		page := globalSlice(t, enc)
		if len(page.Messages) == 0 {
			break
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
			Limit:      1,
		}
	}
	if len(paged) != len(texts) {
		t.Fatalf("paged %v after removal, want the same %d hits: %v", paged, len(texts), texts)
	}
	for i := range texts {
		if paged[i] != texts[i] {
			t.Fatalf("page sequence diverges at %d: got %q, want %q", i, paged[i], texts[i])
		}
	}
}

// A message arriving after a page sequence started may show up at the newest end
// of a fresh search and nowhere else. The wire cursor is whole seconds, so the
// case that catches a second-granularity keyset is an arrival inside the cursor
// row's own second whose peer tuple sorts below it.
func TestSearchGlobalKeepsASequenceStableAgainstSameSecondArrivals(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	d := seedDialogSet(t, s, "+15551321091", "+15551321092")

	// Pin the seed instead of racing the clock: the channel post at a whole
	// second, the owned rows well behind it. The collision this case turns on is
	// then constructed rather than hoped for.
	base := time.Now().Add(-time.Hour).Truncate(time.Second)
	channelExec(t, ctx, dsn,
		`UPDATE channel_messages SET date = $1 WHERE channel_id = $2`, base, d.channel.ID)
	channelExec(t, ctx, dsn,
		`UPDATE messages SET date = $1 WHERE owner_id = $2`, base.Add(-time.Minute), d.caller.ID)

	// Page 1 ends on the channel hit, the highest peer tuple in this second.
	first, err := api.SearchGlobalForTest(s, d.caller.ID, &tg.MessagesSearchGlobalRequest{
		Q:          "deadline",
		Filter:     &tg.InputMessagesFilterEmpty{},
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      1,
	})
	if err != nil {
		t.Fatalf("page 1: %v", err)
	}
	page1 := globalSlice(t, first)
	if len(page1.Messages) != 1 {
		t.Fatalf("page 1 = %v, want one hit", globalTexts(t, page1))
	}
	last, ok := page1.Messages[0].(*tg.Message)
	if !ok {
		t.Fatalf("hit = %T, want *tg.Message", page1.Messages[0])
	}
	if _, isChannel := last.PeerID.(*tg.PeerChannel); !isChannel {
		t.Fatalf("page 1 peer = %T, want the channel hit this case needs", last.PeerID)
	}
	rate, ok := page1.GetNextRate()
	if !ok {
		t.Fatal("page 1 carries no next_rate")
	}

	// A new 1:1 message lands mid-sequence: a microsecond after the cursor row,
	// so inside its second, and with a lower peer tuple. A whole-second keyset
	// admits it on page 2; the row's own timestamp does not.
	if _, err = api.SendMessageForTest(s, d.other.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerUser(d.other.ID, d.caller.ID), Message: "deadline arrived late", RandomID: 8901,
	}); err != nil {
		t.Fatalf("late send: %v", err)
	}
	channelExec(t, ctx, dsn,
		`UPDATE messages SET date = $1 WHERE owner_id = $2 AND message = $3`,
		base.Add(time.Microsecond), d.caller.ID, "deadline arrived late")

	// A fresh search is where the arrival belongs: newest-first, at the top.
	fresh := globalSlice(t, mustSearchGlobal(t, s, d.caller.ID, "deadline"))
	newest, ok := fresh.Messages[0].(*tg.Message)
	if !ok {
		t.Fatalf("newest hit = %T, want *tg.Message", fresh.Messages[0])
	}
	if newest.Message != "deadline arrived late" {
		t.Fatalf("fresh search newest = %q, want the late arrival", newest.Message)
	}
	if newest.Date != rate {
		t.Fatalf("late arrival at %d is not in the cursor row's second %d — the seed no longer builds the case",
			newest.Date, rate)
	}

	rest, err := api.SearchGlobalForTest(s, d.caller.ID, &tg.MessagesSearchGlobalRequest{
		Q:          "deadline",
		Filter:     &tg.InputMessagesFilterEmpty{},
		OffsetRate: rate,
		OffsetPeer: inputPeerFor(t, d.caller.ID, last.PeerID),
		OffsetID:   last.ID,
		Limit:      10,
	})
	if err != nil {
		t.Fatalf("page 2: %v", err)
	}
	// The in-progress sequence must not carry it — it is not lost, the fresh
	// search above is where it belongs.
	for _, text := range globalTexts(t, globalSlice(t, rest)) {
		if text == "deadline arrived late" {
			t.Fatal("a message that arrived after the sequence started was admitted mid-sequence")
		}
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

// A removed member keeps their copy of the create row and loses the roster it
// names. The row is theirs by accepted design; the participant list is read
// live, so serving it ungated would hand them the chat's membership as it stands
// now — including people who joined after they left, with a peer access hash
// derived for them.
func TestSearchGlobalHidesTheRosterFromARemovedMember(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	alice, err := s.CreateUser(ctx, "+15551321101")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551321102")
	if err != nil {
		t.Fatal(err)
	}
	carol, err := s.CreateUser(ctx, "+15551321103")
	if err != nil {
		t.Fatal(err)
	}

	if _, err = api.CreateChatForTest(s, alice.ID, &tg.MessagesCreateChatRequest{
		Title: "Falcon crew",
		Users: []tg.InputUserClass{api.InputUser(alice.ID, bob.ID)},
	}); err != nil {
		t.Fatalf("create chat: %v", err)
	}
	chats, err := s.ChatsForUser(ctx, alice.ID)
	if err != nil || len(chats) != 1 {
		t.Fatalf("locate chat: %d chats, err %v", len(chats), err)
	}
	chatID := chats[0].ID

	// Bob leaves the picture, Carol joins after him: Bob and Carol never shared
	// a peer, so Carol is exactly what must not reach him.
	if _, _, _, err = s.RemoveChatUser(ctx, chatID, bob.ID, alice.ID); err != nil {
		t.Fatalf("remove bob: %v", err)
	}
	if _, _, _, err = s.AddChatUser(ctx, chatID, carol.ID, alice.ID); err != nil {
		t.Fatalf("add carol: %v", err)
	}

	enc, err := searchGlobal(s, bob.ID, "falcon", 10)
	if err != nil {
		t.Fatalf("search global: %v", err)
	}
	res := globalSlice(t, enc)
	// The retained copy is still served — this must not over-correct into
	// dropping the hit, which is the accepted design for the row itself.
	if len(res.Messages) != 1 {
		t.Fatalf("hits = %d, want the retained create row", len(res.Messages))
	}
	svc, ok := res.Messages[0].(*tg.MessageService)
	if !ok {
		t.Fatalf("hit = %T, want *tg.MessageService", res.Messages[0])
	}
	action, ok := svc.Action.(*tg.MessageActionChatCreate)
	if !ok {
		t.Fatalf("action = %T, want *tg.MessageActionChatCreate", svc.Action)
	}
	if len(action.Users) != 0 {
		t.Errorf("action users = %v, want none for a removed member", action.Users)
	}
	// Alice may still appear: she authored the row Bob owns, so the fan-out
	// already decided he may see her, the same reasoning that leaves the
	// add/delete branch's ActionUserID ungated. Carol is the disclosure — she
	// joined after Bob left and reaches him only through the live roster.
	for _, u := range res.Users {
		if u.GetID() == carol.ID {
			t.Error("a removed member was served a user who joined after they left")
		}
	}
}

// Contention reaches the client as a retryable failure. The store proves it
// gives up with that signal rather than an empty page
// (TestSearchGlobalRefillCapReportsContentionNotExhaustion); this pins the other
// half, that the handler forwards it as retryable instead of folding it into the
// 500 every other store failure becomes.
func TestSearchGlobalReportsContentionAsRetryable(t *testing.T) {
	t.Parallel()
	if err := api.SearchGlobalErrorForTest(store.ErrSearchContended); !isFloodWait(err) {
		t.Fatalf("contended page: err = %v, want a retryable FLOOD_WAIT", err)
	}
	if err := api.SearchGlobalErrorForTest(errors.New("boom")); isFloodWait(err) {
		t.Fatal("an ordinary store failure must not be reported as retryable")
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
