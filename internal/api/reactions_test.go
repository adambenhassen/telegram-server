package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// sendDM sends a 1:1 message and returns the sender's stored copy, whose
// PeerLocalID names the recipient's copy of the same message.
func sendDM(t *testing.T, s *store.Store, fromID, toID int64, text string, randomID int64) store.Message {
	t.Helper()
	m, _, _, _, err := s.SendMessage(context.Background(), fromID, toID, text, randomID, 0, 0) //nolint:dogsled // only the stored message is needed here
	if err != nil {
		t.Fatalf("send dm: %v", err)
	}
	return m
}

// sentLocalID reads the caller's own local id off a send reply.
func sentLocalID(t *testing.T, enc bin.Encoder) int64 {
	t.Helper()
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("send reply type = %T", enc)
	}
	for _, u := range ups.Updates {
		if id, ok := u.(*tg.UpdateMessageID); ok {
			return int64(id.ID)
		}
	}
	t.Fatal("send reply carries no updateMessageID")
	return 0
}

// reactionPair seeds A and B, has A send B a message, and has B react to it.
type reactionPair struct {
	a, b     store.User
	aLocalID int64 // A's copy of the message
	bLocalID int64 // B's copy of the same message
}

func seedReactionPair(t *testing.T, s *store.Store, phoneA, phoneB, emoji string) reactionPair {
	t.Helper()
	ctx := context.Background()
	a, err := s.CreateUser(ctx, phoneA)
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := s.CreateUser(ctx, phoneB)
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	sender := sendDM(t, s, a.ID, b.ID, "react to this", 770001)
	if emoji != "" {
		if _, err = s.SendReaction(ctx, b.ID, sender.PeerLocalID, emoji); err != nil {
			t.Fatalf("react: %v", err)
		}
	}
	return reactionPair{a: a, b: b, aLocalID: sender.LocalID, bLocalID: sender.PeerLocalID}
}

// getReactions calls messages.getMessagesReactions and asserts the reply shape.
func getReactions(t *testing.T, s *store.Store, userID int64, peer tg.InputPeerClass, ids ...int) *tg.Updates {
	t.Helper()
	enc, err := api.GetMessagesReactionsForTest(s, userID, &tg.MessagesGetMessagesReactionsRequest{
		Peer: peer, ID: ids,
	})
	if err != nil {
		t.Fatalf("get messages reactions: %v", err)
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("reply type = %T, want *tg.Updates", enc)
	}
	return ups
}

// emojiOf reads the single reaction emoji off an updateMessageReactions.
func emojiOf(t *testing.T, u tg.UpdateClass) (int, string) {
	t.Helper()
	r, ok := u.(*tg.UpdateMessageReactions)
	if !ok {
		t.Fatalf("update type = %T, want *tg.UpdateMessageReactions", u)
	}
	if len(r.Reactions.Results) != 1 {
		t.Fatalf("results = %d, want 1", len(r.Reactions.Results))
	}
	e, ok := r.Reactions.Results[0].Reaction.(*tg.ReactionEmoji)
	if !ok {
		t.Fatalf("reaction type = %T, want *tg.ReactionEmoji", r.Reactions.Results[0].Reaction)
	}
	return r.MsgID, e.Emoticon
}

// The concrete case from the ticket: A asks for its own message plus an id in a
// conversation A is not part of. The reply carries B's reaction for the first
// and says nothing at all about the second (criteria 1 and 2).
func TestGetMessagesReactionsReturnsOwnAndOmitsUnreadable(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	p := seedReactionPair(t, s, "+15551340001", "+15551340002", "❤")

	// A conversation A is not part of, whose message ids A may name freely.
	// local_id is per-owner and starts at 1, so C's FIRST send carries the same
	// id as A's and the request would dedup to A's own message. C sends three
	// times and the test names the third, which exists for C and was never
	// issued to A.
	c, err := s.CreateUser(ctx, "+15551340003")
	if err != nil {
		t.Fatalf("create C: %v", err)
	}
	var outsider store.Message
	for i := range int64(3) {
		outsider = sendDM(t, s, c.ID, p.b.ID, "not for A", 770002+i)
	}
	// The guard needs its own guard: if this id is also one of A's, the request
	// below collapses to a single readable id and the assertion proves nothing
	// about the omission path.
	owned, err := s.MessagesByOwnerLocalIDs(ctx, p.a.ID, []int64{outsider.LocalID})
	if err != nil {
		t.Fatalf("check A's namespace: %v", err)
	}
	if len(owned) != 0 {
		t.Fatalf("outsider local id %d is also one of A's own ids, so the request "+
			"would dedup to A's message and the test would be vacuous", outsider.LocalID)
	}

	ups := getReactions(t, s, p.a.ID, api.InputPeerUser(p.a.ID, p.b.ID),
		int(p.aLocalID), int(outsider.LocalID))
	if len(ups.Updates) != 1 {
		t.Fatalf("updates = %d, want 1 (the unreadable id must be omitted)", len(ups.Updates))
	}
	msgID, emoji := emojiOf(t, ups.Updates[0])
	if int64(msgID) != p.aLocalID || emoji != "❤" {
		t.Fatalf("update = (%d, %q), want (%d, %q)", msgID, emoji, p.aLocalID, "❤")
	}
	r, ok := ups.Updates[0].(*tg.UpdateMessageReactions)
	if !ok {
		t.Fatalf("update type = %T", ups.Updates[0])
	}
	if len(r.Reactions.RecentReactions) != 0 {
		t.Errorf("recent reactions = %d, want 0 (no reactor identity)", len(r.Reactions.RecentReactions))
	}
}

// A chat member reads reactions on a chat message through the same predicate
// the chat read paths already apply, and the update names the chat.
func TestGetMessagesReactionsChatPeer(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	a, err := s.CreateUser(ctx, "+15551349001")
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551349002")
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	chat, err := s.CreateChat(ctx, a.ID, "Crew", []int64{b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}
	enc, err := api.SendMessageForTest(s, a.ID, &tg.MessagesSendMessageRequest{
		Peer: api.InputPeerChat(a.ID, chat.ID), Message: "react in chat", RandomID: 770010,
	})
	if err != nil {
		t.Fatalf("send chat message: %v", err)
	}
	localID := sentLocalID(t, enc)
	if _, err = s.SendReaction(ctx, a.ID, localID, "🎉"); err != nil {
		t.Fatalf("react: %v", err)
	}

	ups := getReactions(t, s, a.ID, api.InputPeerChat(a.ID, chat.ID), int(localID))
	if len(ups.Updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(ups.Updates))
	}
	r, ok := ups.Updates[0].(*tg.UpdateMessageReactions)
	if !ok {
		t.Fatalf("update type = %T", ups.Updates[0])
	}
	if peer, ok := r.Peer.(*tg.PeerChat); !ok || peer.ChatID != chat.ID {
		t.Fatalf("peer = %#v, want PeerChat(%d)", r.Peer, chat.ID)
	}
}

// Criterion 4: the reaction this surface renders is the one getHistory already
// renders for the same message — same emoji, same count, no reactor identity.
func TestGetMessagesReactionsAgreesWithGetHistory(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	p := seedReactionPair(t, s, "+15551341001", "+15551341002", "👍")

	enc, err := api.GetHistoryForTest(s, p.a.ID, &tg.MessagesGetHistoryRequest{
		Peer: api.InputPeerUser(p.a.ID, p.b.ID), Limit: 10,
	})
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	hist, ok := enc.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("history type = %T", enc)
	}
	var fromHistory tg.MessageReactions
	for _, m := range hist.Messages {
		msg, ok := m.(*tg.Message)
		if !ok || int64(msg.ID) != p.aLocalID {
			continue
		}
		r, ok := msg.GetReactions()
		if !ok {
			t.Fatal("history message carries no reactions")
		}
		fromHistory = r
	}

	ups := getReactions(t, s, p.a.ID, api.InputPeerUser(p.a.ID, p.b.ID), int(p.aLocalID))
	if len(ups.Updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(ups.Updates))
	}
	r, ok := ups.Updates[0].(*tg.UpdateMessageReactions)
	if !ok {
		t.Fatalf("update type = %T", ups.Updates[0])
	}
	if r.Reactions.String() != fromHistory.String() {
		t.Errorf("reactions = %s, getHistory renders %s", r.Reactions.String(), fromHistory.String())
	}
}

// Criterion 3: an empty id list, and a list naming only unreadable ids, both
// return an empty result and no error.
func TestGetMessagesReactionsEmptyResults(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	p := seedReactionPair(t, s, "+15551342001", "+15551342002", "❤")
	peer := api.InputPeerUser(p.a.ID, p.b.ID)

	if ups := getReactions(t, s, p.a.ID, peer); len(ups.Updates) != 0 {
		t.Errorf("empty id list: updates = %d, want 0", len(ups.Updates))
	}
	if ups := getReactions(t, s, p.a.ID, peer, 999001, 999002); len(ups.Updates) != 0 {
		t.Errorf("never-issued ids: updates = %d, want 0", len(ups.Updates))
	}
}

// Criterion 5: an id list beyond 100 is refused rather than served or truncated.
func TestGetMessagesReactionsRefusesOversizedIDList(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	p := seedReactionPair(t, s, "+15551343001", "+15551343002", "❤")
	peer := api.InputPeerUser(p.a.ID, p.b.ID)

	ids := make([]int, 0, 101)
	ids = append(ids, int(p.aLocalID))
	for len(ids) < 101 {
		ids = append(ids, 900000+len(ids))
	}
	_, err := api.GetMessagesReactionsForTest(s, p.a.ID, &tg.MessagesGetMessagesReactionsRequest{
		Peer: peer, ID: ids,
	})
	if got := rpcMessage(t, err); got != "LIMIT_INVALID" {
		t.Fatalf("101 ids: err = %v, want LIMIT_INVALID", err)
	}
	// Exactly at the cap is served, so the refusal is the cap and not an
	// off-by-one that would hide a truncation.
	if ups := getReactions(t, s, p.a.ID, peer, ids[:100]...); len(ups.Updates) != 1 {
		t.Fatalf("100 ids: updates = %d, want 1", len(ups.Updates))
	}
	// A repeated id cannot buy work past the cap: dedup happens after the check.
	dup := make([]int, 101)
	for i := range dup {
		dup[i] = int(p.aLocalID)
	}
	_, err = api.GetMessagesReactionsForTest(s, p.a.ID, &tg.MessagesGetMessagesReactionsRequest{
		Peer: peer, ID: dup,
	})
	if got := rpcMessage(t, err); got != "LIMIT_INVALID" {
		t.Fatalf("101 repeats of one readable id: err = %v, want LIMIT_INVALID", err)
	}
}

// Criterion 9: the length check runs before peer resolution and before any
// entitlement check, so an oversized list cannot be used to probe membership of
// a chat that carries no access hash.
func TestGetMessagesReactionsOversizedListPrecedesEntitlement(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	p := seedReactionPair(t, s, "+15551344001", "+15551344002", "❤")

	// A chat A is not a member of, and a chat id that was never issued.
	outsider, err := s.CreateUser(ctx, "+15551344003")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	foreign, err := s.CreateChat(ctx, outsider.ID, "Theirs", []int64{p.b.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	ids := make([]int, 101)
	for i := range ids {
		ids[i] = 900100 + i
	}
	oversized := func(peer tg.InputPeerClass) string {
		_, err := api.GetMessagesReactionsForTest(s, p.a.ID, &tg.MessagesGetMessagesReactionsRequest{
			Peer: peer, ID: ids,
		})
		return rpcMessage(t, err)
	}

	entitled := oversized(api.InputPeerUser(p.a.ID, p.b.ID))
	unentitled := oversized(api.InputPeerChat(p.a.ID, foreign.ID))
	nonexistent := oversized(api.InputPeerChat(p.a.ID, foreign.ID+100000))
	if entitled != "LIMIT_INVALID" {
		t.Fatalf("entitled peer: err = %q, want LIMIT_INVALID", entitled)
	}
	if unentitled != entitled || nonexistent != entitled {
		t.Fatalf("oversized list is a membership oracle: entitled=%q unentitled=%q nonexistent=%q",
			entitled, unentitled, nonexistent)
	}

	// Within the cap the same unentitled peer is refused, which is what makes
	// the ordering above load-bearing rather than vacuous.
	_, err = api.GetMessagesReactionsForTest(s, p.a.ID, &tg.MessagesGetMessagesReactionsRequest{
		Peer: api.InputPeerChat(p.a.ID, foreign.ID), ID: ids[:100],
	})
	if got := rpcMessage(t, err); got != "PEER_ID_INVALID" {
		t.Fatalf("within-cap unentitled peer: err = %v, want PEER_ID_INVALID", err)
	}
}

// Criterion 6: a channel peer is refused with the same error handleSendReaction
// returns for one, and never reaches the owner-keyed read.
func TestGetMessagesReactionsRefusesChannelPeer(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	p := seedReactionPair(t, s, "+15551345001", "+15551345002", "❤")

	ch, err := s.CreateChannel(ctx, p.a.ID, "Ops", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	// The caller owns and administers the channel, so the refusal is about the
	// peer kind rather than about entitlement.
	_, err = api.GetMessagesReactionsForTest(s, p.a.ID, &tg.MessagesGetMessagesReactionsRequest{
		Peer: api.InputPeerChannel(p.a.ID, ch.ID), ID: []int{int(p.aLocalID)},
	})
	if got := rpcMessage(t, err); got != "PEER_ID_INVALID" {
		t.Fatalf("channel peer: err = %v, want PEER_ID_INVALID", err)
	}
}

// Criterion 7: the peer argument is an applied filter. A message the caller
// owns in conversation A is not returned when the request names peer B, and the
// peer on each update comes from the stored row rather than the request.
func TestGetMessagesReactionsFiltersByPeer(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	p := seedReactionPair(t, s, "+15551346001", "+15551346002", "❤")

	other, err := s.CreateUser(ctx, "+15551346003")
	if err != nil {
		t.Fatalf("create other: %v", err)
	}
	sendDM(t, s, p.a.ID, other.ID, "hello", 770003)

	// The id belongs to the A-B conversation; the request names A-other.
	if ups := getReactions(t, s, p.a.ID, api.InputPeerUser(p.a.ID, other.ID), int(p.aLocalID)); len(ups.Updates) != 0 {
		t.Fatalf("wrong-peer id: updates = %d, want 0", len(ups.Updates))
	}

	ups := getReactions(t, s, p.a.ID, api.InputPeerUser(p.a.ID, p.b.ID), int(p.aLocalID))
	if len(ups.Updates) != 1 {
		t.Fatalf("updates = %d, want 1", len(ups.Updates))
	}
	r, ok := ups.Updates[0].(*tg.UpdateMessageReactions)
	if !ok {
		t.Fatalf("update type = %T", ups.Updates[0])
	}
	peer, ok := r.Peer.(*tg.PeerUser)
	if !ok || peer.UserID != p.b.ID {
		t.Fatalf("peer = %#v, want PeerUser(%d) derived from the stored row", r.Peer, p.b.ID)
	}
}

// Criterion 8: a soft-deleted message yields nothing, and the reply is
// indistinguishable from one naming an unreadable id or an id never issued.
func TestGetMessagesReactionsOmitsDeletedIndistinguishably(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()
	p := seedReactionPair(t, s, "+15551347001", "+15551347002", "❤")

	// A second reacted message, which A then deletes on its own side only. The
	// reaction rows survive the delete, so serving them would be a leak.
	second := sendDM(t, s, p.a.ID, p.b.ID, "delete me", 770004)
	if _, err := s.SendReaction(ctx, p.b.ID, second.PeerLocalID, "👍"); err != nil {
		t.Fatalf("react second: %v", err)
	}
	if _, err := s.DeleteMessages(ctx, p.a.ID, []int64{second.LocalID}, false); err != nil {
		t.Fatalf("delete: %v", err)
	}

	peer := api.InputPeerUser(p.a.ID, p.b.ID)
	deleted := getReactions(t, s, p.a.ID, peer, int(second.LocalID))
	if len(deleted.Updates) != 0 {
		t.Fatalf("deleted id: updates = %d, want 0", len(deleted.Updates))
	}
	neverIssued := getReactions(t, s, p.a.ID, peer, 999500)
	if len(neverIssued.Updates) != len(deleted.Updates) {
		t.Fatalf("deleted id distinguishable from never-issued: %d vs %d",
			len(deleted.Updates), len(neverIssued.Updates))
	}

	// The surviving message still answers, so the empty reply above is the
	// delete filter and not a handler that returns nothing at all.
	if ups := getReactions(t, s, p.a.ID, peer, int(p.aLocalID), int(second.LocalID)); len(ups.Updates) != 1 {
		t.Fatalf("mixed list: updates = %d, want 1", len(ups.Updates))
	}
}

// An unauthenticated caller is rejected before anything is read.
func TestGetMessagesReactionsRequiresAuth(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	p := seedReactionPair(t, s, "+15551348001", "+15551348002", "❤")

	_, err := api.GetMessagesReactionsForTest(s, 0, &tg.MessagesGetMessagesReactionsRequest{
		Peer: api.InputPeerUser(p.a.ID, p.b.ID), ID: []int{int(p.aLocalID)},
	})
	if got := rpcMessage(t, err); got != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unauthenticated: err = %v, want AUTH_KEY_UNREGISTERED", err)
	}
}
