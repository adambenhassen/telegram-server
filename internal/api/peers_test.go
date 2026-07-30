package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestInputPeerChannel(t *testing.T) {
	t.Parallel()
	pt, id, err := api.InputPeer(&tg.InputPeerChannel{ChannelID: 5, AccessHash: 5}, 0)
	if err != nil || pt != store.PeerTypeChannel || id != 5 {
		t.Fatalf("inputPeer(channel 5) = (%d, %d, %v), want (channel, 5, nil)", pt, id, err)
	}
	for _, p := range []tg.InputPeerClass{
		&tg.InputPeerChannel{ChannelID: 5, AccessHash: 6},
		&tg.InputPeerChannel{ChannelID: 0, AccessHash: 0},
	} {
		if _, _, err := api.InputPeer(p, 0); err == nil {
			t.Fatalf("inputPeer(%T %+v) = nil error, want PEER_ID_INVALID", p, p)
		}
	}
}

func TestPeerToTLChannel(t *testing.T) {
	t.Parallel()
	got, ok := api.PeerToTL(store.PeerTypeChannel, 7).(*tg.PeerChannel)
	if !ok || got.ChannelID != 7 {
		t.Fatalf("peerToTL(channel, 7) = %#v, want *tg.PeerChannel{ChannelID: 7}", got)
	}
}

// A channel input peer resolves in inputPeer, and the placeholder access hash it
// validates is derivable from the id, so nothing about naming a channel here is
// an authorization step. sendMessage now has a channel path, and what still has
// to hold is that the path rejects a caller with no participant row: the 1:1
// fallthrough would otherwise read the channel id as a user id and write into
// that account's message rows.
func TestSendMessageRejectsChannelPeerFromNonMember(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	u, err := s.CreateUser(context.Background(), "+15551940001")
	if err != nil {
		t.Fatalf("user: %v", err)
	}
	outsider, err := s.CreateUser(context.Background(), "+15551940002")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	ch, err := s.CreateChannel(context.Background(), u.ID, "news", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	_, err = api.SendMessageForTest(s, outsider.ID, &tg.MessagesSendMessageRequest{
		Peer:     &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.ID},
		Message:  "hi",
		RandomID: 11,
	})
	rpcError(t, err, "PEER_ID_INVALID")
}
