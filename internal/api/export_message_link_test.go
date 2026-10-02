package api_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

const (
	exportMessageLinkMethodID    = 0xe63fadeb
	exportedMessageLinkResultID  = 0x5dab1af4
	exportMessageLinkGroupedFlag = 1 << 0
	exportMessageLinkThreadFlag  = 1 << 1
)

type exportMessageLinkWireRequest struct {
	channel tg.InputChannelClass
	id      int
	grouped bool
	thread  bool
}

func (r exportMessageLinkWireRequest) Encode(b *bin.Buffer) error {
	flags := 0
	if r.grouped {
		flags |= exportMessageLinkGroupedFlag
	}
	if r.thread {
		flags |= exportMessageLinkThreadFlag
	}
	b.PutID(exportMessageLinkMethodID)
	b.PutInt(flags)
	if err := r.channel.Encode(b); err != nil {
		return err
	}
	b.PutInt(r.id)
	return nil
}

type exportedMessageLinkWireResponse struct {
	Link string
	HTML string
}

func (r *exportedMessageLinkWireResponse) Decode(b *bin.Buffer) error {
	if err := b.ConsumeID(exportedMessageLinkResultID); err != nil {
		return err
	}
	link, err := b.String()
	if err != nil {
		return err
	}
	html, err := b.String()
	if err != nil {
		return err
	}
	r.Link, r.HTML = link, html
	return nil
}

func exportMessageLinkCall(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	channel tg.InputChannelClass,
	messageID int,
	grouped, thread bool,
) (string, string, *mt.RPCError, []byte) {
	t.Helper()
	body := dispatchSettings(t, h, settingsHandler{
		name: "channels.exportMessageLink",
		request: func() bin.Encoder {
			return exportMessageLinkWireRequest{
				channel: channel,
				id:      messageID,
				grouped: grouped,
				thread:  thread,
			}
		},
	}, userID, false)
	var result exportedMessageLinkWireResponse
	if err := result.Decode(&bin.Buffer{Buf: body}); err == nil {
		return result.Link, result.HTML, nil, body
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode exportMessageLink response: %v", err)
	}
	return "", "", &rpc, body
}

func TestExportMessageLinkRequiresAuthentication(t *testing.T) {
	link, html, rpc, wire := exportMessageLinkCall(
		t, fullChannelDispatcher(nil), 0, api.InputChannel(7, 1), 1, false, false,
	)
	if rpc == nil || rpc.ErrorMessage != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("unauthenticated error = %v, want AUTH_KEY_UNREGISTERED", rpc)
	}
	if link != "" || html != "" || len(wire) == 0 {
		t.Fatalf("unauthenticated response = {%q, %q, %x}, want an RPC error", link, html, wire)
	}
}

func TestExportMessageLinkReturnsCurrentPublicAndPrivateAddresses(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	creator, err := s.CreateUser(ctx, "+15551293001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	public, err := s.CreateChannel(ctx, creator.ID, "Public", "", false)
	if err != nil {
		t.Fatalf("create public channel: %v", err)
	}
	if err := api.ClaimChannelUsernameForTest(s, public.ID, "currentnews"); err != nil {
		t.Fatalf("set public username: %v", err)
	}
	if _, err := sendToChannel(t, s, creator.ID, public.ID, "public post", 93001); err != nil {
		t.Fatalf("post to public channel: %v", err)
	}

	private, err := s.CreateChannel(ctx, creator.ID, "Private", "", false)
	if err != nil {
		t.Fatalf("create private channel: %v", err)
	}
	if _, err := sendToChannel(t, s, creator.ID, private.ID, "private post", 93002); err != nil {
		t.Fatalf("post to private channel: %v", err)
	}

	const prefix = "https://links.example/"
	h := fullChannelDispatcher(s, prefix)
	publicLink, publicHTML, rpc, _ := exportMessageLinkCall(
		t, h, creator.ID, api.InputChannel(creator.ID, public.ID), 1, true, true,
	)
	if rpc != nil {
		t.Fatalf("export public link: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if want := prefix + "currentnews/1"; publicLink != want {
		t.Errorf("public link = %q, want %q", publicLink, want)
	}
	if publicHTML != "" {
		t.Errorf("public link html = %q, want empty", publicHTML)
	}

	if err := s.EditChannelUsername(ctx, public.ID, creator.ID, "newscurrent"); err != nil {
		t.Fatalf("change public username: %v", err)
	}
	updatedLink, _, rpc, _ := exportMessageLinkCall(
		t, h, creator.ID, api.InputChannel(creator.ID, public.ID), 1, false, false,
	)
	if rpc != nil {
		t.Fatalf("export link after username change: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if want := prefix + "newscurrent/1"; updatedLink != want {
		t.Errorf("link after username change = %q, want %q", updatedLink, want)
	}
	otherOriginLink, _, rpc, _ := exportMessageLinkCall(
		t, fullChannelDispatcher(s, "https://alternate.example/"), creator.ID,
		api.InputChannel(creator.ID, public.ID), 1, false, false,
	)
	if rpc != nil {
		t.Fatalf("export link with alternate origin: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if want := "https://alternate.example/newscurrent/1"; otherOriginLink != want {
		t.Errorf("link with alternate origin = %q, want %q", otherOriginLink, want)
	}

	privateLink, _, rpc, _ := exportMessageLinkCall(
		t, h, creator.ID, api.InputChannel(creator.ID, private.ID), 1, false, false,
	)
	if rpc != nil {
		t.Fatalf("export private link: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	wantPrivate := prefix + "c/" + strconv.FormatInt(private.ID, 10) + "/1"
	if privateLink != wantPrivate {
		t.Errorf("private link = %q, want %q", privateLink, wantPrivate)
	}
}

func TestExportMessageLinkDenialsAreByteIdentical(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator, err := s.CreateUser(ctx, "+15551293011")
	if err != nil {
		t.Fatalf("create creator: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551293012")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551293013")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	banRecipient, err := s.CreateUser(ctx, "+15551293014")
	if err != nil {
		t.Fatalf("create banned member: %v", err)
	}
	channel, err := s.CreateChannel(ctx, creator.ID, "Private", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	joinChannel(t, ctx, dsn, channel.ID, member.ID)
	joinChannel(t, ctx, dsn, channel.ID, banRecipient.ID)
	if _, err := sendToChannel(t, s, creator.ID, channel.ID, "active post", 93101); err != nil {
		t.Fatalf("post active message: %v", err)
	}
	if _, err := sendToChannel(t, s, creator.ID, channel.ID, "deleted post", 93102); err != nil {
		t.Fatalf("post deleted message: %v", err)
	}
	channelExec(t, ctx, dsn,
		`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`, channel.ID, 2)
	banChannelMember(t, ctx, dsn, channel.ID, banRecipient.ID, time.Now().Add(time.Hour))

	wrongHash := api.InputChannel(creator.ID, channel.ID)
	wrongHash.AccessHash ^= 1
	h := fullChannelDispatcher(s)
	unknownChannel := api.InputChannel(creator.ID, channel.ID+1)
	cases := []struct {
		name    string
		userID  int64
		channel tg.InputChannelClass
		id      int
	}{
		{name: "unknown channel", userID: creator.ID, channel: unknownChannel, id: 1},
		{name: "wrong access hash", userID: creator.ID, channel: wrongHash, id: 1},
		{name: "non-member", userID: outsider.ID, channel: api.InputChannel(outsider.ID, channel.ID), id: 1},
		{name: "banned member", userID: banRecipient.ID, channel: api.InputChannel(banRecipient.ID, channel.ID), id: 1},
		{name: "missing message", userID: creator.ID, channel: api.InputChannel(creator.ID, channel.ID), id: 99},
		{name: "deleted message", userID: creator.ID, channel: api.InputChannel(creator.ID, channel.ID), id: 2},
		{name: "zero message id", userID: creator.ID, channel: api.InputChannel(creator.ID, channel.ID), id: 0},
		{name: "negative message id", userID: creator.ID, channel: api.InputChannel(creator.ID, channel.ID), id: -1},
	}
	var wantWire []byte
	for i, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, rpc, wire := exportMessageLinkCall(t, h, tc.userID, tc.channel, tc.id, false, false)
			if rpc == nil {
				t.Fatal("export succeeded, want PEER_ID_INVALID")
			}
			if rpc.ErrorCode != 400 || rpc.ErrorMessage != "PEER_ID_INVALID" {
				t.Fatalf("error = %d %s, want 400 PEER_ID_INVALID", rpc.ErrorCode, rpc.ErrorMessage)
			}
			if i == 0 {
				wantWire = wire
				return
			}
			if !bytes.Equal(wire, wantWire) {
				t.Errorf("denial wire body differs from unknown-channel response: %x != %x", wire, wantWire)
			}
		})
	}

}
