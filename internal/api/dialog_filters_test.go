package api_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type dialogFilterRPC struct {
	handler   mtproto.Handler
	conn      *mtproto.Conn
	transport *settingsDispatcherTransport
	key       crypto.AuthKey
	userID    int64
	nextMsgID int64
}

func newDialogFilterRPC(t *testing.T, s *store.Store, userID int64) *dialogFilterRPC {
	t.Helper()
	return newDialogFilterRPCWithTransport(t, s, userID, &settingsDispatcherTransport{})
}

func newDialogFilterRPCWithTransport(t *testing.T, s *store.Store, userID int64, transport *settingsDispatcherTransport) *dialogFilterRPC {
	t.Helper()
	return newDialogFilterRPCWithSync(t, s, userID, transport, api.NewDialogFilterSync())
}

func newDialogFilterRPCWithSync(t *testing.T, s *store.Store, userID int64, transport *settingsDispatcherTransport, syncState *api.DialogFilterSync) *dialogFilterRPC {
	t.Helper()
	key := testKey()
	conn := mtproto.NewTestConn(transport, key)
	conn.SetOwner(userID)
	return &dialogFilterRPC{
		handler:   api.NewWithDialogFilterSync(s, 2, &tg.Config{}, slog.New(slog.DiscardHandler), false, 1, nil, 1, pgtest.PeerDeriver(), config.RateLimitsConfig{}, config.RegistrationClosed, syncState),
		conn:      conn,
		transport: transport,
		key:       key,
		userID:    userID,
		nextMsgID: 1 << 32,
	}
}

func (c *dialogFilterRPC) call(t *testing.T, request bin.Encoder) []byte {
	t.Helper()
	response, err := c.dispatch(t, request)
	if err != nil {
		t.Fatalf("dispatch %T: %v", request, err)
	}
	return response
}

func (c *dialogFilterRPC) dispatch(t *testing.T, request bin.Encoder) ([]byte, error) {
	t.Helper()
	var body bin.Buffer
	if err := request.Encode(&body); err != nil {
		return nil, err
	}
	msgID := c.nextMsgID
	c.nextMsgID += 4
	if err := c.handler.OnMessage(c.conn, &mtproto.Request{
		AuthKeyID: c.key.ID,
		UserID:    c.userID,
		SessionID: 123,
		MsgID:     msgID,
		Buf:       &body,
		Ctx:       context.Background(),
	}); err != nil {
		return nil, err
	}
	return c.transport.result(t, c.key, msgID), nil
}

func requireDialogFilterBool(t *testing.T, response []byte) {
	t.Helper()
	buf := &bin.Buffer{Buf: response}
	id, err := buf.PeekID()
	if err != nil {
		t.Fatalf("peek bool response: %v", err)
	}
	if id == mt.RPCErrorTypeID {
		var rpcErr mt.RPCError
		if err := rpcErr.Decode(buf); err != nil {
			t.Fatalf("decode RPC error: %v", err)
		}
		t.Fatalf("folder mutation returned %s", rpcErr.ErrorMessage)
	}
	if id != (&tg.BoolTrue{}).TypeID() {
		t.Fatalf("folder mutation response constructor = %#x, want boolTrue", id)
	}
}

func requireDialogFilterRPCError(t *testing.T, response []byte, want string) {
	t.Helper()
	buf := &bin.Buffer{Buf: response}
	id, err := buf.PeekID()
	if err != nil {
		t.Fatalf("peek RPC error: %v", err)
	}
	if id != mt.RPCErrorTypeID {
		t.Fatalf("response constructor = %#x, want RPC error %s", id, want)
	}
	var got mt.RPCError
	if err := got.Decode(buf); err != nil {
		t.Fatalf("decode RPC error: %v", err)
	}
	if got.ErrorMessage != want {
		t.Fatalf("RPC error = %q, want %q", got.ErrorMessage, want)
	}
}

func TestDialogFiltersRPCPersistsAndOrdersPrivateFolder(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	owner, err := s.CreateUser(ctx, "+15551090001")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	member, err := s.CreateUser(ctx, "+15551090002")
	if err != nil {
		t.Fatalf("create member: %v", err)
	}
	chat, err := s.CreateChat(ctx, owner.ID, "folder group", []int64{member.ID})
	if err != nil {
		t.Fatalf("create group: %v", err)
	}
	client := newDialogFilterRPC(t, s, owner.ID)

	filter := &tg.DialogFilter{
		ID:             2,
		Title:          tg.TextWithEntities{Text: "Groups", Entities: []tg.MessageEntityClass{&tg.MessageEntityCustomEmoji{Offset: 0, Length: 1, DocumentID: 42}}},
		Groups:         true,
		ExcludeMuted:   true,
		TitleNoanimate: true,
		IncludePeers:   []tg.InputPeerClass{&tg.InputPeerChat{ChatID: chat.ID}},
	}
	filter.SetEmoticon("📁")
	filter.SetColor(3)
	filter.SetFlags()
	upsert := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: filter}
	upsert.SetFlags()
	requireDialogFilterBool(t, client.call(t, upsert))

	var got tg.MessagesDialogFilters
	if err := got.Decode(&bin.Buffer{Buf: client.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("decode folders: %v", err)
	}
	if got.TagsEnabled {
		t.Fatal("folder tags are enabled")
	}
	if len(got.Filters) != 2 {
		t.Fatalf("folders = %d, want All chats and ID 2", len(got.Filters))
	}
	if _, ok := got.Filters[0].(*tg.DialogFilterDefault); !ok {
		t.Fatalf("first folder = %T, want All chats", got.Filters[0])
	}
	custom, ok := got.Filters[1].(*tg.DialogFilter)
	if !ok {
		t.Fatalf("second folder = %T, want *tg.DialogFilter", got.Filters[1])
	}
	if custom.ID != 2 || custom.Title.Text != "Groups" || !custom.Groups || !custom.ExcludeMuted || !custom.TitleNoanimate {
		t.Fatalf("folder = id %d, title %q, groups %v", custom.ID, custom.Title.Text, custom.Groups)
	}
	if emoticon, ok := custom.GetEmoticon(); !ok || emoticon != "📁" {
		t.Fatalf("emoticon = %q, present %v, want folder glyph", emoticon, ok)
	}
	if color, ok := custom.GetColor(); !ok || color != 3 {
		t.Fatalf("color = %d, present %v, want 3", color, ok)
	}
	if len(custom.Title.Entities) != 1 {
		t.Fatalf("title entities = %d, want custom emoji", len(custom.Title.Entities))
	}
	if entity, ok := custom.Title.Entities[0].(*tg.MessageEntityCustomEmoji); !ok || entity.DocumentID != 42 || entity.Offset != 0 || entity.Length != 1 {
		t.Fatalf("title entity = %#v, want custom emoji document 42", custom.Title.Entities[0])
	}
	if len(custom.IncludePeers) != 1 {
		t.Fatalf("included peers = %d, want one", len(custom.IncludePeers))
	}
	if peer, ok := custom.IncludePeers[0].(*tg.InputPeerChat); !ok || peer.ChatID != chat.ID {
		t.Fatalf("included peer = %T %v, want authorized group %d", custom.IncludePeers[0], custom.IncludePeers[0], chat.ID)
	}
	otherSession := newDialogFilterRPC(t, s, owner.ID)
	var otherSessionFilters tg.MessagesDialogFilters
	if err := otherSessionFilters.Decode(&bin.Buffer{Buf: otherSession.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("other session get dialog filters: %v", err)
	}
	if len(otherSessionFilters.Filters) != 2 {
		t.Fatalf("other session saw %d folders, want the owner's persisted folder", len(otherSessionFilters.Filters))
	}
	if otherCustom, ok := otherSessionFilters.Filters[1].(*tg.DialogFilter); !ok || otherCustom.Title.Text != "Groups" {
		t.Fatalf("other session folder = %#v, want persisted Groups", otherSessionFilters.Filters[1])
	}

	reorder := &tg.MessagesUpdateDialogFiltersOrderRequest{Order: []int{2, 0}}
	requireDialogFilterBool(t, client.call(t, reorder))
	var reordered tg.MessagesDialogFilters
	if err := reordered.Decode(&bin.Buffer{Buf: client.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("decode reordered folders: %v", err)
	}
	if len(reordered.Filters) != 2 {
		t.Fatalf("reordered folders = %d, want 2", len(reordered.Filters))
	}
	if folder, ok := reordered.Filters[0].(*tg.DialogFilter); !ok || folder.ID != 2 {
		t.Fatalf("first reordered folder = %T, want ID 2", reordered.Filters[0])
	}
	if _, ok := reordered.Filters[1].(*tg.DialogFilterDefault); !ok {
		t.Fatalf("second reordered folder = %T, want All chats", reordered.Filters[1])
	}

	deleteRequest := &tg.MessagesUpdateDialogFilterRequest{ID: 2}
	deleteRequest.SetFlags()
	requireDialogFilterBool(t, client.call(t, deleteRequest))
	var afterDelete tg.MessagesDialogFilters
	if err := afterDelete.Decode(&bin.Buffer{Buf: client.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("decode folders after delete: %v", err)
	}
	if len(afterDelete.Filters) != 1 {
		t.Fatalf("folders after delete = %d, want only All chats", len(afterDelete.Filters))
	}
}

func TestDialogFiltersAreOwnerScopedAndReadsPruneInaccessiblePeers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	owner, err := s.CreateUser(ctx, "+15551090011")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	other, err := s.CreateUser(ctx, "+15551090012")
	if err != nil {
		t.Fatalf("create other owner: %v", err)
	}
	creator, err := s.CreateUser(ctx, "+15551090013")
	if err != nil {
		t.Fatalf("create group creator: %v", err)
	}
	group, err := s.CreateChat(ctx, creator.ID, "shared group", []int64{owner.ID, other.ID})
	if err != nil {
		t.Fatalf("create shared group: %v", err)
	}
	privateChat, err := s.CreateChat(ctx, creator.ID, "private group", nil)
	if err != nil {
		t.Fatalf("create private group: %v", err)
	}
	ownerClient := newDialogFilterRPC(t, s, owner.ID)
	otherClient := newDialogFilterRPC(t, s, other.ID)

	ownerFilter := &tg.DialogFilter{
		ID:           2,
		Title:        tg.TextWithEntities{Text: "Owner folder"},
		Groups:       true,
		IncludePeers: []tg.InputPeerClass{&tg.InputPeerChat{ChatID: group.ID}},
	}
	ownerFilter.SetFlags()
	ownerMutation := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: ownerFilter}
	ownerMutation.SetFlags()
	requireDialogFilterBool(t, ownerClient.call(t, ownerMutation))
	var ownerFilters tg.MessagesDialogFilters
	if err := ownerFilters.Decode(&bin.Buffer{Buf: ownerClient.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("owner get dialog filters: %v", err)
	}

	var otherFilters tg.MessagesDialogFilters
	if err := otherFilters.Decode(&bin.Buffer{Buf: otherClient.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("other owner get dialog filters: %v", err)
	}
	if len(otherFilters.Filters) != 1 {
		t.Fatalf("other owner saw %d filters, want only All chats", len(otherFilters.Filters))
	}

	foreignFilter := &tg.DialogFilter{
		ID:           3,
		Title:        tg.TextWithEntities{Text: "Foreign"},
		Groups:       true,
		IncludePeers: []tg.InputPeerClass{&tg.InputPeerChat{ChatID: privateChat.ID}},
	}
	foreignFilter.SetFlags()
	foreignMutation := &tg.MessagesUpdateDialogFilterRequest{ID: 3, Filter: foreignFilter}
	foreignMutation.SetFlags()
	requireDialogFilterRPCError(t, otherClient.call(t, foreignMutation), "PEER_ID_INVALID")
	generation, covered, first, otherPending, ok := otherClient.conn.DialogFilterRecoverySnapshot(other.ID, 123, 0)
	if !ok || !otherPending {
		t.Fatalf("rejected mutation did not repair its requester: generation %d covered %d first %v pending %v ok %v", generation, covered, first, otherPending, ok)
	}
	generation, covered, first, ownerPending, ok := ownerClient.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
	if !ok || ownerPending {
		t.Fatalf("rejected mutation affected another owner's connection: generation %d covered %d first %v pending %v ok %v", generation, covered, first, ownerPending, ok)
	}
	if _, found, err := s.DialogFilterChangeAt(ctx, other.ID); err != nil || found {
		t.Fatalf("rejected peer mutation changed the durable marker: found %v, err %v", found, err)
	}

	removed, _, _, err := s.RemoveChatUser(ctx, group.ID, owner.ID, creator.ID)
	if err != nil || !removed {
		t.Fatalf("remove owner from shared group: removed %v, err %v", removed, err)
	}
	var pruned tg.MessagesDialogFilters
	if err := pruned.Decode(&bin.Buffer{Buf: ownerClient.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("owner get filters after removal: %v", err)
	}
	custom, ok := pruned.Filters[1].(*tg.DialogFilter)
	if !ok || len(custom.IncludePeers) != 0 {
		t.Fatalf("inaccessible group was not pruned from response: %#v", pruned.Filters)
	}
	persisted, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read persisted owner folders: %v", err)
	}
	if len(persisted.Filters) != 1 || len(persisted.Filters[0].IncludePeers) != 1 {
		t.Fatalf("pruning changed durable peer references: %#v", persisted.Filters)
	}

	badHashFilter := &tg.DialogFilter{
		ID:           4,
		Title:        tg.TextWithEntities{Text: "Bad hash"},
		Groups:       true,
		IncludePeers: []tg.InputPeerClass{&tg.InputPeerUser{UserID: creator.ID, AccessHash: 1}},
	}
	badHashFilter.SetFlags()
	badHashMutation := &tg.MessagesUpdateDialogFilterRequest{ID: 4, Filter: badHashFilter}
	badHashMutation.SetFlags()
	requireDialogFilterRPCError(t, ownerClient.call(t, badHashMutation), "PEER_ID_INVALID")
}

func TestDialogFilterChannelPeersRequireOwnerHashAndActiveMembership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	creator, err := s.CreateUser(ctx, "+15551090041")
	if err != nil {
		t.Fatalf("create channel creator: %v", err)
	}
	owner, err := s.CreateUser(ctx, "+15551090042")
	if err != nil {
		t.Fatalf("create folder owner: %v", err)
	}
	outsider, err := s.CreateUser(ctx, "+15551090043")
	if err != nil {
		t.Fatalf("create outsider: %v", err)
	}
	channel, err := s.CreateChannel(ctx, creator.ID, "folder channel", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create channel invite: %v", err)
	}
	if _, _, err := s.JoinChannelByInvite(ctx, invite, owner.ID); err != nil {
		t.Fatalf("join folder owner to channel: %v", err)
	}
	deriver := pgtest.PeerDeriver()
	ownerHash := deriver.Derive(owner.ID, peerhash.KindChannel, channel.ID)
	ownerClient := newDialogFilterRPC(t, s, owner.ID)
	filter := &tg.DialogFilter{
		ID:           2,
		Title:        tg.TextWithEntities{Text: "Broadcasts"},
		Broadcasts:   true,
		IncludePeers: []tg.InputPeerClass{&tg.InputPeerChannel{ChannelID: channel.ID, AccessHash: ownerHash}},
	}
	filter.SetFlags()
	upsert := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: filter}
	upsert.SetFlags()
	requireDialogFilterBool(t, ownerClient.call(t, upsert))

	wrongHash := *filter
	wrongHash.ID = 3
	wrongHash.IncludePeers = []tg.InputPeerClass{&tg.InputPeerChannel{ChannelID: channel.ID, AccessHash: 0}}
	wrongHash.SetFlags()
	wrongHashMutation := &tg.MessagesUpdateDialogFilterRequest{ID: 3, Filter: &wrongHash}
	wrongHashMutation.SetFlags()
	requireDialogFilterRPCError(t, ownerClient.call(t, wrongHashMutation), "PEER_ID_INVALID")

	outsiderClient := newDialogFilterRPC(t, s, outsider.ID)
	outsiderHash := deriver.Derive(outsider.ID, peerhash.KindChannel, channel.ID)
	foreignChannel := &tg.DialogFilter{
		ID:           2,
		Title:        tg.TextWithEntities{Text: "Foreign"},
		Broadcasts:   true,
		IncludePeers: []tg.InputPeerClass{&tg.InputPeerChannel{ChannelID: channel.ID, AccessHash: outsiderHash}},
	}
	foreignChannel.SetFlags()
	foreignMutation := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: foreignChannel}
	foreignMutation.SetFlags()
	requireDialogFilterRPCError(t, outsiderClient.call(t, foreignMutation), "PEER_ID_INVALID")

	banUntil := time.Now().Add(time.Hour)
	if err := s.SetChannelBan(ctx, channel.ID, creator.ID, owner.ID, &banUntil, false); err != nil {
		t.Fatalf("ban folder owner: %v", err)
	}
	var folders tg.MessagesDialogFilters
	if err := folders.Decode(&bin.Buffer{Buf: ownerClient.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("read folders after channel ban: %v", err)
	}
	custom, ok := folders.Filters[1].(*tg.DialogFilter)
	if !ok || len(custom.IncludePeers) != 0 {
		t.Fatalf("banned channel reference was not pruned: %#v", folders.Filters)
	}
}

func TestGetDifferenceSignalsDialogFilterRefreshOnceAndForRecentMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	owner, err := s.CreateUser(ctx, "+15551090021")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	client := newDialogFilterRPC(t, s, owner.ID)
	first := &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: 1}
	if updates := getDialogFilterDifferenceUpdates(t, client.call(t, first)); updates != 1 {
		t.Fatalf("first difference folder refresh updates = %d, want one", updates)
	}
	_, _, stillFirst, pending, ok := client.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
	if !ok || stillFirst || pending {
		t.Fatalf("successful first difference left recovery first=%v pending=%v ok=%v", stillFirst, pending, ok)
	}
	if err := s.SaveDialogFilter(ctx, owner.ID, store.DialogFilter{ID: 2, Title: "Recent", Groups: true}); err != nil {
		t.Fatalf("save marker folder: %v", err)
	}
	nearNow := &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: int(time.Now().Unix())}
	if updates := getDialogFilterDifferenceUpdates(t, client.call(t, nearNow)); updates != 1 {
		t.Fatalf("recent marker difference folder refresh updates = %d, want one", updates)
	}
}

func TestDialogFilterFetchCoverageWaitsForSuccessfulSameConnectionWrite(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	owner, err := s.CreateUser(ctx, "+15551090022")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	syncState := api.NewDialogFilterSync()
	transport := &settingsDispatcherTransport{sendErr: errors.New("blocked result write")}
	client := newDialogFilterRPCWithSync(t, s, owner.ID, transport, syncState)
	req := &mtproto.Request{UserID: owner.ID, SessionID: 123}
	syncState.RequesterRepair(client.conn, req)
	otherSession := newDialogFilterRPCWithSync(t, s, owner.ID, &settingsDispatcherTransport{}, syncState)
	otherReq := &mtproto.Request{UserID: owner.ID, SessionID: 123}
	syncState.EnsureBinding(otherSession.conn, otherReq)
	if _, err := client.dispatch(t, &tg.MessagesGetDialogFiltersRequest{}); err == nil {
		t.Fatal("failed folder-fetch write unexpectedly succeeded")
	}
	_, covered, first, pending, ok := client.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
	if !ok || covered != 0 || !first || !pending {
		t.Fatalf("failed fetch acknowledged recovery: covered %d, first %v, pending %v, ok %v", covered, first, pending, ok)
	}
	_, _, otherFirst, otherPending, ok := otherSession.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
	if !ok || !otherFirst || otherPending {
		t.Fatalf("requester repair spread to another same-owner session: first %v, pending %v, ok %v", otherFirst, otherPending, ok)
	}
	transport.sendErr = nil
	response, err := client.dispatch(t, &tg.MessagesGetDialogFiltersRequest{})
	if err != nil {
		t.Fatalf("successful folder fetch: %v", err)
	}
	var folders tg.MessagesDialogFilters
	if err := folders.Decode(&bin.Buffer{Buf: response}); err != nil {
		t.Fatalf("decode successful folder fetch: %v", err)
	}
	_, covered, first, pending, ok = client.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
	if !ok || covered != 1 || !first || pending {
		t.Fatalf("successful fetch coverage = %d, first %v, pending %v, ok %v", covered, first, pending, ok)
	}
}

func getDialogFilterDifferenceUpdates(t *testing.T, response []byte) int {
	t.Helper()
	var result tg.UpdatesDifferenceBox
	if err := result.Decode(&bin.Buffer{Buf: response}); err != nil {
		t.Fatalf("decode updates.getDifference result: %v", err)
	}
	updates := 0
	switch diff := result.Difference.(type) {
	case *tg.UpdatesDifference:
		for _, update := range diff.OtherUpdates {
			if _, ok := update.(*tg.UpdateDialogFilters); ok {
				updates++
			}
		}
	case *tg.UpdatesDifferenceSlice:
		for _, update := range diff.OtherUpdates {
			if _, ok := update.(*tg.UpdateDialogFilters); ok {
				updates++
			}
		}
	}
	return updates
}
