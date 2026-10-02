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
	if len(otherFilters.Filters) != 2 {
		t.Fatalf("other owner saw %d filters, want All chats and Unread", len(otherFilters.Filters))
	}
	if _, ok := otherFilters.Filters[0].(*tg.DialogFilterDefault); !ok {
		t.Fatalf("other owner's first folder = %T, want All chats", otherFilters.Filters[0])
	}
	if unread, ok := otherFilters.Filters[1].(*tg.DialogFilter); !ok || unread.Title.Text != "Unread" {
		t.Fatalf("other owner's second folder = %#v, want Unread", otherFilters.Filters[1])
	}
	baselineMarker, found, err := s.DialogFilterChangeAt(ctx, other.ID)
	if err != nil || !found {
		t.Fatalf("read other owner's seeded marker: found %v, err %v", found, err)
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
	if marker, found, err := s.DialogFilterChangeAt(ctx, other.ID); err != nil || !found || !marker.Equal(baselineMarker) {
		t.Fatalf("rejected peer mutation changed the durable marker from %s to %s (found %v, err %v)", baselineMarker, marker, found, err)
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

func TestSuggestedDialogFiltersOfferOnlyMissingUnreadAndPersonal(t *testing.T) {
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
	owner, err := s.CreateUser(ctx, "+15551090099")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	client := newDialogFilterRPC(t, s, owner.ID)
	getSuggestions := func() []tg.DialogFilterSuggested {
		t.Helper()
		var got tg.DialogFilterSuggestedVector
		if err := got.Decode(&bin.Buffer{Buf: client.call(t, &tg.MessagesGetSuggestedDialogFiltersRequest{})}); err != nil {
			t.Fatalf("decode suggested folders: %v", err)
		}
		return got.Elems
	}
	assertSuggestion := func(got tg.DialogFilterSuggested, id int, title, description string) *tg.DialogFilter {
		t.Helper()
		filter, ok := got.Filter.(*tg.DialogFilter)
		if !ok || filter.ID != id || filter.Title.Text != title || got.Description != description {
			t.Fatalf("suggestion = %#v, want ID %d %s with description %q", got, id, title, description)
		}
		return filter
	}

	initial := getSuggestions()
	if len(initial) != 2 {
		t.Fatalf("initial suggestions = %d, want Unread and Personal", len(initial))
	}
	unread := assertSuggestion(initial[0], 2, "Unread", "Chats with unread messages")
	if !unread.Contacts || !unread.NonContacts || !unread.Groups || !unread.Broadcasts || !unread.Bots || !unread.ExcludeRead {
		t.Fatalf("Unread suggestion = %+v, want all chat types excluding read chats", unread)
	}
	personal := assertSuggestion(initial[1], 3, "Personal", "Private chats")
	if !personal.Contacts || !personal.NonContacts || personal.Groups || personal.Broadcasts || personal.Bots || personal.ExcludeRead {
		t.Fatalf("Personal suggestion = %+v, want private contacts and non-contacts only", personal)
	}

	var folders tg.MessagesDialogFilters
	if err := folders.Decode(&bin.Buffer{Buf: client.call(t, &tg.MessagesGetDialogFiltersRequest{})}); err != nil {
		t.Fatalf("get dialog filters: %v", err)
	}
	if len(folders.Filters) != 2 {
		t.Fatalf("first get dialog filters returned %d folders, want All chats and Unread", len(folders.Filters))
	}
	if seeded, ok := folders.Filters[1].(*tg.DialogFilter); !ok || seeded.Title.Text != "Unread" {
		t.Fatalf("first seeded folder = %#v, want Unread", folders.Filters[1])
	}
	remaining := getSuggestions()
	if len(remaining) != 1 {
		t.Fatalf("suggestions after seeding Unread = %d, want only Personal", len(remaining))
	}
	assertSuggestion(remaining[0], 3, "Personal", "Private chats")

	personal.SetFlags()
	upsert := &tg.MessagesUpdateDialogFilterRequest{ID: 3, Filter: personal}
	upsert.SetFlags()
	requireDialogFilterBool(t, client.call(t, upsert))
	if got := getSuggestions(); len(got) != 0 {
		t.Fatalf("suggestions after adding both folders = %d, want none", len(got))
	}
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

func TestDialogFilterAcknowledgementsIgnoreResponsesWrittenAfterDeadline(t *testing.T) {
	t.Parallel()
	baseCtx := context.Background()
	s, err := store.Open(baseCtx, pgtest.DSN(t), pgtest.EncKey(), store.WithoutBlobStore())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})
	owner, err := s.CreateUser(baseCtx, "+15551090024")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}

	for _, tc := range []struct {
		name       string
		request    bin.Encoder
		prepare    func(*api.DialogFilterSync, *mtproto.Conn, *mtproto.Request)
		wantResult func(*testing.T, []byte)
	}{
		{
			name:    "folder fetch",
			request: &tg.MessagesGetDialogFiltersRequest{},
			prepare: func(syncState *api.DialogFilterSync, conn *mtproto.Conn, req *mtproto.Request) {
				syncState.RequesterRepair(conn, req)
			},
			wantResult: func(t *testing.T, response []byte) {
				t.Helper()
				var folders tg.MessagesDialogFilters
				if err := folders.Decode(&bin.Buffer{Buf: response}); err != nil {
					t.Fatalf("decode folder response: %v", err)
				}
			},
		},
		{
			name:    "difference",
			request: &tg.UpdatesGetDifferenceRequest{Pts: 0, Date: int(time.Now().Unix())},
			prepare: func(syncState *api.DialogFilterSync, conn *mtproto.Conn, req *mtproto.Request) {
				syncState.EnsureBinding(conn, req)
			},
			wantResult: func(t *testing.T, response []byte) {
				t.Helper()
				if got := getDialogFilterDifferenceUpdates(t, response); got != 1 {
					t.Fatalf("difference refresh updates = %d, want one", got)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			syncState := api.NewDialogFilterSync()
			transport := &settingsDispatcherTransport{
				sendEntered: make(chan struct{}, 1),
				sendRelease: make(chan struct{}),
			}
			client := newDialogFilterRPCWithSync(t, s, owner.ID, transport, syncState)
			requestCtx, cancel := context.WithTimeout(baseCtx, 500*time.Millisecond)
			defer cancel()
			msgID := client.nextMsgID
			client.nextMsgID += 4
			var body bin.Buffer
			if err := tc.request.Encode(&body); err != nil {
				t.Fatalf("encode %s request: %v", tc.name, err)
			}
			req := &mtproto.Request{
				AuthKeyID: client.key.ID,
				UserID:    owner.ID,
				SessionID: 123,
				MsgID:     msgID,
				Buf:       &body,
				Ctx:       requestCtx,
			}
			tc.prepare(syncState, client.conn, req)

			done := make(chan error, 1)
			go func() { done <- client.handler.OnMessage(client.conn, req) }()
			select {
			case <-transport.sendEntered:
			case <-time.After(3 * time.Second):
				t.Fatal("response write did not begin")
			}
			releaseWrite := func() {
				select {
				case <-transport.sendRelease:
				default:
					close(transport.sendRelease)
				}
			}
			defer releaseWrite()
			<-requestCtx.Done()
			syncState.RequesterRepair(client.conn, req)
			releaseWrite()
			if err := <-done; err != nil {
				t.Fatalf("successful response write after deadline: %v", err)
			}
			tc.wantResult(t, transport.result(t, client.key, msgID))

			generation, covered, first, pending, ok := client.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
			if !ok || generation != 2 && tc.name == "folder fetch" || generation != 1 && tc.name == "difference" || covered != 0 || !first || !pending {
				t.Fatalf("expired response acknowledged recovery: generation %d covered %d first %v pending %v ok %v", generation, covered, first, pending, ok)
			}
		})
	}
}

func TestNestedInvokeAfterFolderMutationRefusesAndAccountsRequester(t *testing.T) {
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
	owner, err := s.CreateUser(ctx, "+15551090023")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	if err := s.SaveDialogFilter(ctx, owner.ID, store.DialogFilter{ID: 2, Title: "Before", Groups: true}); err != nil {
		t.Fatalf("save existing folder: %v", err)
	}
	if _, err := s.UpdateDialogFilterOrder(ctx, owner.ID, []int{2, 0}); err != nil {
		t.Fatalf("set existing order: %v", err)
	}
	before, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read initial folder state: %v", err)
	}

	syncState := api.NewDialogFilterSync()
	client := newDialogFilterRPCWithSync(t, s, owner.ID, &settingsDispatcherTransport{}, syncState)
	otherSession := newDialogFilterRPCWithSync(t, s, owner.ID, &settingsDispatcherTransport{}, syncState)
	syncState.EnsureBinding(otherSession.conn, &mtproto.Request{UserID: owner.ID, SessionID: 123})

	for range 58 {
		result, err := s.CheckRateLimitCost(ctx, owner.ID, "dialog_filter_mutation", store.RateLimitConfig{Limit: 60, Window: time.Minute}, 1)
		if err != nil || result != nil {
			t.Fatalf("precharge folder mutation attempt: result %v, err %v", result, err)
		}
	}

	filter := &tg.DialogFilter{ID: 2, Title: tg.TextWithEntities{Text: "After"}, Groups: false}
	filter.SetFlags()
	edit := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: filter}
	edit.SetFlags()
	nested := &tg.InvokeAfterMsgRequest{
		MsgID: 1,
		Query: &tg.InvokeAfterMsgRequest{MsgID: 2, Query: edit},
	}
	response, err := client.dispatch(t, nested)
	if err != nil {
		t.Fatalf("nested folder mutation refusal: %v", err)
	}
	requireDialogFilterRPCError(t, response, "MSG_WAIT_TIMEOUT")
	if _, err := client.dispatch(t, &tg.MessagesGetDialogFiltersRequest{}); err != nil {
		t.Fatalf("fetch after first refusal: %v", err)
	}
	orderRequest := &tg.MessagesUpdateDialogFiltersOrderRequest{Order: []int{0, 2}}
	nestedOrder := &tg.InvokeAfterMsgsRequest{
		MsgIDs: []int64{1},
		Query:  &tg.InvokeWithLayerRequest{Layer: 1, Query: orderRequest},
	}
	response, err = client.dispatch(t, nestedOrder)
	if err != nil {
		t.Fatalf("nested order refusal: %v", err)
	}
	requireDialogFilterRPCError(t, response, "MSG_WAIT_TIMEOUT")

	after, err := s.DialogFilters(ctx, owner.ID)
	if err != nil {
		t.Fatalf("read folder state after refusal: %v", err)
	}
	if len(after.Filters) != 1 || after.Filters[0].Title != "Before" || after.Filters[0].Groups != true {
		t.Fatalf("nested folder mutation changed the definition: %+v", after.Filters)
	}
	if len(after.Order) != len(before.Order) || after.Order[0] != before.Order[0] || after.Order[1] != before.Order[1] {
		t.Fatalf("nested folder mutation changed order from %v to %v", before.Order, after.Order)
	}
	if before.ChangedAt == nil || after.ChangedAt == nil || !before.ChangedAt.Equal(*after.ChangedAt) {
		t.Fatalf("nested folder mutation changed committed marker from %v to %v", before.ChangedAt, after.ChangedAt)
	}
	requesterGeneration, requesterCovered, requesterFirstDifference, requesterPending, ok := client.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
	if !ok || !requesterPending {
		t.Fatalf("authorized requester has no repair pending: generation %d covered %d first difference %v pending %v ok %v", requesterGeneration, requesterCovered, requesterFirstDifference, requesterPending, ok)
	}
	otherGeneration, otherCovered, otherFirstDifference, otherPending, ok := otherSession.conn.DialogFilterRecoverySnapshot(owner.ID, 123, 0)
	if !ok || otherPending {
		t.Fatalf("refusal repair spread to another session: generation %d covered %d first difference %v pending %v ok %v", otherGeneration, otherCovered, otherFirstDifference, otherPending, ok)
	}

	if _, err := client.dispatch(t, edit); err != nil {
		t.Fatalf("rate-limited folder mutation: %v", err)
	}
	requireDialogFilterRPCError(t, client.transport.result(t, client.key, client.nextMsgID-4), "FLOOD_WAIT_60")
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
