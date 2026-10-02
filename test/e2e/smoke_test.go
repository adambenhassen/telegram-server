package e2e_test

import (
	"context"
	"crypto/rsa"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/telegram/updates/hook"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSmoke(t *testing.T) {
	t.Run("one-to-one", func(t *testing.T) {
		t.Parallel()
		testSmokeOneToOne(t)
	})
	t.Run("saved-messages", func(t *testing.T) {
		t.Parallel()
		testSmokeSavedMessages(t)
	})
	t.Run("dialog-filters", func(t *testing.T) {
		t.Parallel()
		testSmokeDialogFilters(t)
	})
	t.Run("basic-group", func(t *testing.T) {
		t.Parallel()
		testSmokeBasicGroup(t)
	})
	t.Run("channel", func(t *testing.T) {
		t.Parallel()
		testSmokeChannel(t)
	})
	t.Run("megagroup-slow-mode", func(t *testing.T) {
		t.Parallel()
		testSmokeMegagroupSlowMode(t)
	})
	t.Run("contacts-search", func(t *testing.T) {
		t.Parallel()
		testSmokeContactsSearch(t)
	})
	t.Run("username-registration", func(t *testing.T) {
		t.Parallel()
		testSmokeUsernameRegistration(t)
	})
	t.Run("username-password-reset", func(t *testing.T) {
		t.Parallel()
		testSmokeUsernamePasswordReset(t)
	})
}

func testSmokeOneToOne(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phoneA, phoneB = "+15551046001", "+15551046002"
	seedPhoneUsers(t, f.ctx, f.store, phoneA, phoneB)

	a1 := newSmokeClient(t, f, "A1", phoneA)
	a2 := newSmokeClient(t, f, "A2", phoneA)
	b1 := newSmokeClient(t, f, "B1", phoneB)
	b2 := newSmokeClient(t, f, "B2", phoneB)
	waitForDistinctAuthKeys(t, f.ctx, f.registry, a1.id, 2, "A1", a1.lifecycle)
	waitForDistinctAuthKeys(t, f.ctx, f.registry, b1.id, 2, "B1", b1.lifecycle)

	// Seed A's local ID space through a real Saved Messages send, so the two
	// accounts' IDs differ when they receive the same 1:1 message.
	var seedResult tg.UpdatesClass
	if err := a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		seedResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerSelf{}, Message: "smoke-id-seed", RandomID: 1046001,
		})
		return err
	}); err != nil {
		t.Fatalf("send Saved Messages ID seed: %v", err)
	}
	seed := assertSmokeSendResult(t, seedResult, "smoke-id-seed", 1, 1)
	assertObservedMessage(t, f.ctx, a1.seen, seed.Message, seed.ID, true, a1.id, 1, "A1 ID seed", true)
	assertObservedMessage(t, f.ctx, a2.seen, seed.Message, seed.ID, true, a1.id, 1, "A2 ID seed", true)
	assertObservedMessage(t, f.ctx, a2.push, seed.Message, seed.ID, true, a1.id, 1, "A2 ID seed push")

	var aToBResult tg.UpdatesClass
	if err := a1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		aToBResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(a1.id, b1.id), Message: "a-to-b-smoke", RandomID: 1046002,
		})
		return err
	}); err != nil {
		t.Fatalf("A send: %v", err)
	}
	aToB := assertSmokeSendResult(t, aToBResult, "a-to-b-smoke", 2, 2)
	assertObservedMessage(t, f.ctx, a1.seen, aToB.Message, aToB.ID, true, b1.id, 2, "A1 sender echo", true)
	assertObservedMessage(t, f.ctx, a2.seen, aToB.Message, aToB.ID, true, b1.id, 2, "A2 sender echo", true)
	assertObservedMessage(t, f.ctx, b1.seen, "a-to-b-smoke", 1, false, a1.id, 1, "B1 incoming", true)
	assertObservedMessage(t, f.ctx, b2.seen, "a-to-b-smoke", 1, false, a1.id, 1, "B2 incoming", true)
	assertObservedMessage(t, f.ctx, a2.push, aToB.Message, aToB.ID, true, b1.id, 2, "A2 sender echo push")
	assertObservedMessage(t, f.ctx, b1.push, "a-to-b-smoke", 1, false, a1.id, 1, "B1 incoming push")
	assertObservedMessage(t, f.ctx, b2.push, "a-to-b-smoke", 1, false, a1.id, 1, "B2 incoming push")

	var bToAResult tg.UpdatesClass
	if err := b1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		bToAResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerUser(b1.id, a1.id), Message: "b-to-a-smoke", RandomID: 1046003,
		})
		return err
	}); err != nil {
		t.Fatalf("B send: %v", err)
	}
	bToA := assertSmokeSendResult(t, bToAResult, "b-to-a-smoke", 2, 2)
	assertObservedMessage(t, f.ctx, b1.seen, bToA.Message, bToA.ID, true, a1.id, 2, "B1 sender echo", true)
	assertObservedMessage(t, f.ctx, b2.seen, bToA.Message, bToA.ID, true, a1.id, 2, "B2 sender echo", true)
	assertObservedMessage(t, f.ctx, a1.seen, "b-to-a-smoke", 3, false, b1.id, 3, "A1 incoming", true)
	assertObservedMessage(t, f.ctx, a2.seen, "b-to-a-smoke", 3, false, b1.id, 3, "A2 incoming", true)
	assertObservedMessage(t, f.ctx, b2.push, bToA.Message, bToA.ID, true, a1.id, 2, "B2 sender echo push")
	assertObservedMessage(t, f.ctx, a1.push, "b-to-a-smoke", 3, false, b1.id, 3, "A1 incoming push")
	assertObservedMessage(t, f.ctx, a2.push, "b-to-a-smoke", 3, false, b1.id, 3, "A2 incoming push")

	if err := b1.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		_, err := api.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
			Peer: peerUser(b1.id, a1.id), MaxID: 1,
		})
		return err
	}); err != nil {
		t.Fatalf("B read A's received message: %v", err)
	}
	assertObservedReadMarker(t, f.ctx, a1.seen, aToB.ID, 4, "A1 read receipt")
	assertObservedReadMarker(t, f.ctx, a2.seen, aToB.ID, 4, "A2 read receipt")
	assertObservedReadMarker(t, f.ctx, a1.push, aToB.ID, 4, "A1 read receipt push")
	assertObservedReadMarker(t, f.ctx, a2.push, aToB.ID, 4, "A2 read receipt push")

	wantA := map[string]smokeHistoryMessage{
		"a-to-b-smoke": {id: 2, out: true},
		"b-to-a-smoke": {id: 3, out: false},
	}
	wantB := map[string]smokeHistoryMessage{
		"a-to-b-smoke": {id: 1, out: false},
		"b-to-a-smoke": {id: 2, out: true},
	}
	for _, check := range []struct {
		client *smokeClient
		peer   int64
		want   map[string]smokeHistoryMessage
	}{
		{client: a1, peer: b1.id, want: wantA},
		{client: a2, peer: b1.id, want: wantA},
		{client: b1, peer: a1.id, want: wantB},
		{client: b2, peer: a1.id, want: wantB},
	} {
		if err := check.client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			return verifySmokeHistory(ctx, api, peerUser(check.client.id, check.peer), check.peer, check.want)
		}); err != nil {
			t.Fatalf("getHistory for %d with peer %d: %v", check.client.id, check.peer, err)
		}
	}

	// Every managed stream and live push stream must contain each message once.
	for _, collector := range []*updateCollector{a1.seen, a2.seen, b1.seen, b2.seen, a1.push, a2.push, b1.push, b2.push} {
		assertNoMessageFor(t, f.ctx, collector.newMsg, "smoke session")
	}

	a1.stopClient(t)
	a2.stopClient(t)
	b1.stopClient(t)
	b2.stopClient(t)
	f.restart(t)

	assertSmokeReconnect(t, f, a1.session, a1.id, a1.id, b1.id, wantA)
	assertSmokeReconnect(t, f, b1.session, b1.id, b1.id, a1.id, wantB)
}

func testSmokeSavedMessages(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phone = "+15551046003"
	seedPhoneUsers(t, f.ctx, f.store, phone)
	client := newSmokeClient(t, f, "A1", phone)

	var result tg.UpdatesClass
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		result, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerSelf{}, Message: "saved-smoke", RandomID: 1046004,
		})
		return err
	}); err != nil {
		t.Fatalf("send Saved Messages: %v", err)
	}
	saved := assertSmokeSendResult(t, result, "saved-smoke", 1, 1)
	assertObservedMessage(t, f.ctx, client.seen, saved.Message, saved.ID, true, client.id, 1, "Saved Messages sender echo", true)
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		return verifySmokeHistory(ctx, api, &tg.InputPeerSelf{}, client.id, map[string]smokeHistoryMessage{
			"saved-smoke": {id: saved.ID, out: true},
		})
	}); err != nil {
		t.Fatalf("getHistory for Saved Messages: %v", err)
	}
	assertNoMessageFor(t, f.ctx, client.seen.newMsg, "Saved Messages session")
}

func testSmokeDialogFilters(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phone, otherPhone = "+15551049001", "+15551049002"
	seedPhoneUsers(t, f.ctx, f.store, phone, otherPhone)
	client := newSmokeClient(t, f, "A1", phone)
	otherSession := newSmokeClient(t, f, "A2", phone)
	otherOwner := newSmokeClient(t, f, "B1", otherPhone)
	var appConfig *tg.HelpAppConfig
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		result, err := api.HelpGetAppConfig(ctx, 0)
		if err != nil {
			return err
		}
		var ok bool
		appConfig, ok = result.(*tg.HelpAppConfig)
		if !ok {
			return errors.New("help.getAppConfig returned no config")
		}
		return nil
	}); err != nil {
		t.Fatalf("help.getAppConfig: %v", err)
	}
	config, ok := appConfig.Config.(*tg.JSONObject)
	if !ok {
		t.Fatalf("app config = %T, want *tg.JSONObject", appConfig.Config)
	}
	filtersEnabled := false
	filtersEnabledFound := false
	for _, value := range config.Value {
		if value.Key != "dialog_filters_enabled" {
			continue
		}
		if filtersEnabledFound {
			t.Fatal("app config repeats dialog_filters_enabled")
		}
		enabled, ok := value.Value.(*tg.JSONBool)
		if !ok {
			t.Fatalf("dialog_filters_enabled = %T, want *tg.JSONBool", value.Value)
		}
		filtersEnabled = enabled.Value
		filtersEnabledFound = true
	}
	if !filtersEnabledFound || !filtersEnabled {
		t.Fatal("dialog_filters_enabled is missing or false")
	}

	filter := &tg.DialogFilter{ID: 2, Title: tg.TextWithEntities{Text: "Groups"}, Groups: true}
	filter.SetFlags()
	upsert := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: filter}
	upsert.SetFlags()
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		ok, err := api.MessagesUpdateDialogFilter(ctx, upsert)
		if err == nil && !ok {
			return errors.New("folder create returned false")
		}
		return err
	}); err != nil {
		t.Fatalf("create dialog filter: %v", err)
	}
	var listed *tg.MessagesDialogFilters
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		listed, err = api.MessagesGetDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("list dialog filters: %v", err)
	}
	if listed.TagsEnabled || len(listed.Filters) != 2 {
		t.Fatalf("filters = %d, tags enabled = %v, want default plus ID 2 with tags disabled", len(listed.Filters), listed.TagsEnabled)
	}
	custom, ok := listed.Filters[1].(*tg.DialogFilter)
	if !ok || custom.ID != 2 || custom.Title.Text != "Groups" || !custom.Groups {
		t.Fatalf("listed custom filter = %#v, want ID 2 Groups", listed.Filters[1])
	}

	edit := &tg.DialogFilter{ID: 2, Title: tg.TextWithEntities{Text: "Bots"}, Bots: true}
	edit.SetFlags()
	editRequest := &tg.MessagesUpdateDialogFilterRequest{ID: 2, Filter: edit}
	editRequest.SetFlags()
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		ok, err := api.MessagesUpdateDialogFilter(ctx, editRequest)
		if err == nil && !ok {
			return errors.New("folder edit returned false")
		}
		return err
	}); err != nil {
		t.Fatalf("edit dialog filter: %v", err)
	}
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		listed, err = api.MessagesGetDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("list edited dialog filters: %v", err)
	}
	custom, ok = listed.Filters[1].(*tg.DialogFilter)
	if !ok || custom.ID != 2 || custom.Title.Text != "Bots" || !custom.Bots || custom.Groups {
		t.Fatalf("edited custom filter = %#v, want ID 2 Bots", listed.Filters[1])
	}
	var isolated *tg.MessagesDialogFilters
	if err := otherOwner.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		isolated, err = api.MessagesGetDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("other owner get dialog filters: %v", err)
	}
	if isolated.TagsEnabled || len(isolated.Filters) != 1 {
		t.Fatalf("other owner saw %d folders with tags enabled=%v, want only All chats", len(isolated.Filters), isolated.TagsEnabled)
	}
	if _, ok := isolated.Filters[0].(*tg.DialogFilterDefault); !ok {
		t.Fatalf("other owner's first folder = %T, want All chats", isolated.Filters[0])
	}

	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		ok, err := api.MessagesUpdateDialogFiltersOrder(ctx, []int{2, 0})
		if err == nil && !ok {
			return errors.New("folder reorder returned false")
		}
		return err
	}); err != nil {
		t.Fatalf("reorder dialog filters: %v", err)
	}
	if err := client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		listed, err = api.MessagesGetDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("list reordered dialog filters: %v", err)
	}
	if folder, ok := listed.Filters[0].(*tg.DialogFilter); !ok || folder.ID != 2 {
		t.Fatalf("first reordered filter = %#v, want ID 2", listed.Filters[0])
	}
	f.restart(t)
	if err := otherSession.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		listed, err = api.MessagesGetDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("other authorized session get edited dialog filters after restart: %v", err)
	}
	if len(listed.Filters) != 2 {
		t.Fatalf("other session saw %d filters after restart, want two", len(listed.Filters))
	}
	persisted, ok := listed.Filters[0].(*tg.DialogFilter)
	if !ok || persisted.ID != 2 || persisted.Title.Text != "Bots" || !persisted.Bots || persisted.Groups {
		t.Fatalf("persisted edited filter = %#v, want ID 2 Bots", listed.Filters[0])
	}
	if err := otherOwner.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		isolated, err = api.MessagesGetDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("other owner get dialog filters after restart: %v", err)
	}
	if len(isolated.Filters) != 1 {
		t.Fatalf("other owner saw %d folders after restart, want only All chats", len(isolated.Filters))
	}
	deleteRequest := &tg.MessagesUpdateDialogFilterRequest{ID: 2}
	deleteRequest.SetFlags()
	if err := otherSession.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		ok, err := api.MessagesUpdateDialogFilter(ctx, deleteRequest)
		if err == nil && !ok {
			return errors.New("folder delete returned false")
		}
		return err
	}); err != nil {
		t.Fatalf("delete dialog filter: %v", err)
	}
	if err := otherSession.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		listed, err = api.MessagesGetDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("list dialog filters after delete: %v", err)
	}
	if len(listed.Filters) != 1 {
		t.Fatalf("filters after delete = %d, want only All chats", len(listed.Filters))
	}
	var suggested []tg.DialogFilterSuggested
	if err := otherSession.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		suggested, err = api.MessagesGetSuggestedDialogFilters(ctx)
		return err
	}); err != nil {
		t.Fatalf("get suggested dialog filters: %v", err)
	}
	if len(suggested) != 0 {
		t.Fatalf("suggested filters = %d, want empty", len(suggested))
	}
}

func testSmokeBasicGroup(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phoneA, phoneB, phoneC = "+15551047001", "+15551047002", "+15551047003"
	seedPhoneUsers(t, f.ctx, f.store, phoneA, phoneB, phoneC)
	a, b, c := newSmokeClient(t, f, "A1", phoneA), newSmokeClient(t, f, "B1", phoneB), newSmokeClient(t, f, "C", phoneC)

	// Offset C's message IDs so the sender's read receipt must use C's local ID,
	// not the reader's ID for the same group message.
	var seedResult tg.UpdatesClass
	if err := c.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		seedResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerSelf{}, Message: "group-id-seed", RandomID: 1047000,
		})
		return err
	}); err != nil {
		t.Fatalf("send group ID seed: %v", err)
	}
	seed := assertSmokeSendResult(t, seedResult, "group-id-seed", 1, 1)
	assertObservedMessage(t, f.ctx, c.seen, seed.Message, seed.ID, true, c.id, 1, "C group ID seed", true)

	var chatID int64
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		created, err := api.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Smoke group",
			Users: []tg.InputUserClass{inputUser(a.id, b.id), inputUser(a.id, c.id)},
		})
		if err != nil {
			return err
		}
		updates, ok := created.Updates.(*tg.Updates)
		if !ok {
			return fmt.Errorf("createChat updates = %T, want *tg.Updates", created.Updates)
		}
		if len(updates.Chats) != 1 {
			return fmt.Errorf("createChat chats = %d, want one chat", len(updates.Chats))
		}
		chat, ok := updates.Chats[0].(*tg.Chat)
		if !ok {
			return fmt.Errorf("createChat chat = %T, want *tg.Chat", updates.Chats[0])
		}
		if chat.ParticipantsCount != 3 {
			return fmt.Errorf("createChat participants = %d, want 3", chat.ParticipantsCount)
		}
		chatID = chat.ID
		return nil
	}); err != nil {
		t.Fatalf("create smoke group: %v", err)
	}

	wantSenders := map[string]int64{
		"group-a": a.id,
		"group-b": b.id,
		"group-c": c.id,
	}
	memberLocalIDs := map[int64]map[string]int{
		a.id: {},
		b.id: {},
		c.id: {},
	}
	senderLocalIDs := make(map[string]int, len(wantSenders))
	for _, send := range []struct {
		client   *smokeClient
		text     string
		randomID int64
	}{{a, "group-a", 1047001}, {b, "group-b", 1047002}, {c, "group-c", 1047003}} {
		var result tg.UpdatesClass
		if err := send.client.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			var err error
			result, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: &tg.InputPeerChat{ChatID: chatID}, Message: send.text, RandomID: send.randomID,
			})
			return err
		}); err != nil {
			t.Fatalf("%s send to group: %v", send.text, err)
		}
		message, pts, ok := outgoingMessage(t, result, send.text)
		if !ok || countOutgoingMessages(result, send.text) != 1 {
			t.Fatalf("%s send result omitted exactly one outgoing message", send.text)
		}
		if message.ID <= 0 || pts <= 0 || !message.Out {
			t.Fatalf("%s outgoing id/pts/out = %d/%d/%t, want positive id and pts and outgoing", send.text, message.ID, pts, message.Out)
		}
		senderLocalIDs[send.text] = message.ID
		peer, ok := message.PeerID.(*tg.PeerChat)
		if !ok || peer.ChatID != chatID {
			t.Fatalf("%s outgoing peer = %+v, want chat %d", send.text, message.PeerID, chatID)
		}
		from, ok := message.FromID.(*tg.PeerUser)
		if !ok || from.UserID != send.client.id {
			t.Fatalf("%s outgoing sender = %+v, want user %d", send.text, message.FromID, send.client.id)
		}
		for _, member := range []*smokeClient{a, b, c} {
			got := recvOrCtx(t, f.ctx, member.seen.newMsg, send.text+" group update")
			if got.Message != send.text || got.ID <= 0 || got.Out != (member.id == send.client.id) {
				t.Fatalf("%s group update for %d = {text:%q id:%d out:%v}, want positive local id and out:%t", send.text, member.id, got.Message, got.ID, got.Out, member.id == send.client.id)
			}
			updatePeer, ok := got.PeerID.(*tg.PeerChat)
			if !ok || updatePeer.ChatID != chatID {
				t.Fatalf("%s group update for %d peer = %+v, want chat %d", send.text, member.id, got.PeerID, chatID)
			}
			updateFrom, ok := got.FromID.(*tg.PeerUser)
			if !ok || updateFrom.UserID != send.client.id {
				t.Fatalf("%s group update for %d sender = %+v, want user %d", send.text, member.id, got.FromID, send.client.id)
			}
			if member.id == send.client.id && got.ID != message.ID {
				t.Fatalf("%s sender update id = %d, want sender-local id %d", send.text, got.ID, message.ID)
			}
			memberLocalIDs[member.id][send.text] = got.ID
		}
	}

	for _, member := range []*smokeClient{a, b, c} {
		if err := member.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			if err := verifySmokeGroupHistory(ctx, api, chatID, member.id, wantSenders); err != nil {
				return err
			}
			full, err := api.MessagesGetFullChat(ctx, chatID)
			if err != nil {
				return fmt.Errorf("getFullChat: %w", err)
			}
			return checkFullChat(t, full, chatID, a.id, member.id, b.id, c.id)
		}); err != nil {
			t.Fatalf("smoke group for member %d: %v", member.id, err)
		}
	}

	readerMessageID := memberLocalIDs[a.id]["group-c"]
	senderMessageID := senderLocalIDs["group-c"]
	if readerMessageID <= memberLocalIDs[a.id]["group-b"] {
		t.Fatalf("A's last received group id = %d, want greater than prior id %d", readerMessageID, memberLocalIDs[a.id]["group-b"])
	}
	if readerMessageID == senderMessageID {
		t.Fatalf("group-c reader and sender local IDs both equal %d; fixture must distinguish owner ID spaces", readerMessageID)
	}

	var beforeRead *tg.Dialog
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		beforeRead, err = smokeGroupDialog(ctx, api, chatID)
		return err
	}); err != nil {
		t.Fatalf("A getDialogs before reading group: %v", err)
	}
	if beforeRead.UnreadCount != 2 {
		t.Fatalf("A group unread count before read = %d, want 2", beforeRead.UnreadCount)
	}

	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		result, err := api.MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, MaxID: readerMessageID,
		})
		if err != nil {
			return err
		}
		if result == nil {
			return errors.New("readHistory response is nil")
		}
		return nil
	}); err != nil {
		t.Fatalf("A read group history through local id %d: %v", readerMessageID, err)
	}

	assertSenderReceipt := func(collector *updateCollector, label string) {
		t.Helper()
		if got := recvOrCtx(t, f.ctx, collector.readOutbox, label+" updateReadHistoryOutbox max_id"); got != senderMessageID {
			t.Fatalf("%s updateReadHistoryOutbox max_id = %d, want C-local id %d", label, got, senderMessageID)
		}
		if got := recvOrCtx(t, f.ctx, collector.readOutboxPts, label+" updateReadHistoryOutbox pts"); got <= 0 {
			t.Fatalf("%s updateReadHistoryOutbox pts = %d, want positive pts", label, got)
		}
	}
	assertSenderReceipt(c.seen, "C managed updates")
	assertSenderReceipt(c.push, "C live push")

	for i, wantInboxID := range []int{readerMessageID, readerMessageID} {
		var dialog *tg.Dialog
		if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			var err error
			dialog, err = smokeGroupDialog(ctx, api, chatID)
			return err
		}); err != nil {
			t.Fatalf("A getDialogs after group read %d: %v", i+1, err)
		}
		if dialog.UnreadCount != 0 || dialog.ReadInboxMaxID != wantInboxID || dialog.ReadInboxMaxID <= beforeRead.ReadInboxMaxID {
			t.Fatalf("A group dialog after read %d = {unread:%d inbox:%d}, want unread=0 and advanced inbox=%d (before %d)", i+1, dialog.UnreadCount, dialog.ReadInboxMaxID, wantInboxID, beforeRead.ReadInboxMaxID)
		}
	}

	// A and B use equal IDs here; C's seeded ID space must retain its own ID in
	// the live notification and full-group display.
	var happyMessageID int
	var happySend tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		happySend, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Message: "group-pin-happy", RandomID: 1047004,
		})
		return err
	}); err != nil {
		t.Fatalf("send happy-path pin message: %v", err)
	}
	happyMessage, _, ok := outgoingMessage(t, happySend, "group-pin-happy")
	if !ok {
		t.Fatal("happy-path pin send omitted its outgoing message")
	}
	happyMessageID = happyMessage.ID
	happyLocalIDs := make(map[int64]int, 3)
	for _, member := range []*smokeClient{a, b, c} {
		if err := member.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			got, err := smokeGroupMessageID(ctx, api, chatID, "group-pin-happy")
			if err != nil {
				return err
			}
			if got <= 0 {
				return fmt.Errorf("happy-path pin copy id = %d for user %d, want positive ID", got, member.id)
			}
			happyLocalIDs[member.id] = got
			return nil
		}); err != nil {
			t.Fatalf("read happy-path pin copy for member %d: %v", member.id, err)
		}
	}
	if happyLocalIDs[a.id] != happyMessageID || happyLocalIDs[b.id] != happyMessageID {
		t.Fatalf("A/B happy-path pin IDs = %d/%d, want sender-local ID %d", happyLocalIDs[a.id], happyLocalIDs[b.id], happyMessageID)
	}
	var happyPinResult tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		happyPinResult, err = api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, ID: happyMessageID,
		})
		return err
	}); err != nil {
		t.Fatalf("pin happy-path group message: %v", err)
	}
	assertSmokePinResult(t, happyPinResult, true, chatID, []int{happyMessageID})
	assertSmokePinnedUpdate(t, recvOrCtx(t, f.ctx, b.push.pinnedMsg, "B happy-path pin push"), true, chatID, []int{happyLocalIDs[b.id]})
	assertSmokePinnedUpdate(t, recvOrCtx(t, f.ctx, c.push.pinnedMsg, "C happy-path pin push"), true, chatID, []int{happyLocalIDs[c.id]})
	for _, member := range []*smokeClient{a, b, c} {
		wantID := happyLocalIDs[member.id]
		if err := member.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			if err := verifySmokeGroupPin(ctx, api, chatID, wantID, "group-pin-happy"); err != nil {
				return err
			}
			result, err := api.MessagesSearch(ctx, &tg.MessagesSearchRequest{
				Peer: &tg.InputPeerChat{ChatID: chatID}, Q: "", Filter: &tg.InputMessagesFilterPinned{}, Limit: 100,
			})
			if err != nil {
				return fmt.Errorf("messages.search pinned group: %w", err)
			}
			pinned, ok := result.(*tg.MessagesMessages)
			if !ok || len(pinned.Messages) != 1 {
				return fmt.Errorf("pinned search result = %T, want one group message", result)
			}
			message, ok := pinned.Messages[0].(*tg.Message)
			if !ok || message.ID != wantID || message.Message != "group-pin-happy" {
				return fmt.Errorf("pinned search message = %T %+v, want id %d and text group-pin-happy", pinned.Messages[0], pinned.Messages[0], wantID)
			}
			return nil
		}); err != nil {
			t.Fatalf("reopen happy-path pinned group for member %d: %v", member.id, err)
		}
	}
	var happyUnpinResult tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		happyUnpinResult, err = api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, ID: happyMessageID, Unpin: true,
		})
		return err
	}); err != nil {
		t.Fatalf("unpin happy-path group message: %v", err)
	}
	assertSmokePinResult(t, happyUnpinResult, false, chatID, nil)
	assertSmokePinnedUpdate(t, recvOrCtx(t, f.ctx, b.push.pinnedMsg, "B happy-path unpin push"), false, chatID, nil)
	for _, member := range []*smokeClient{a, b, c} {
		if err := member.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			if err := verifySmokeGroupUnpinned(ctx, api, chatID); err != nil {
				return err
			}
			result, err := api.MessagesSearch(ctx, &tg.MessagesSearchRequest{
				Peer: &tg.InputPeerChat{ChatID: chatID}, Q: "", Filter: &tg.InputMessagesFilterPinned{}, Limit: 100,
			})
			if err != nil {
				return fmt.Errorf("messages.search after group unpin: %w", err)
			}
			pinned, ok := result.(*tg.MessagesMessages)
			if !ok || len(pinned.Messages) != 0 {
				return fmt.Errorf("pinned search after unpin = %T, want empty group messages", result)
			}
			return nil
		}); err != nil {
			t.Fatalf("reopen unpinned group for member %d: %v", member.id, err)
		}
	}

	// Advancing only B's local ID space makes the same next group message have
	// different IDs for A and B, which is the pin namespace regression.
	var pinSeedResult tg.UpdatesClass
	if err := b.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		pinSeedResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerSelf{}, Message: "group-pin-id-seed", RandomID: 1047005,
		})
		return err
	}); err != nil {
		t.Fatalf("seed B's message ID space: %v", err)
	}
	pinSeed, pinSeedPts, ok := outgoingMessage(t, pinSeedResult, "group-pin-id-seed")
	if !ok {
		t.Fatal("B ID seed omitted its outgoing message")
	}
	if pinSeedPts <= 0 {
		t.Fatalf("B ID seed pts = %d, want positive", pinSeedPts)
	}
	if err := b.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		return verifySmokeHistory(ctx, api, &tg.InputPeerSelf{}, b.id, map[string]smokeHistoryMessage{
			"group-pin-id-seed": {id: pinSeed.ID, out: true},
		})
	}); err != nil {
		t.Fatalf("verify B's Saved Messages ID seed: %v", err)
	}

	const mismatchText = "group-pin-mismatch"
	var mismatchSend tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		mismatchSend, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, Message: mismatchText, RandomID: 1047006,
		})
		return err
	}); err != nil {
		t.Fatalf("send mismatched-ID pin message: %v", err)
	}
	mismatchMessage, _, ok := outgoingMessage(t, mismatchSend, mismatchText)
	if !ok {
		t.Fatal("mismatched-ID pin send omitted its outgoing message")
	}
	ownerMessageIDs := make(map[int64]int, 2)
	for _, member := range []*smokeClient{a, b} {
		if err := member.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			id, err := smokeGroupMessageID(ctx, api, chatID, mismatchText)
			if err != nil {
				return err
			}
			ownerMessageIDs[member.id] = id
			return nil
		}); err != nil {
			t.Fatalf("read mismatched-ID copy for member %d: %v", member.id, err)
		}
	}
	if ownerMessageIDs[a.id] != mismatchMessage.ID {
		t.Fatalf("A group copy id = %d, want sender id %d", ownerMessageIDs[a.id], mismatchMessage.ID)
	}
	if ownerMessageIDs[a.id] == ownerMessageIDs[b.id] {
		t.Fatalf("ID seed did not create distinct owner-local IDs: A/B = %d", ownerMessageIDs[a.id])
	}
	var mismatchPinResult tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		mismatchPinResult, err = api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, ID: mismatchMessage.ID,
		})
		return err
	}); err != nil {
		t.Fatalf("pin mismatched-ID group message: %v", err)
	}
	assertSmokePinResult(t, mismatchPinResult, true, chatID, []int{ownerMessageIDs[a.id]})
	assertSmokePinnedUpdate(t, recvOrCtx(t, f.ctx, b.push.pinnedMsg, "B mismatched-ID pin push"), true, chatID, []int{ownerMessageIDs[b.id]})
	for _, member := range []*smokeClient{a, b} {
		wantID := ownerMessageIDs[member.id]
		if err := member.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			return verifySmokeGroupPin(ctx, api, chatID, wantID, mismatchText)
		}); err != nil {
			t.Fatalf("reopen mismatched-ID pinned group for member %d: %v", member.id, err)
		}
	}
	var mismatchUnpinResult tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		mismatchUnpinResult, err = api.MessagesUpdatePinnedMessage(ctx, &tg.MessagesUpdatePinnedMessageRequest{
			Peer: &tg.InputPeerChat{ChatID: chatID}, ID: mismatchMessage.ID, Unpin: true,
		})
		return err
	}); err != nil {
		t.Fatalf("unpin mismatched-ID group message: %v", err)
	}
	assertSmokePinResult(t, mismatchUnpinResult, false, chatID, nil)
	assertSmokePinnedUpdate(t, recvOrCtx(t, f.ctx, b.push.pinnedMsg, "B mismatched-ID unpin push"), false, chatID, nil)
	for _, member := range []*smokeClient{a, b} {
		if err := member.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
			return verifySmokeGroupUnpinned(ctx, api, chatID)
		}); err != nil {
			t.Fatalf("reopen unpinned mismatched-ID group for member %d: %v", member.id, err)
		}
	}
}

func testSmokeChannel(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phoneCreator, phoneSubscriber = "+15551048001", "+15551048002"
	seedPhoneUsers(t, f.ctx, f.store, phoneCreator, phoneSubscriber)
	creator := newSmokeClient(t, f, "A1", phoneCreator)
	subscriber := newSmokeClient(t, f, "B1", phoneSubscriber)
	execChannel(t, f.ctx, creator.cmds, func(ctx context.Context, client *tg.Client) error {
		cfg, err := client.HelpGetConfig(ctx)
		if err != nil {
			return err
		}
		if cfg.DCTxtDomainName != testPublicLinkPrefix {
			return fmt.Errorf("help.getConfig public link prefix = %q, want %q", cfg.DCTxtDomainName, testPublicLinkPrefix)
		}
		return nil
	})

	channelID := createBroadcastChannel(t, f.ctx, creator.cmds, "Smoke channel")
	hash := exportChannelInvite(t, f.ctx, creator.id, creator.cmds, channelID)
	if joinedID := importChannelInvite(t, f.ctx, subscriber.cmds, hash); joinedID != channelID {
		t.Fatalf("subscriber joined channel %d, want %d", joinedID, channelID)
	}

	const post = "channel-smoke"
	execChannel(t, f.ctx, creator.cmds, func(ctx context.Context, api *tg.Client) error {
		_, err := api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer:     peerChannel(creator.id, channelID),
			Message:  post,
			RandomID: 1048001,
		})
		return err
	})

	update := recvOrCtx(t, f.ctx, subscriber.seen.newChannelMsg, "subscriber channel update")
	if update.Msg.Message != post || update.Msg.ID <= 0 {
		t.Fatalf("subscriber channel message = {text:%q id:%d}, want {%q, positive id}", update.Msg.Message, update.Msg.ID, post)
	}
	peer, ok := update.Msg.PeerID.(*tg.PeerChannel)
	if !ok || peer.ChannelID != channelID {
		t.Fatalf("subscriber channel peer = %+v, want channel %d", update.Msg.PeerID, channelID)
	}
	from, ok := update.Msg.FromID.(*tg.PeerUser)
	if !ok || from.UserID != creator.id {
		t.Fatalf("subscriber channel sender = %+v, want creator %d", update.Msg.FromID, creator.id)
	}
	noDuplicate := time.NewTimer(50 * time.Millisecond)
	defer noDuplicate.Stop()
	select {
	case duplicate := <-subscriber.seen.newChannelMsg:
		t.Fatalf("subscriber received duplicate channel message: %+v", duplicate.Msg)
	case <-noDuplicate.C:
	case <-f.ctx.Done():
		t.Fatalf("waiting for duplicate channel message check: %s", contextFailureDescription(f.ctx))
	}

	if err := subscriber.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
			Peer:  peerChannel(subscriber.id, channelID),
			Limit: 10,
		})
		if err != nil {
			return err
		}
		history, ok := result.(*tg.MessagesChannelMessages)
		if !ok {
			return fmt.Errorf("channel history response = %T, want *tg.MessagesChannelMessages", result)
		}
		if len(history.Messages) != 1 {
			return fmt.Errorf("channel history count = %d, want 1", len(history.Messages))
		}
		message, ok := history.Messages[0].(*tg.Message)
		if !ok {
			return fmt.Errorf("channel history message = %T, want *tg.Message", history.Messages[0])
		}
		if message.Message != post || message.ID != update.Msg.ID || message.Out {
			return fmt.Errorf("channel history message = {text:%q id:%d out:%v}, want {%q id:%d out:false}", message.Message, message.ID, message.Out, post, update.Msg.ID)
		}
		peer, ok := message.PeerID.(*tg.PeerChannel)
		if !ok || peer.ChannelID != channelID {
			return fmt.Errorf("channel history peer = %+v, want channel %d", message.PeerID, channelID)
		}
		from, ok := message.FromID.(*tg.PeerUser)
		if !ok || from.UserID != creator.id {
			return fmt.Errorf("channel history sender = %+v, want creator %d", message.FromID, creator.id)
		}
		return nil
	}); err != nil {
		t.Fatalf("subscriber getHistory: %v", err)
	}

	if err := creator.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		fullResult, err := api.ChannelsGetFullChannel(ctx, inputChannel(creator.id, channelID))
		if err != nil {
			return err
		}
		full, ok := fullResult.FullChat.(*tg.ChannelFull)
		if !ok {
			return fmt.Errorf("creator getFullChannel = %T, want *tg.ChannelFull", fullResult.FullChat)
		}
		count, ok := full.GetParticipantsCount()
		if full.ID != channelID || !ok || count != 2 || !full.GetCanViewParticipants() {
			return fmt.Errorf("creator getFullChannel id/count/can_view = %d/%d/%v, want %d/2/true", full.ID, count, full.GetCanViewParticipants(), channelID)
		}
		if len(fullResult.Chats) != 1 {
			return fmt.Errorf("creator getFullChannel chats = %d, want 1", len(fullResult.Chats))
		}
		channel, ok := fullResult.Chats[0].(*tg.Channel)
		if !ok || channel.ID != channelID || !channel.Creator || channel.Left {
			return fmt.Errorf("creator getFullChannel channel = %+v, want creator member %d", fullResult.Chats[0], channelID)
		}

		participantsResult, err := api.ChannelsGetParticipants(ctx, &tg.ChannelsGetParticipantsRequest{
			Channel: inputChannel(creator.id, channelID),
			Filter:  &tg.ChannelParticipantsRecent{},
			Limit:   10,
		})
		if err != nil {
			return err
		}
		participants, ok := participantsResult.(*tg.ChannelsChannelParticipants)
		if !ok {
			return fmt.Errorf("getParticipants response = %T, want *tg.ChannelsChannelParticipants", participantsResult)
		}
		if participants.Count != count || len(participants.Participants) != count {
			return fmt.Errorf("getParticipants count/rows = %d/%d, want %d", participants.Count, len(participants.Participants), count)
		}
		roles := make(map[int64]string, len(participants.Participants))
		for _, participant := range participants.Participants {
			switch member := participant.(type) {
			case *tg.ChannelParticipantCreator:
				roles[member.UserID] = "creator"
			case *tg.ChannelParticipant:
				roles[member.UserID] = "member"
			default:
				return fmt.Errorf("getParticipants role = %T, want creator or member", participant)
			}
		}
		if len(roles) != 2 || roles[creator.id] != "creator" || roles[subscriber.id] != "member" {
			return fmt.Errorf("getParticipants roles = %v, want creator %d and subscriber %d", roles, creator.id, subscriber.id)
		}
		return nil
	}); err != nil {
		t.Fatalf("creator channel membership reads: %v", err)
	}

	if err := subscriber.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		result, err := api.ChannelsGetFullChannel(ctx, inputChannel(subscriber.id, channelID))
		if err != nil {
			return err
		}
		full, ok := result.FullChat.(*tg.ChannelFull)
		if !ok {
			return fmt.Errorf("subscriber getFullChannel = %T, want *tg.ChannelFull", result.FullChat)
		}
		count, ok := full.GetParticipantsCount()
		if full.ID != channelID || !ok || count != 2 || full.GetCanViewParticipants() {
			return fmt.Errorf("subscriber getFullChannel id/count/can_view = %d/%d/%v, want %d/2/false", full.ID, count, full.GetCanViewParticipants(), channelID)
		}
		if len(result.Chats) != 1 {
			return fmt.Errorf("subscriber getFullChannel chats = %d, want 1", len(result.Chats))
		}
		channel, ok := result.Chats[0].(*tg.Channel)
		if !ok || channel.ID != channelID || channel.Creator || channel.Left {
			return fmt.Errorf("subscriber getFullChannel channel = %+v, want current member %d", result.Chats[0], channelID)
		}
		return nil
	}); err != nil {
		t.Fatalf("subscriber getFullChannel: %v", err)
	}
}

func testSmokeContactsSearch(t *testing.T) {
	t.Helper()
	f := newSmokeFixture(t)
	const phoneA, phoneB = "+15551049001", "+15551049002"
	seedPhoneUsers(t, f.ctx, f.store, phoneA, phoneB)

	a := newSmokeClient(t, f, "A1", phoneA)
	b := newSmokeClient(t, f, "B1", phoneB)
	if err := f.store.ClaimUsername(f.ctx, a.id, "smokealpha"); err != nil {
		t.Fatalf("claim A username: %v", err)
	}
	if err := f.store.ClaimUsername(f.ctx, b.id, "smokebravo"); err != nil {
		t.Fatalf("claim B username: %v", err)
	}

	var search *tg.ContactsFound
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		search, err = api.ContactsSearch(ctx, &tg.ContactsSearchRequest{Q: "smokebravo", Limit: 10})
		return err
	}); err != nil {
		t.Fatalf("exact username search: %v", err)
	}
	if search == nil {
		t.Fatal("exact username search returned nil")
	}
	if len(search.Results) != 1 || len(search.Users) != 1 {
		t.Fatalf("exact username search results/users = %d/%d, want one each", len(search.Results), len(search.Users))
	}
	peer, ok := search.Results[0].(*tg.PeerUser)
	if !ok {
		t.Fatalf("exact username search peer type = %T, want *tg.PeerUser", search.Results[0])
	}
	if peer.UserID != b.id {
		t.Fatalf("exact username search peer id = %d, want user %d", peer.UserID, b.id)
	}
	searchUser, err := requireSmokeFullUser(search.Users, b.id, "contacts.search")
	if err != nil {
		t.Fatal(err)
	}

	var users []tg.UserClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		users, err = api.UsersGetUsers(ctx, []tg.InputUserClass{
			&tg.InputUser{UserID: searchUser.ID, AccessHash: searchUser.AccessHash},
		})
		return err
	}); err != nil {
		t.Fatalf("getUsers for search result: %v", err)
	}
	resolvedUser, err := requireSmokeFullUser(users, b.id, "users.getUsers")
	if err != nil {
		t.Fatal(err)
	}

	var added tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		added, err = api.ContactsAddContact(ctx, &tg.ContactsAddContactRequest{
			ID:        &tg.InputUser{UserID: resolvedUser.ID, AccessHash: resolvedUser.AccessHash},
			FirstName: "Smoke Bravo",
			Phone:     "+15550000000",
		})
		return err
	}); err != nil {
		t.Fatalf("add B as contact: %v", err)
	}
	if _, ok := added.(*tg.Updates); !ok {
		t.Fatalf("add contact response = %T, want *tg.Updates", added)
	}

	var contactResult tg.ContactsContactsClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		contactResult, err = api.ContactsGetContacts(ctx, 0)
		return err
	}); err != nil {
		t.Fatalf("A getContacts: %v", err)
	}
	contacts, ok := contactResult.(*tg.ContactsContacts)
	if !ok {
		t.Fatalf("A getContacts response = %T, want *tg.ContactsContacts", contactResult)
	}
	if contacts.SavedCount != 1 || len(contacts.Contacts) != 1 || contacts.Contacts[0].UserID != b.id {
		t.Fatalf("A getContacts saved/users = %d/%d, want B %d", contacts.SavedCount, len(contacts.Contacts), b.id)
	}
	contactUser, err := requireSmokeFullUser(contacts.Users, b.id, "A contacts.getContacts")
	if err != nil {
		t.Fatal(err)
	}
	if !contactUser.Contact {
		t.Fatalf("A getContacts B contact flag = false, want true")
	}

	var bContactsResult tg.ContactsContactsClass
	if err := b.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		bContactsResult, err = api.ContactsGetContacts(ctx, 0)
		return err
	}); err != nil {
		t.Fatalf("B getContacts: %v", err)
	}
	bContacts, ok := bContactsResult.(*tg.ContactsContacts)
	if !ok || bContacts.SavedCount != 0 || len(bContacts.Contacts) != 0 || len(bContacts.Users) != 0 {
		t.Fatalf("B sees A's one-sided contact edge: %T, want an empty contact list", bContactsResult)
	}

	const messageText = "contact-smoke"
	peerForSend := &tg.InputPeerUser{UserID: resolvedUser.ID, AccessHash: resolvedUser.AccessHash}
	var sendResult tg.UpdatesClass
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		sendResult, err = api.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: peerForSend, Message: messageText, RandomID: 1049001,
		})
		return err
	}); err != nil {
		t.Fatalf("send to searched B peer: %v", err)
	}
	sent, _, ok := outgoingMessage(t, sendResult, messageText)
	if !ok || countOutgoingMessages(sendResult, messageText) != 1 || sent.ID <= 0 || !sent.Out {
		t.Fatalf("send result does not contain one outgoing %q message", messageText)
	}
	sentPeer, ok := sent.PeerID.(*tg.PeerUser)
	if !ok || sentPeer.UserID != b.id {
		t.Fatalf("send result peer type/id = %T/%d, want B %d", sent.PeerID, smokePeerUserID(sent.PeerID), b.id)
	}

	var historyResult any
	if err := a.call(f.ctx, func(ctx context.Context, api *tg.Client) error {
		var err error
		historyResult, err = api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerForSend, Limit: 10})
		return err
	}); err != nil {
		t.Fatalf("A getHistory with searched B peer: %v", err)
	}
	history, ok := historyResult.(*tg.MessagesMessages)
	if !ok {
		t.Fatalf("A getHistory response = %T, want *tg.MessagesMessages", historyResult)
	}
	if len(history.Messages) != 1 {
		t.Fatalf("A getHistory response = %T, want one message", historyResult)
	}
	historyMessage, ok := history.Messages[0].(*tg.Message)
	if !ok {
		t.Fatalf("A getHistory message = %T, want *tg.Message", history.Messages[0])
	}
	if historyMessage.Message != messageText || historyMessage.ID != sent.ID || !historyMessage.Out {
		t.Fatalf("A getHistory text/id/out = %q/%d/%v, want %q/%d/true", historyMessage.Message, historyMessage.ID, historyMessage.Out, messageText, sent.ID)
	}
	historyPeer, ok := historyMessage.PeerID.(*tg.PeerUser)
	if !ok || historyPeer.UserID != b.id {
		t.Fatalf("A getHistory peer type/id = %T/%d, want B %d", historyMessage.PeerID, smokePeerUserID(historyMessage.PeerID), b.id)
	}
	if _, err := requireSmokeFullUser(history.Users, b.id, "A messages.getHistory"); err != nil {
		t.Fatal(err)
	}
}

func smokePeerUserID(peer tg.PeerClass) int64 {
	if user, ok := peer.(*tg.PeerUser); ok {
		return user.UserID
	}
	return 0
}

func testSmokeUsernameRegistration(t *testing.T) {
	t.Helper()
	f := newSmokeFixtureWithRegistration(t, config.RegistrationOpen)
	const username, pendingPhone, password = "smokenewacct", "+15551049003", "smoke-password-1049"
	pending, err := f.store.CreateUser(f.ctx, pendingPhone)
	if err != nil {
		t.Fatalf("create pending signup account: %v", err)
	}

	firstSession := &session.StorageMemory{}
	firstClient := f.savedSessionClient(firstSession)
	var accountID int64
	if err := firstClient.Run(f.ctx, func(ctx context.Context) error {
		api := firstClient.API()
		codeHash, err := sendCodeUsername(ctx, api, username)
		if err != nil {
			return fmt.Errorf("sendCode for signup: %w", err)
		}
		code, err := f.codes.wait(ctx, username)
		if err != nil {
			return fmt.Errorf("wait for in-memory signup code: %w", err)
		}
		sessionData, err := (&session.Loader{Storage: firstSession}).Load(ctx)
		if err != nil {
			return fmt.Errorf("load signup session: %w", err)
		}
		if len(sessionData.AuthKeyID) != 8 {
			return fmt.Errorf("signup auth key id length = %d, want 8", len(sessionData.AuthKeyID))
		}
		var authKeyID [8]byte
		copy(authKeyID[:], sessionData.AuthKeyID)
		if err := f.store.SetPendingUser(ctx, mtproto.AuthKeyIDInt64(authKeyID), pending.ID); err != nil {
			return fmt.Errorf("stage test signup account: %w", err)
		}

		response, err := signInUsername(ctx, api, username, codeHash, code)
		if err != nil {
			if !isSignUpRequired(err) {
				return fmt.Errorf("signIn before signup: %w", err)
			}
		} else if _, ok := response.(*tg.AuthAuthorizationSignUpRequired); !ok {
			return fmt.Errorf("signIn before signup response = %T, want signup required", response)
		}

		response, err = signUpUsername(ctx, api, username, codeHash, "Smoke", "Account")
		if err != nil {
			return fmt.Errorf("signUp: %w", err)
		}
		authorization, ok := response.(*tg.AuthAuthorization)
		if !ok || authorization.User == nil {
			return fmt.Errorf("signUp response = %T, want authorization with a user", response)
		}
		signupUser, ok := authorization.User.(*tg.User)
		if !ok || signupUser.ID <= 0 {
			return fmt.Errorf("signUp user = %T, want a full user", authorization.User)
		}
		accountID = signupUser.ID
		passwordState, err := api.AccountGetPassword(ctx)
		if err != nil {
			return fmt.Errorf("usable RPC after signUp: %w", err)
		}
		if passwordState.HasPassword {
			return errors.New("new smoke account unexpectedly has a password")
		}
		verifier, salt1, salt2, err := testComputeSRPVerifier([]byte(password))
		if err != nil {
			return fmt.Errorf("prepare smoke password: %w", err)
		}
		_, err = api.AccountUpdatePasswordSettings(ctx, &tg.AccountUpdatePasswordSettingsRequest{
			Password: &tg.InputCheckPasswordEmpty{},
			NewSettings: tg.AccountPasswordInputSettings{
				NewAlgo: &tg.PasswordKdfAlgoSHA256SHA256PBKDF2HMACSHA512iter100000SHA256ModPow{
					Salt1: salt1,
					Salt2: salt2,
				},
				NewPasswordHash: verifier,
			},
		})
		if err != nil {
			return fmt.Errorf("set synthetic smoke password: %w", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("open username registration: %v", err)
	}

	secondSession := &session.StorageMemory{}
	secondClient := f.savedSessionClient(secondSession)
	if err := secondClient.Run(f.ctx, func(ctx context.Context) error {
		api := secondClient.API()
		codeHash, err := sendCodeUsername(ctx, api, username)
		if err != nil {
			return fmt.Errorf("sendCode for fresh sign-in: %w", err)
		}
		code, err := f.codes.wait(ctx, username)
		if err != nil {
			return fmt.Errorf("wait for in-memory sign-in code: %w", err)
		}
		response, err := signInUsername(ctx, api, username, codeHash, code)
		if !isSessionPasswordNeeded(err) {
			if err != nil {
				return fmt.Errorf("fresh signIn expected password challenge: %w", err)
			}
			return fmt.Errorf("fresh signIn response = %T, want password challenge", response)
		}
		passwordState, err := api.AccountGetPassword(ctx)
		if err != nil {
			return fmt.Errorf("get fresh sign-in password challenge: %w", err)
		}
		if !passwordState.HasPassword {
			return errors.New("fresh sign-in account has no password")
		}
		proof, err := auth.PasswordHash([]byte(password), passwordState.SRPID, passwordState.SRPB, passwordState.SecureRandom, passwordState.CurrentAlgo)
		if err != nil {
			return fmt.Errorf("compute fresh sign-in proof: %w", err)
		}
		response, err = api.AuthCheckPassword(ctx, proof)
		if err != nil {
			return fmt.Errorf("complete fresh signIn: %w", err)
		}
		authorization, ok := response.(*tg.AuthAuthorization)
		if !ok || authorization.User == nil {
			return fmt.Errorf("fresh signIn response = %T, want user %d", response, accountID)
		}
		signinUser, ok := authorization.User.(*tg.User)
		if !ok || signinUser.ID != accountID {
			return fmt.Errorf("fresh signIn user = %T, want user %d", authorization.User, accountID)
		}
		if _, err := api.AccountGetPassword(ctx); err != nil {
			return fmt.Errorf("usable RPC after fresh signIn: %w", err)
		}
		return nil
	}); err != nil {
		t.Fatalf("fresh username sign-in: %v", err)
	}
}

func requireSmokeFullUser(users []tg.UserClass, userID int64, source string) (*tg.User, error) {
	var found *tg.User
	for _, candidate := range users {
		switch user := candidate.(type) {
		case *tg.User:
			if user.ID == userID {
				if found != nil {
					return nil, fmt.Errorf("%s returned duplicate user %d", source, userID)
				}
				found = user
			}
		case *tg.UserEmpty:
			if user.ID == userID {
				return nil, fmt.Errorf("%s returned userEmpty for user %d", source, userID)
			}
		}
	}
	if found == nil {
		return nil, fmt.Errorf("%s omitted user %d", source, userID)
	}
	if found.AccessHash == 0 {
		return nil, fmt.Errorf("%s returned user %d without a usable peer identity", source, userID)
	}
	return found, nil
}

type smokeFixture struct {
	ctx      context.Context
	failures *clientFailureSignal
	key      *rsa.PrivateKey
	dsn      string
	store    *store.Store
	codes    *multiCodeSink
	dcID     int
	port     int
	listener *acceptCountingListener
	registry *mtproto.SessionRegistry
	stop     func()
	regMode  config.RegistrationMode
}

func newSmokeFixture(t *testing.T) *smokeFixture {
	t.Helper()
	return newSmokeFixtureWithRegistration(t, config.RegistrationClosed)
}

func newSmokeFixtureWithRegistration(t *testing.T, regMode config.RegistrationMode) *smokeFixture {
	t.Helper()
	deadlineCtx, cancelDeadline := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancelDeadline)
	deadlineCtx = withRegistrySnapshotState(deadlineCtx)
	ctx, cancelFailure := context.WithCancelCause(deadlineCtx)
	t.Cleanup(func() { cancelFailure(nil) })
	key, err := rsakey.LoadOrGenerate(filepath.Join(t.TempDir(), "key.pem"))
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})
	f := &smokeFixture{ctx: ctx, failures: newClientFailureSignal(cancelFailure), key: key, dsn: dsn, store: st, codes: newMultiCodeSink(), dcID: 2, regMode: regMode}
	f.start(t, "127.0.0.1:0")
	return f
}

func (f *smokeFixture) start(t *testing.T, address string) {
	t.Helper()
	baseListener := mustListen(t, f.ctx, address)
	ln := newAcceptCountingListener(baseListener)
	if _, ok := ln.Addr().(*net.TCPAddr); !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	f.port = tcpPort(t, ln)
	f.listener = ln
	f.registry, f.stop = bootServerWithRegistryAndRegistrationMode(t, f.ctx, f.key, f.dcID, f.store, f.dsn, f.codes.Logger(), ln, f.regMode)
	stop := f.stop
	t.Cleanup(stop)
}

func (f *smokeFixture) restart(t *testing.T) {
	t.Helper()
	f.stop()
	f.start(t, fmt.Sprintf("127.0.0.1:%d", f.port))
}

func (f *smokeFixture) managedClient(sess *session.StorageMemory, seen, push *updateCollector, manager *updates.Manager) *telegram.Client {
	return telegram.NewClient(1, "hash", telegram.Options{
		DC:             f.dcID,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: f.dcID, IPAddress: "127.0.0.1", Port: f.port}}},
		PublicKeys:     []telegram.PublicKey{{RSA: &f.key.PublicKey}},
		Resolver:       dcs.Plain(dcs.PlainOptions{}),
		SessionStorage: sess,
		UpdateHandler:  observedManagerHandler{observer: push, manager: manager},
		Middlewares: []telegram.Middleware{
			hook.UpdateHook(manager.Handle),
			hook.AffectedHook(manager),
		},
	})
}

func (f *smokeFixture) savedSessionClient(sess *session.StorageMemory) *telegram.Client {
	return telegram.NewClient(1, "hash", telegram.Options{
		DC:             f.dcID,
		DCList:         dcs.List{Options: []tg.DCOption{{ID: f.dcID, IPAddress: "127.0.0.1", Port: f.port}}},
		PublicKeys:     []telegram.PublicKey{{RSA: &f.key.PublicKey}},
		Resolver:       dcs.Plain(dcs.PlainOptions{}),
		SessionStorage: sess,
	})
}

type smokeClient struct {
	client    *telegram.Client
	session   *session.StorageMemory
	manager   *updates.Manager
	seen      *updateCollector
	push      *updateCollector
	cmds      chan command
	lifecycle *clientLifecycle
	label     string
	id        int64
	stop      sync.Once
}

func newSmokeClient(t *testing.T, f *smokeFixture, label, phone string) *smokeClient {
	t.Helper()
	sess := &session.StorageMemory{}
	seen, push := newUpdateCollector(), newUpdateCollector()
	manager := updates.New(updates.Config{Handler: seen})
	client := &smokeClient{
		client:  f.managedClient(sess, seen, push, manager),
		session: sess,
		manager: manager,
		seen:    seen,
		push:    push,
		cmds:    make(chan command),
		label:   label,
	}
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
			return f.codes.wait(ctx, phone)
		})),
		auth.SendCodeOptions{},
	)
	ids, ready := make(chan int64, 1), make(chan struct{}, 1)
	client.lifecycle = startClientLifecycle(f.ctx, label, f.failures, func(phase *clientPhaseState) error {
		return runManagedInteractive(f.ctx, client.client, flow, ids, ready, client.cmds, manager, true, phase)
	})
	t.Cleanup(func() { client.stopClient(t) })
	loginStarted := time.Now()
	select {
	case client.id = <-ids:
	case <-f.ctx.Done():
		t.Fatalf("%s", client.lifecycle.diagnostic("login", time.Since(loginStarted), contextFailureDescription(f.ctx)))
	case <-client.lifecycle.result.done:
		t.Fatalf("%s", client.lifecycle.diagnostic("login", time.Since(loginStarted), "cause="+safeErrorClass(client.lifecycle.result.error())))
	}
	managerStarted := time.Now()
	select {
	case <-ready:
	case <-f.ctx.Done():
		t.Fatalf("%s", client.lifecycle.diagnostic("manager readiness", time.Since(managerStarted), contextFailureDescription(f.ctx)))
	case <-client.lifecycle.result.done:
		t.Fatalf("%s", client.lifecycle.diagnostic("manager readiness", time.Since(managerStarted), "cause="+safeErrorClass(client.lifecycle.result.error())))
	}
	return client
}

func (c *smokeClient) call(ctx context.Context, fn func(context.Context, *tg.Client) error) error {
	started := time.Now()
	done := make(chan error, 1)
	select {
	case c.cmds <- command{fn: fn, done: done}:
	case <-c.lifecycle.result.done:
		return errors.New(c.lifecycle.diagnostic("command", time.Since(started), "cause="+safeErrorClass(c.lifecycle.result.error())))
	case <-ctx.Done():
		return errors.New(c.lifecycle.diagnostic("command", time.Since(started), contextFailureDescription(ctx)))
	}
	select {
	case err := <-done:
		return err
	case <-c.lifecycle.result.done:
		return errors.New(c.lifecycle.diagnostic("command", time.Since(started), "cause="+safeErrorClass(c.lifecycle.result.error())))
	case <-ctx.Done():
		return errors.New(c.lifecycle.diagnostic("command", time.Since(started), contextFailureDescription(ctx)))
	}
}

func (c *smokeClient) stopClient(t *testing.T) {
	t.Helper()
	c.stop.Do(func() {
		stopClientLifecycle(t, c.lifecycle, func() { close(c.cmds) })
		c.manager.Reset()
	})
}

type smokeSend struct {
	Message string
	ID      int
}

func assertSmokeSendResult(t *testing.T, result tg.UpdatesClass, text string, wantID, wantPts int) smokeSend {
	t.Helper()
	message, pts, ok := outgoingMessage(t, result, text)
	if !ok || countOutgoingMessages(result, text) != 1 {
		t.Fatalf("sendMessage result omitted exactly one outgoing %q", text)
	}
	if message.ID != wantID {
		t.Fatalf("outgoing %q id = %d, want %d", text, message.ID, wantID)
	}
	if pts != wantPts {
		t.Fatalf("outgoing %q pts = %d, want %d", text, pts, wantPts)
	}
	return smokeSend{Message: message.Message, ID: message.ID}
}

type smokeHistoryMessage struct {
	id  int
	out bool
}

func verifySmokeHistory(
	ctx context.Context,
	api *tg.Client,
	peer tg.InputPeerClass,
	peerID int64,
	want map[string]smokeHistoryMessage,
) error {
	result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peer, Limit: 10})
	if err != nil {
		return err
	}
	history, ok := result.(*tg.MessagesMessages)
	if !ok {
		return fmt.Errorf("history response = %T, want *tg.MessagesMessages", result)
	}
	if len(history.Messages) != len(want) {
		return fmt.Errorf("history count = %d, want %d", len(history.Messages), len(want))
	}
	seen := make(map[string]bool, len(want))
	for _, class := range history.Messages {
		message, ok := class.(*tg.Message)
		if !ok {
			return fmt.Errorf("history message = %T, want *tg.Message", class)
		}
		expect, ok := want[message.Message]
		if !ok {
			return fmt.Errorf("history contains unexpected text %q", message.Message)
		}
		if seen[message.Message] {
			return fmt.Errorf("history contains duplicate text %q", message.Message)
		}
		seen[message.Message] = true
		if message.ID != expect.id || message.Out != expect.out {
			return fmt.Errorf("history %q = {id:%d out:%v}, want {id:%d out:%v}", message.Message, message.ID, message.Out, expect.id, expect.out)
		}
		peer, ok := message.PeerID.(*tg.PeerUser)
		if !ok || peer.UserID != peerID {
			return fmt.Errorf("history %q peer = %+v, want user %d", message.Message, message.PeerID, peerID)
		}
	}
	for text := range want {
		if !seen[text] {
			return fmt.Errorf("history is missing %q", text)
		}
	}
	return nil
}

func smokeGroupDialog(ctx context.Context, api *tg.Client, chatID int64) (*tg.Dialog, error) {
	result, err := api.MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		OffsetPeer: &tg.InputPeerEmpty{},
		Limit:      20,
	})
	if err != nil {
		return nil, err
	}
	var dialogs []tg.DialogClass
	switch response := result.(type) {
	case *tg.MessagesDialogs:
		dialogs = response.Dialogs
	case *tg.MessagesDialogsSlice:
		dialogs = response.Dialogs
	default:
		return nil, fmt.Errorf("getDialogs response = %T, want MessagesDialogs or MessagesDialogsSlice", result)
	}
	for _, entry := range dialogs {
		dialog, ok := entry.(*tg.Dialog)
		if !ok {
			continue
		}
		peer, ok := dialog.Peer.(*tg.PeerChat)
		if ok && peer.ChatID == chatID {
			return dialog, nil
		}
	}
	return nil, fmt.Errorf("getDialogs omitted chat %d", chatID)
}

func verifySmokeGroupHistory(
	ctx context.Context,
	api *tg.Client,
	chatID, viewerID int64,
	want map[string]int64,
) error {
	result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chatID}, Limit: 10,
	})
	if err != nil {
		return err
	}
	history, ok := result.(*tg.MessagesMessages)
	if !ok {
		return fmt.Errorf("group history response = %T, want *tg.MessagesMessages", result)
	}
	seen := make(map[string]bool, len(want))
	ids := make(map[int]bool, len(want))
	for _, class := range history.Messages {
		message, ok := class.(*tg.Message)
		if !ok {
			if _, service := class.(*tg.MessageService); service {
				continue
			}
			return fmt.Errorf("group history message = %T, want *tg.Message or *tg.MessageService", class)
		}
		senderID, ok := want[message.Message]
		if !ok {
			return fmt.Errorf("group history contains unexpected text %q", message.Message)
		}
		if seen[message.Message] {
			return fmt.Errorf("group history contains duplicate text %q", message.Message)
		}
		seen[message.Message] = true
		if message.ID <= 0 || ids[message.ID] {
			return fmt.Errorf("group history %q has invalid owner-local id %d", message.Message, message.ID)
		}
		ids[message.ID] = true
		if message.Out != (senderID == viewerID) {
			return fmt.Errorf("group history %q out = %t for viewer %d, want %t", message.Message, message.Out, viewerID, senderID == viewerID)
		}
		from, ok := message.FromID.(*tg.PeerUser)
		if !ok || from.UserID != senderID {
			return fmt.Errorf("group history %q sender = %+v, want user %d", message.Message, message.FromID, senderID)
		}
		peer, ok := message.PeerID.(*tg.PeerChat)
		if !ok || peer.ChatID != chatID {
			return fmt.Errorf("group history %q peer = %+v, want chat %d", message.Message, message.PeerID, chatID)
		}
	}
	for text := range want {
		if !seen[text] {
			return fmt.Errorf("group history for %d is missing %q", viewerID, text)
		}
	}
	return nil
}

func smokeGroupMessageID(ctx context.Context, api *tg.Client, chatID int64, wantText string) (int, error) {
	result, err := api.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{
		Peer: &tg.InputPeerChat{ChatID: chatID}, Limit: 20,
	})
	if err != nil {
		return 0, err
	}
	history, ok := result.(*tg.MessagesMessages)
	if !ok {
		return 0, fmt.Errorf("group history response = %T, want *tg.MessagesMessages", result)
	}
	for _, class := range history.Messages {
		message, ok := class.(*tg.Message)
		if !ok || message.Message != wantText {
			continue
		}
		peer, ok := message.PeerID.(*tg.PeerChat)
		if !ok || peer.ChatID != chatID {
			return 0, fmt.Errorf("group message %q peer = %+v, want chat %d", wantText, message.PeerID, chatID)
		}
		return message.ID, nil
	}
	return 0, fmt.Errorf("group history is missing %q", wantText)
}

func verifySmokeGroupPin(ctx context.Context, api *tg.Client, chatID int64, wantID int, wantText string) error {
	fullResult, err := api.MessagesGetFullChat(ctx, chatID)
	if err != nil {
		return fmt.Errorf("getFullChat: %w", err)
	}
	full, ok := fullResult.FullChat.(*tg.ChatFull)
	if !ok {
		return fmt.Errorf("full chat = %T, want *tg.ChatFull", fullResult.FullChat)
	}
	pinnedID, ok := full.GetPinnedMsgID()
	if !ok || pinnedID != wantID {
		return fmt.Errorf("full chat pinned id = %d/%t, want %d", pinnedID, ok, wantID)
	}
	gotID, err := smokeGroupMessageID(ctx, api, chatID, wantText)
	if err != nil {
		return err
	}
	if gotID != wantID {
		return fmt.Errorf("pinned text %q has owner-local id %d, want %d", wantText, gotID, wantID)
	}
	return nil
}

func verifySmokeGroupUnpinned(ctx context.Context, api *tg.Client, chatID int64) error {
	result, err := api.MessagesGetFullChat(ctx, chatID)
	if err != nil {
		return fmt.Errorf("getFullChat: %w", err)
	}
	full, ok := result.FullChat.(*tg.ChatFull)
	if !ok {
		return fmt.Errorf("full chat = %T, want *tg.ChatFull", result.FullChat)
	}
	if id, present := full.GetPinnedMsgID(); present && id != 0 {
		return fmt.Errorf("full chat pinned id = %d after unpin, want none", id)
	}
	return nil
}

func assertSmokePinResult(t *testing.T, result tg.UpdatesClass, wantPinned bool, chatID int64, wantMessages []int) {
	t.Helper()
	updates, ok := result.(*tg.Updates)
	if !ok {
		t.Fatalf("pin result = %T, want *tg.Updates", result)
	}
	for _, update := range updates.Updates {
		pinned, ok := update.(*tg.UpdatePinnedMessages)
		if ok {
			assertSmokePinnedUpdate(t, pinned, wantPinned, chatID, wantMessages)
			return
		}
	}
	t.Fatal("pin result omitted updatePinnedMessages")
}

func assertSmokePinnedUpdate(t *testing.T, update *tg.UpdatePinnedMessages, wantPinned bool, chatID int64, wantMessages []int) {
	t.Helper()
	peer, ok := update.Peer.(*tg.PeerChat)
	if update.Pinned != wantPinned || !ok || peer.ChatID != chatID {
		t.Fatalf("pin update = {pinned:%t peer:%T}, want pinned=%t chat=%d", update.Pinned, update.Peer, wantPinned, chatID)
	}
	if len(update.Messages) != len(wantMessages) {
		t.Fatalf("pin update messages = %v, want %v", update.Messages, wantMessages)
	}
	for i, id := range wantMessages {
		if update.Messages[i] != id {
			t.Fatalf("pin update messages = %v, want %v", update.Messages, wantMessages)
		}
	}
}

func assertSmokeReconnect(
	t *testing.T,
	f *smokeFixture,
	sess *session.StorageMemory,
	wantUserID, viewerID, peerID int64,
	want map[string]smokeHistoryMessage,
) {
	t.Helper()
	client := f.savedSessionClient(sess)
	if err := client.Run(f.ctx, func(ctx context.Context) error {
		status, err := client.Auth().Status(ctx)
		if err != nil {
			return err
		}
		if !status.Authorized || status.User == nil || status.User.ID != wantUserID {
			return fmt.Errorf("reconnected authorization = %+v, want authorized user %d", status, wantUserID)
		}
		return verifySmokeHistory(ctx, client.API(), peerUser(viewerID, peerID), peerID, want)
	}); err != nil {
		t.Fatalf("reconnect saved session for %d: %v", wantUserID, err)
	}
}
