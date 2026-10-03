package api_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/store"
)

func getParticipantsViaDispatcher(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	provisional bool,
	channel tg.InputChannelClass,
	filter tg.ChannelParticipantsFilterClass,
	offset, limit int,
) (*tg.ChannelsChannelParticipants, *mt.RPCError) {
	t.Helper()
	method := settingsHandler{
		name: "channels.getParticipants",
		request: func() bin.Encoder {
			return &tg.ChannelsGetParticipantsRequest{
				Channel: channel,
				Filter:  filter,
				Offset:  offset,
				Limit:   limit,
			}
		},
	}
	body := dispatchSettings(t, h, method, userID, provisional)
	var result tg.ChannelsChannelParticipants
	if err := result.Decode(&bin.Buffer{Buf: body}); err == nil {
		return &result, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode getParticipants response: %v", err)
	}
	return nil, &rpc
}

func getParticipantViaDispatcher(
	t *testing.T,
	h mtproto.Handler,
	userID int64,
	provisional bool,
	channel tg.InputChannelClass,
	participant tg.InputPeerClass,
) (*tg.ChannelsChannelParticipant, *mt.RPCError) {
	t.Helper()
	method := settingsHandler{
		name: "channels.getParticipant",
		request: func() bin.Encoder {
			return &tg.ChannelsGetParticipantRequest{Channel: channel, Participant: participant}
		},
	}
	body := dispatchSettings(t, h, method, userID, provisional)
	var result tg.ChannelsChannelParticipant
	if err := result.Decode(&bin.Buffer{Buf: body}); err == nil {
		return &result, nil
	}
	var rpc mt.RPCError
	if err := rpc.Decode(&bin.Buffer{Buf: body}); err != nil {
		t.Fatalf("decode getParticipant response: %v", err)
	}
	return nil, &rpc
}

func participantID(p tg.ChannelParticipantClass) int64 {
	switch v := p.(type) {
	case *tg.ChannelParticipant:
		return v.UserID
	case *tg.ChannelParticipantSelf:
		return v.UserID
	case *tg.ChannelParticipantCreator:
		return v.UserID
	case *tg.ChannelParticipantAdmin:
		return v.UserID
	case *tg.ChannelParticipantBanned:
		peer, ok := v.Peer.(*tg.PeerUser)
		if ok {
			return peer.UserID
		}
	}
	return 0
}

func assertParticipantPageIDs(t *testing.T, page *tg.ChannelsChannelParticipants, wantIDs, excludedIDs []int64) {
	t.Helper()
	if page.Count != len(wantIDs) {
		t.Errorf("participant count = %d, want %d", page.Count, len(wantIDs))
	}
	if len(page.Participants) != len(wantIDs) {
		t.Errorf("participant rows = %d, want %d", len(page.Participants), len(wantIDs))
	}
	gotIDs := make(map[int64]bool, len(page.Participants))
	for _, participant := range page.Participants {
		gotIDs[participantID(participant)] = true
	}
	for _, id := range wantIDs {
		if !gotIDs[id] {
			t.Errorf("participant rows %v are missing matching user %d", gotIDs, id)
		}
	}
	for _, id := range excludedIDs {
		if gotIDs[id] {
			t.Errorf("participant rows %v unexpectedly include excluded user %d", gotIDs, id)
		}
	}
}

func TestGetParticipantsEnforcesBroadcastVisibilityAndRendersStoredRoles(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openStoreDSN(t)
	creator := mustUser(t, s, "+15551982001")
	admin := mustUser(t, s, "+15551982002")
	member := mustUser(t, s, "+15551982003")
	outsider := mustUser(t, s, "+15551982004")
	broadcast := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Broadcast: true, Title: "Roster"})
	if _, err := api.InviteToChannelForTest(s, creator.ID, &tg.ChannelsInviteToChannelRequest{
		Channel: api.InputChannel(creator.ID, broadcast.ID),
		Users: []tg.InputUserClass{
			api.InputUser(creator.ID, admin.ID),
			api.InputUser(creator.ID, member.ID),
		},
	}); err != nil {
		t.Fatalf("invite admin and member: %v", err)
	}
	if err := s.SetChannelRole(ctx, broadcast.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	if err := api.ClaimChannelUsernameForTest(s, broadcast.ID, "roster_public"); err != nil {
		t.Fatalf("claim public username: %v", err)
	}
	h := fullChannelDispatcher(s)
	for _, session := range []struct {
		name        string
		userID      int64
		provisional bool
	}{
		{name: "unauthenticated"},
		{name: "provisional", userID: creator.ID, provisional: true},
	} {
		t.Run(session.name, func(t *testing.T) {
			_, rpc := getParticipantsViaDispatcher(t, h, session.userID, session.provisional, api.InputChannel(creator.ID, broadcast.ID), &tg.ChannelParticipantsRecent{}, 0, 20)
			if rpc == nil || rpc.ErrorMessage != "AUTH_KEY_UNREGISTERED" {
				t.Errorf("getParticipants error = %v, want AUTH_KEY_UNREGISTERED", rpc)
			}
			_, rpc = getParticipantViaDispatcher(t, h, session.userID, session.provisional, api.InputChannel(creator.ID, broadcast.ID), &tg.InputPeerSelf{})
			if rpc == nil || rpc.ErrorMessage != "AUTH_KEY_UNREGISTERED" {
				t.Errorf("getParticipant error = %v, want AUTH_KEY_UNREGISTERED", rpc)
			}
		})
	}

	listed, rpc := getParticipantsViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, broadcast.ID), &tg.ChannelParticipantsRecent{}, 0, 20)
	if rpc != nil {
		t.Fatalf("creator getParticipants: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if listed.Count != 3 || len(listed.Participants) != 3 {
		t.Fatalf("creator roster count/rows = %d/%d, want 3/3", listed.Count, len(listed.Participants))
	}
	roles := make(map[int64]tg.ChannelParticipantClass, len(listed.Participants))
	for _, participant := range listed.Participants {
		roles[participantID(participant)] = participant
	}
	if _, ok := roles[creator.ID].(*tg.ChannelParticipantCreator); !ok {
		t.Errorf("creator row = %T, want creator", roles[creator.ID])
	}
	adminRow, ok := roles[admin.ID].(*tg.ChannelParticipantAdmin)
	if !ok {
		t.Errorf("admin row = %T, want admin", roles[admin.ID])
	} else {
		if adminRow.PromotedBy != 0 {
			t.Errorf("admin promoted_by = %d, want unknown/zero", adminRow.PromotedBy)
		}
		if _, present := adminRow.GetInviterID(); present {
			t.Error("admin row fabricated an inviter identity")
		}
	}
	if _, ok := roles[member.ID].(*tg.ChannelParticipant); !ok {
		t.Errorf("member row = %T, want member", roles[member.ID])
	}
	if len(listed.Chats) != 1 {
		t.Fatalf("roster chats = %d, want the referenced channel", len(listed.Chats))
	}
	for _, id := range []int64{creator.ID, admin.ID, member.ID} {
		user, ok := loadUsersWire(t, listed.Users, id).(*tg.User)
		if !ok {
			t.Errorf("roster user %d = %T, want a requester-scoped profile", id, loadUsersWire(t, listed.Users, id))
			continue
		}
		if user.AccessHash != api.DeriveUserHash(creator.ID, id) {
			t.Errorf("user %d access hash = %d, want viewer-derived hash", id, user.AccessHash)
		}
		if id != creator.ID && user.Phone != "" {
			t.Errorf("creator roster exposed user %d phone %q", id, user.Phone)
		}
	}

	_, rpc = getParticipantsViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, broadcast.ID), &tg.ChannelParticipantsRecent{}, 0, 20)
	if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("broadcast member list error = %v, want PEER_ID_INVALID", rpc)
	}
	self, rpc := getParticipantViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, broadcast.ID), &tg.InputPeerSelf{})
	if rpc != nil {
		t.Fatalf("broadcast member getParticipant(self): %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if participantID(self.Participant) != member.ID || len(self.Users) != 1 {
		t.Errorf("self participant id/users = %d/%d, want only member %d", participantID(self.Participant), len(self.Users), member.ID)
	}
	if _, ok := self.Participant.(*tg.ChannelParticipant); !ok {
		t.Errorf("self role = %T, want stored member role", self.Participant)
	}
	if user, ok := loadUsersWire(t, self.Users, member.ID).(*tg.User); !ok || user.AccessHash != api.DeriveUserHash(member.ID, member.ID) {
		t.Errorf("self profile = %#v, want member-scoped user", loadUsersWire(t, self.Users, member.ID))
	}
	for _, target := range []int64{creator.ID, 999999999} {
		_, rpc := getParticipantViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, broadcast.ID), api.InputPeerUser(member.ID, target))
		if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
			t.Errorf("member getParticipant(%d) error = %v, want PEER_ID_INVALID", target, rpc)
		}
	}
	adminView, rpc := getParticipantViaDispatcher(t, h, admin.ID, false, api.InputChannel(admin.ID, broadcast.ID), api.InputPeerUser(admin.ID, creator.ID))
	if rpc != nil {
		t.Fatalf("admin getParticipant(creator): %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if _, ok := adminView.Participant.(*tg.ChannelParticipantCreator); !ok {
		t.Errorf("admin inspection of creator = %T, want creator role", adminView.Participant)
	}

	// A viewer-derived hash for a public peer is only an address, not list rights.
	_, rpc = getParticipantsViaDispatcher(t, h, outsider.ID, false, api.InputChannel(outsider.ID, broadcast.ID), &tg.ChannelParticipantsRecent{}, 0, 20)
	if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("public outsider getParticipants error = %v, want PEER_ID_INVALID", rpc)
	}
	_, rpc = getParticipantsViaDispatcher(t, h, outsider.ID, false, api.InputChannel(outsider.ID, broadcast.ID+1), &tg.ChannelParticipantsRecent{}, 0, 20)
	if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("absent channel getParticipants error = %v, want PEER_ID_INVALID", rpc)
	}
	if err := s.SetChannelBan(ctx, broadcast.ID, creator.ID, member.ID, nil, true); err != nil {
		t.Fatalf("ban broadcast member: %v", err)
	}
	_, rpc = getParticipantsViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, broadcast.ID), &tg.ChannelParticipantsRecent{}, 0, 20)
	if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("banned viewer getParticipants error = %v, want PEER_ID_INVALID", rpc)
	}
}

func TestGetParticipantsMegagroupFiltersExcludeBansAndRequireAdmin(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := mustUser(t, s, "+15551982101")
	member := mustUser(t, s, "+15551982102")
	banned := mustUser(t, s, "+15551982103")
	group := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "Group"})
	joinChannel(t, ctx, dsn, group.ID, member.ID)
	joinChannel(t, ctx, dsn, group.ID, banned.ID)
	if err := s.SetChannelBan(ctx, group.ID, creator.ID, banned.ID, nil, true); err != nil {
		t.Fatalf("ban participant: %v", err)
	}
	h := fullChannelDispatcher(s)

	listed, rpc := getParticipantsViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, group.ID), &tg.ChannelParticipantsRecent{}, 0, 20)
	if rpc != nil {
		t.Fatalf("megagroup member list: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if listed.Count != 2 || len(listed.Participants) != 2 {
		t.Fatalf("active megagroup members count/rows = %d/%d, want 2/2", listed.Count, len(listed.Participants))
	}
	for _, row := range listed.Participants {
		if participantID(row) == banned.ID {
			t.Error("non-banned member list included a banned participant")
		}
	}
	_, rpc = getParticipantsViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, group.ID), &tg.ChannelParticipantsBanned{}, 0, 20)
	if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("member banned-filter error = %v, want PEER_ID_INVALID", rpc)
	}
	_, rpc = getParticipantsViaDispatcher(t, h, member.ID, false, api.InputChannel(member.ID, group.ID), &tg.ChannelParticipantsKicked{}, 0, 20)
	if rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("member kicked-filter error = %v, want PEER_ID_INVALID", rpc)
	}

	for _, filter := range []tg.ChannelParticipantsFilterClass{&tg.ChannelParticipantsBanned{}, &tg.ChannelParticipantsKicked{}} {
		bannedPage, rpc := getParticipantsViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, group.ID), filter, 0, 20)
		if rpc != nil {
			t.Fatalf("creator banned filter %T: %d %s", filter, rpc.ErrorCode, rpc.ErrorMessage)
		}
		if bannedPage.Count != 1 || len(bannedPage.Participants) != 1 || participantID(bannedPage.Participants[0]) != banned.ID {
			t.Errorf("creator %T filter count/rows = %d/%v, want banned user %d", filter, bannedPage.Count, bannedPage.Participants, banned.ID)
		}
		if _, ok := bannedPage.Participants[0].(*tg.ChannelParticipantBanned); !ok {
			t.Errorf("banned row = %T, want banned participant", bannedPage.Participants[0])
		}
		user, ok := loadUsersWire(t, bannedPage.Users, banned.ID).(*tg.User)
		if !ok || user.AccessHash != api.DeriveUserHash(creator.ID, banned.ID) || user.Phone != "" {
			t.Errorf("admin banned-user profile = %#v, want requester hash and no phone", loadUsersWire(t, bannedPage.Users, banned.ID))
		}
	}
	row, rpc := getParticipantViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, group.ID), api.InputPeerUser(creator.ID, banned.ID))
	if rpc != nil {
		t.Fatalf("creator getParticipant(banned): %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if _, ok := row.Participant.(*tg.ChannelParticipantBanned); !ok {
		t.Errorf("getParticipant banned row = %T, want banned participant", row.Participant)
	}
}

func TestGetParticipantsAdminSearchAndContactsFiltersMatchRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := mustUser(t, s, "+15551982401")
	admin := mustUser(t, s, "+15551982402")
	searchMatch := mustUser(t, s, "+15551982403")
	contactMatch := mustUser(t, s, "+15551982404")
	otherMember := mustUser(t, s, "+15551982405")
	contactOutsideChannel := mustUser(t, s, "+15551982406")
	group := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "Filter matches"})
	for _, user := range []store.User{admin, searchMatch, contactMatch, otherMember} {
		joinChannel(t, ctx, dsn, group.ID, user.ID)
	}
	if err := s.SetChannelRole(ctx, group.ID, creator.ID, admin.ID, 1); err != nil {
		t.Fatalf("promote admin: %v", err)
	}
	for _, user := range []struct {
		id        int64
		firstName string
	}{
		{id: searchMatch.ID, firstName: "Needle Match"},
		{id: contactMatch.ID, firstName: "Contact Match"},
		{id: otherMember.ID, firstName: "Plain Member"},
	} {
		if err := api.SetUserFirstNameForTest(dsn, user.id, user.firstName); err != nil {
			t.Fatalf("set user %d first name: %v", user.id, err)
		}
	}
	for _, contactID := range []int64{contactMatch.ID, contactOutsideChannel.ID} {
		if changed, err := s.AddContact(ctx, creator.ID, contactID); err != nil || !changed {
			t.Fatalf("add creator contact %d: changed=%v err=%v", contactID, changed, err)
		}
	}
	h := fullChannelDispatcher(s)
	channel := api.InputChannel(creator.ID, group.ID)

	admins, rpc := getParticipantsViaDispatcher(t, h, creator.ID, false, channel, &tg.ChannelParticipantsAdmins{}, 0, 20)
	if rpc != nil {
		t.Fatalf("admins filter: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	assertParticipantPageIDs(t, admins, []int64{creator.ID, admin.ID}, []int64{searchMatch.ID, contactMatch.ID, otherMember.ID})

	search, rpc := getParticipantsViaDispatcher(t, h, creator.ID, false, channel, &tg.ChannelParticipantsSearch{Q: "Needle"}, 0, 20)
	if rpc != nil {
		t.Fatalf("search filter: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	assertParticipantPageIDs(t, search, []int64{searchMatch.ID}, []int64{creator.ID, admin.ID, contactMatch.ID, otherMember.ID})

	contacts, rpc := getParticipantsViaDispatcher(t, h, creator.ID, false, channel, &tg.ChannelParticipantsContacts{}, 0, 20)
	if rpc != nil {
		t.Fatalf("contacts filter: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	assertParticipantPageIDs(t, contacts, []int64{contactMatch.ID}, []int64{creator.ID, admin.ID, searchMatch.ID, otherMember.ID, contactOutsideChannel.ID})
}

func TestGetParticipantsBoundsPageOffsetAndSearch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := mustUser(t, s, "+15551982201")
	group := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "Page"})
	channelExec(t, ctx, dsn, `
WITH new_users AS (
    INSERT INTO users (phone, first_name)
    SELECT '1999822' || i::text, 'Page user'
    FROM generate_series(1, 205) AS i
    RETURNING id
)
INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
SELECT $1, id, 0, 0 FROM new_users`, group.ID)
	h := fullChannelDispatcher(s)

	page, rpc := getParticipantsViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, group.ID), &tg.ChannelParticipantsRecent{}, 0, 999)
	if rpc != nil {
		t.Fatalf("oversized participants page: %d %s", rpc.ErrorCode, rpc.ErrorMessage)
	}
	if page.Count != 206 || len(page.Participants) != 200 {
		t.Errorf("participants count/page size = %d/%d, want 206/200", page.Count, len(page.Participants))
	}
	for _, tc := range []struct {
		name   string
		offset int
		filter tg.ChannelParticipantsFilterClass
	}{
		{name: "negative offset", offset: -1, filter: &tg.ChannelParticipantsRecent{}},
		{name: "offset past bound", offset: 10001, filter: &tg.ChannelParticipantsRecent{}},
		{name: "search too long", filter: &tg.ChannelParticipantsSearch{Q: strings.Repeat("x", 257)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, rpc := getParticipantsViaDispatcher(t, h, creator.ID, false, api.InputChannel(creator.ID, group.ID), tc.filter, tc.offset, 20)
			if rpc == nil {
				t.Fatal("invalid page request succeeded")
			}
			want := "LIMIT_INVALID"
			if tc.name == "search too long" {
				want = "SEARCH_QUERY_TOO_LONG"
			}
			if rpc.ErrorMessage != want {
				t.Errorf("error = %s, want %s", rpc.ErrorMessage, want)
			}
		})
	}
}

func TestGetParticipantsSnapshotCannotAddAfterViewerRemoval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := mustUser(t, s, "+15551982301")
	viewer := mustUser(t, s, "+15551982302")
	group := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "Race"})
	joinChannel(t, ctx, dsn, group.ID, viewer.ID)
	h := fullChannelDispatcher(s)

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	store.SetChannelParticipantsSnapshotHook(s, func() {
		close(entered)
		<-release
	})
	defer func() {
		if !released {
			close(release)
		}
		store.SetChannelParticipantsSnapshotHook(s, nil)
	}()

	type outcome struct {
		response *tg.ChannelsChannelParticipants
		rpc      *mt.RPCError
	}
	done := make(chan outcome, 1)
	go func() {
		response, rpc := getParticipantsViaDispatcher(t, h, viewer.ID, false, api.InputChannel(viewer.ID, group.ID), &tg.ChannelParticipantsRecent{}, 0, 20)
		done <- outcome{response: response, rpc: rpc}
	}()
	select {
	case <-entered:
	case result := <-done:
		t.Fatalf("participant snapshot did not reach read barrier: response=%v rpc=%v", result.response, result.rpc)
	case <-time.After(2 * time.Second):
		t.Fatal("participant snapshot did not reach its read barrier")
	}

	left, err := s.LeaveChannel(ctx, group.ID, viewer.ID)
	if err != nil || !left {
		t.Fatalf("remove viewer during read: left=%v err=%v", left, err)
	}
	added := mustUser(t, s, "+15551982303")
	joinChannel(t, ctx, dsn, group.ID, added.ID)
	close(release)
	released = true

	var result outcome
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("participant snapshot did not finish after membership change")
	}
	if result.rpc != nil {
		t.Fatalf("in-flight participant snapshot: %d %s", result.rpc.ErrorCode, result.rpc.ErrorMessage)
	}
	if result.response.Count != 2 || len(result.response.Participants) != 2 {
		t.Errorf("snapshot count/page = %d/%d, want original two members", result.response.Count, len(result.response.Participants))
	}
	for _, participant := range result.response.Participants {
		if participantID(participant) == added.ID {
			t.Error("request that began before removal included the newly added participant")
		}
	}
	for _, user := range result.response.Users {
		if wire, ok := user.(*tg.User); ok && wire.ID == added.ID {
			t.Error("request that began before removal returned the new participant's profile")
		}
		if empty, ok := user.(*tg.UserEmpty); ok && empty.ID == added.ID {
			t.Error("request returned a userEmpty entry for a participant added after viewer removal")
		}
	}
	if _, rpc := getParticipantsViaDispatcher(t, h, viewer.ID, false, api.InputChannel(viewer.ID, group.ID), &tg.ChannelParticipantsRecent{}, 0, 20); rpc == nil || rpc.ErrorMessage != "PEER_ID_INVALID" {
		t.Errorf("post-removal participant list error = %v, want PEER_ID_INVALID", rpc)
	}
}

func TestGetParticipantSnapshotCannotAddAfterViewerRemoval(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	creator := mustUser(t, s, "+15551982311")
	viewer := mustUser(t, s, "+15551982312")
	added := mustUser(t, s, "+15551982313")
	group := createChannel(t, s, creator.ID, &tg.ChannelsCreateChannelRequest{Megagroup: true, Title: "Participant race"})
	joinChannel(t, ctx, dsn, group.ID, viewer.ID)
	h := fullChannelDispatcher(s)

	entered := make(chan struct{})
	release := make(chan struct{})
	released := false
	store.SetChannelParticipantsSnapshotHook(s, func() {
		close(entered)
		<-release
	})
	defer func() {
		if !released {
			close(release)
		}
		store.SetChannelParticipantsSnapshotHook(s, nil)
	}()

	type outcome struct {
		response *tg.ChannelsChannelParticipant
		rpc      *mt.RPCError
	}
	done := make(chan outcome, 1)
	go func() {
		response, rpc := getParticipantViaDispatcher(
			t,
			h,
			viewer.ID,
			false,
			api.InputChannel(viewer.ID, group.ID),
			api.InputPeerUser(viewer.ID, added.ID),
		)
		done <- outcome{response: response, rpc: rpc}
	}()
	select {
	case <-entered:
	case result := <-done:
		t.Fatalf("participant snapshot did not reach read barrier: response=%v rpc=%v", result.response, result.rpc)
	case <-time.After(2 * time.Second):
		t.Fatal("participant snapshot did not reach its read barrier")
	}

	left, err := s.LeaveChannel(ctx, group.ID, viewer.ID)
	if err != nil || !left {
		t.Fatalf("remove viewer during read: left=%v err=%v", left, err)
	}
	joinChannel(t, ctx, dsn, group.ID, added.ID)
	close(release)
	released = true

	var result outcome
	select {
	case result = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("participant snapshot did not finish after membership change")
	}
	if result.rpc == nil || result.rpc.ErrorMessage != "PEER_ID_INVALID" {
		if result.rpc != nil {
			t.Fatalf("in-flight getParticipant error = %s, want PEER_ID_INVALID", result.rpc.ErrorMessage)
		}
		if result.response == nil {
			t.Fatal("in-flight getParticipant returned neither a response nor an RPC error")
		}
		if participantID(result.response.Participant) == added.ID {
			t.Errorf("request that began before removal returned the newly admitted participant")
		}
		for _, user := range result.response.Users {
			if wire, ok := user.(*tg.User); ok && wire.ID == added.ID {
				t.Error("request that began before removal returned the new participant's profile")
			}
		}
		t.Fatalf("in-flight getParticipant unexpectedly succeeded for newly admitted user %d", added.ID)
	}
}
