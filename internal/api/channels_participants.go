package api

import (
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	maxChannelParticipantsPage       = 200
	maxChannelParticipantsOffset     = 10000
	maxChannelParticipantsQueryBytes = 256
)

type channelParticipantsFilter struct {
	storeFilter int32
	query       string
	adminOnly   bool
}

func parseChannelParticipantsFilter(filter tg.ChannelParticipantsFilterClass) (channelParticipantsFilter, error) {
	var parsed channelParticipantsFilter
	switch f := filter.(type) {
	case *tg.ChannelParticipantsRecent:
		parsed.storeFilter = store.ChannelParticipantFilterRecent
	case *tg.ChannelParticipantsAdmins:
		parsed.storeFilter = store.ChannelParticipantFilterAdmins
	case *tg.ChannelParticipantsBanned:
		parsed.storeFilter = store.ChannelParticipantFilterBanned
		parsed.query = f.Q
		parsed.adminOnly = true
	case *tg.ChannelParticipantsKicked:
		parsed.storeFilter = store.ChannelParticipantFilterKicked
		parsed.query = f.Q
		parsed.adminOnly = true
	case *tg.ChannelParticipantsSearch:
		parsed.storeFilter = store.ChannelParticipantFilterSearch
		parsed.query = f.Q
		if parsed.query == "" {
			return channelParticipantsFilter{}, errSearchQueryEmpty
		}
	case *tg.ChannelParticipantsContacts:
		parsed.storeFilter = store.ChannelParticipantFilterContacts
	case *tg.ChannelParticipantsBots:
		parsed.storeFilter = store.ChannelParticipantFilterBots
	case *tg.ChannelParticipantsMentions:
		parsed.storeFilter = store.ChannelParticipantFilterMentions
		parsed.query = f.Q
	default:
		return channelParticipantsFilter{}, errInputFilterInvalid
	}
	if len(parsed.query) > maxChannelParticipantsQueryBytes {
		return channelParticipantsFilter{}, errSearchQueryTooLong
	}
	if !validText(parsed.query) {
		return channelParticipantsFilter{}, errSearchQueryInvalid
	}
	return parsed, nil
}

// handleGetParticipants applies the channel list policy before rendering one
// repeatable-read page. Broadcast members cannot list anyone; megagroup members
// can list current participants, while banned rows require admin rights.
func (h *handlers) handleGetParticipants(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsGetParticipantsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if req.Offset < 0 || req.Offset > maxChannelParticipantsOffset || req.Limit < 1 {
		return nil, errLimitInvalid
	}
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}
	filter, err := parseChannelParticipantsFilter(req.Filter)
	if err != nil {
		return nil, err
	}
	limit := min(req.Limit, maxChannelParticipantsPage)
	snapshot, found, err := h.store.ChannelParticipantsPageForViewer(
		r.Ctx,
		channelID,
		r.UserID,
		filter.storeFilter,
		filter.query,
		int32(req.Offset),
		int32(limit),
	)
	if err != nil {
		h.log.Error("get channel participants", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !found || !snapshot.HasViewer || snapshot.Viewer.Banned(time.Now()) {
		return nil, errPeerIDInvalid
	}
	admin := snapshot.Viewer.Role >= channelRoleAdmin
	if filter.adminOnly && !admin {
		return nil, errPeerIDInvalid
	}
	if !snapshot.Channel.Megagroup && !admin {
		return nil, errPeerIDInvalid
	}

	ids := make(map[int64]bool, len(snapshot.Participants))
	participants := make([]tg.ChannelParticipantClass, len(snapshot.Participants))
	now := time.Now()
	for i, member := range snapshot.Participants {
		ids[member.UserID] = true
		participants[i] = channelParticipantToTL(member, r.UserID, now)
	}
	users, err := h.loadUsersForChannelParticipants(r.Ctx, ids, r.UserID)
	if err != nil {
		h.log.Error("get channel participant users", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.ChannelsChannelParticipants{
		Count:        int(snapshot.Count),
		Participants: participants,
		Chats:        []tg.ChatClass{h.channelToTL(snapshot.Channel, snapshot.Viewer, true, r.UserID)},
		Users:        users,
	}, nil
}

// handleGetParticipant lets a broadcast member inspect only their own row.
// Megagroup members and channel admins can inspect rows their role permits.
func (h *handlers) handleGetParticipant(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ChannelsGetParticipantRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	channelID, err := h.inputChannelID(req.Channel, r.UserID)
	if err != nil {
		return nil, err
	}
	participantID, err := h.peerUserID(req.Participant, r.UserID)
	if err != nil {
		return nil, err
	}
	snapshot, found, err := h.store.ChannelParticipantForViewer(r.Ctx, channelID, r.UserID, participantID)
	if err != nil {
		h.log.Error("get channel participant", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !found || !snapshot.HasViewer || snapshot.Viewer.Banned(time.Now()) {
		return nil, errPeerIDInvalid
	}

	admin := snapshot.Viewer.Role >= channelRoleAdmin
	canViewOther := admin || snapshot.Channel.Megagroup
	if participantID != r.UserID && !canViewOther {
		return nil, errPeerIDInvalid
	}
	if !snapshot.HasParticipant || (snapshot.Participant.Banned(time.Now()) && !admin) {
		return nil, errPeerIDInvalid
	}
	users, err := h.loadUsersForChannelParticipants(r.Ctx, map[int64]bool{participantID: true}, r.UserID)
	if err != nil {
		h.log.Error("get channel participant user", "channel_id", channelID, "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.ChannelsChannelParticipant{
		Participant: channelParticipantToTL(snapshot.Participant, r.UserID, time.Now()),
		Chats:       []tg.ChatClass{h.channelToTL(snapshot.Channel, snapshot.Viewer, true, r.UserID)},
		Users:       users,
	}, nil
}

func channelParticipantToTL(member store.ChannelMember, viewerID int64, now time.Time) tg.ChannelParticipantClass {
	date := 0
	if !member.Date.IsZero() {
		date = int(member.Date.Unix())
	}
	if member.Banned(now) {
		untilDate := 0
		if !member.Forever() && member.BannedUntil != nil {
			untilDate = int(member.BannedUntil.Unix())
		}
		return &tg.ChannelParticipantBanned{
			Left: true,
			Peer: &tg.PeerUser{UserID: member.UserID},
			Date: date,
			BannedRights: tg.ChatBannedRights{
				ViewMessages: true,
				UntilDate:    untilDate,
			},
		}
	}
	switch member.Role {
	case channelRoleCreator:
		return &tg.ChannelParticipantCreator{
			UserID:      member.UserID,
			AdminRights: tg.ChatAdminRights{Other: true},
		}
	case channelRoleAdmin:
		return &tg.ChannelParticipantAdmin{
			Self:        member.UserID == viewerID,
			UserID:      member.UserID,
			PromotedBy:  0,
			Date:        date,
			AdminRights: tg.ChatAdminRights{Other: true},
		}
	default:
		return &tg.ChannelParticipant{UserID: member.UserID, Date: date}
	}
}
