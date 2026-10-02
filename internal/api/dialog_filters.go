package api

import (
	"errors"
	"slices"
	"strings"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	dialogFilterRateLimitSurface = "dialog_filter_mutation"
	maxDialogFilterPeers         = 100
	maxDialogFilterRawPeers      = 100
	maxDialogFilterEntities      = 12
	maxDialogFilterOrder         = 256
)

var dialogFilterMutationRateLimit = store.RateLimitConfig{Limit: 60, Window: time.Minute}

func (h *handlers) checkDialogFilterRateLimit(req *mtproto.Request) error {
	result, err := h.store.CheckRateLimitCost(req.Ctx, req.UserID, dialogFilterRateLimitSurface, dialogFilterMutationRateLimit, 1)
	if err != nil {
		h.log.Error("dialog filter rate limit", "user_id", req.UserID, "err", err)
		return errInternal
	}
	if result != nil {
		h.recordRateLimitDenial(dialogFilterRateLimitSurface)
		return FloodWaitError(60)
	}
	return nil
}

func (h *handlers) registerDialogFilterMutation(d *mtproto.Dispatcher, id uint32, fn methodFunc) {
	d.HandleFunc(id, func(c *mtproto.Conn, req *mtproto.Request) error {
		if provisionalBlocked(id, req) {
			return c.SendErr(req, errAuthKeyUnreg)
		}
		result, err := fn(req)
		if err != nil {
			if req.UserID != 0 && !req.Provisional {
				h.dialogFilterSync.RequesterRepair(c, req)
			}
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) {
				rpc = errInternal
			}
			return c.SendErr(req, rpc)
		}
		if err := c.SendResult(req, result); err != nil {
			if req.UserID != 0 && !req.Provisional {
				h.dialogFilterSync.RequesterRepair(c, req)
			}
			return err
		}
		return nil
	})
}

func (h *handlers) handleGetDialogFilters(c *mtproto.Conn, req *mtproto.Request) (bin.Encoder, func(), error) {
	var request tg.MessagesGetDialogFiltersRequest
	if err := request.Decode(req.Buf); err != nil {
		return nil, nil, errMethodNotImpl
	}
	if req.UserID == 0 || req.Provisional {
		return nil, nil, errAuthKeyUnreg
	}

	h.dialogFilterSync.EnsureBinding(c, req)
	captured := h.dialogFilterSync.Capture(c, req)
	snapshot, err := h.store.DialogFilters(req.Ctx, req.UserID)
	if err != nil {
		h.log.Error("get dialog filters", "user_id", req.UserID, "err", err)
		return nil, nil, errInternal
	}
	peers := make([]store.DialogFilterPeer, 0)
	for i := range snapshot.Filters {
		peers = append(peers, snapshot.Filters[i].PinnedPeers...)
		peers = append(peers, snapshot.Filters[i].IncludePeers...)
		peers = append(peers, snapshot.Filters[i].ExcludePeers...)
	}
	accessible, err := h.store.AccessibleDialogFilterPeers(req.Ctx, req.UserID, peers, h.now())
	if err != nil {
		h.log.Error("authorize stored dialog filter peers", "user_id", req.UserID, "err", err)
		return nil, nil, errInternal
	}
	filters := make(map[int]store.DialogFilter, len(snapshot.Filters))
	for _, filter := range snapshot.Filters {
		filter.PinnedPeers = accessibleDialogFilterPeers(filter.PinnedPeers, accessible)
		filter.IncludePeers = accessibleDialogFilterPeers(filter.IncludePeers, accessible)
		filter.ExcludePeers = accessibleDialogFilterPeers(filter.ExcludePeers, accessible)
		filters[filter.ID] = filter
	}

	ordered := make([]tg.DialogFilterClass, 0, len(filters)+1)
	for _, id := range snapshot.Order {
		if id == 0 {
			ordered = append(ordered, &tg.DialogFilterDefault{})
			continue
		}
		filter, ok := filters[id]
		if !ok {
			continue
		}
		ordered = append(ordered, h.dialogFilterToTL(filter, req.UserID))
	}
	if len(ordered) == 0 {
		ordered = append(ordered, &tg.DialogFilterDefault{})
	}
	result := &tg.MessagesDialogFilters{Filters: ordered}
	return result, func() { h.dialogFilterSync.AcknowledgeFetch(c, req, captured) }, nil
}

func (h *handlers) handleUpdateDialogFilter(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesUpdateDialogFilterRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if err := h.checkDialogFilterRateLimit(r); err != nil {
		return nil, err
	}
	if req.ID < 2 || req.ID > 255 {
		return nil, errFilterIDInvalid
	}
	if req.Filter == nil {
		if _, err := h.store.DeleteDialogFilter(r.Ctx, r.UserID, req.ID); err != nil {
			h.log.Error("delete dialog filter", "user_id", r.UserID, "err", err)
			return nil, errInternal
		}
		return &tg.BoolTrue{}, nil
	}
	filter, ok := req.Filter.(*tg.DialogFilter)
	if !ok || filter == nil || filter.ID != req.ID {
		return nil, errFilterIDInvalid
	}
	if filter.Title.Text == "" {
		return nil, errFilterTitleEmpty
	}
	if !utf8.ValidString(filter.Title.Text) || utf8.RuneCountInString(filter.Title.Text) > 12 {
		return nil, errMessageTooLong
	}
	if len(filter.Title.Entities) > maxDialogFilterEntities {
		return nil, errEntitiesTooLong
	}
	entities, err := validateDialogFilterEntities(filter.Title)
	if err != nil {
		return nil, err
	}
	emoticon, _ := filter.GetEmoticon()
	if len([]byte(emoticon)) > 64 {
		return nil, errMessageTooLong
	}

	if len(filter.PinnedPeers) > maxDialogFilterRawPeers || len(filter.IncludePeers) > maxDialogFilterRawPeers || len(filter.ExcludePeers) > maxDialogFilterRawPeers {
		return nil, errPeerIDInvalid
	}
	pinned, included, excluded, err := h.dialogFilterPeerLists(r, filter)
	if err != nil {
		return nil, err
	}
	pinned, included, excluded = normalizeDialogFilterPeers(pinned, included, excluded)
	if len(pinned)+len(included) > maxDialogFilterPeers || len(excluded) > maxDialogFilterPeers {
		return nil, errPeerIDInvalid
	}
	if !filter.Contacts && !filter.NonContacts && !filter.Groups && !filter.Broadcasts && !filter.Bots && len(pinned)+len(included) == 0 {
		return nil, errFilterIncludeEmpty
	}

	definition := store.DialogFilter{
		ID:              req.ID,
		Title:           filter.Title.Text,
		Entities:        entities,
		Emoticon:        emoticon,
		Contacts:        filter.Contacts,
		NonContacts:     filter.NonContacts,
		Groups:          filter.Groups,
		Broadcasts:      filter.Broadcasts,
		Bots:            filter.Bots,
		ExcludeMuted:    filter.ExcludeMuted,
		ExcludeRead:     filter.ExcludeRead,
		ExcludeArchived: filter.ExcludeArchived,
		TitleNoanimate:  filter.TitleNoanimate,
		PinnedPeers:     pinned,
		IncludePeers:    included,
		ExcludePeers:    excluded,
	}
	if color, ok := filter.GetColor(); ok && color >= 0 && color <= 6 {
		definition.Color = &color
	}
	if err := h.store.SaveDialogFilter(r.Ctx, r.UserID, definition); err != nil {
		if errors.Is(err, store.ErrDialogFilterLimit) {
			return nil, errFilterIDInvalid
		}
		h.log.Error("save dialog filter", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.BoolTrue{}, nil
}

func (h *handlers) handleUpdateDialogFiltersOrder(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesUpdateDialogFiltersOrderRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if err := h.checkDialogFilterRateLimit(r); err != nil {
		return nil, err
	}
	if len(req.Order) > maxDialogFilterOrder {
		return nil, errFilterIDInvalid
	}
	if _, err := h.store.UpdateDialogFilterOrder(r.Ctx, r.UserID, req.Order); err != nil {
		h.log.Error("reorder dialog filters", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.BoolTrue{}, nil
}

func (h *handlers) handleGetSuggestedDialogFilters(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetSuggestedDialogFiltersRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	existing, err := h.store.DialogFilterDefinitions(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get dialog filter suggestions", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.DialogFilterSuggestedVector{Elems: suggestedDialogFilters(existing)}, nil
}

func suggestedDialogFilters(existing []store.DialogFilter) []tg.DialogFilterSuggested {
	candidates := []struct {
		definition  store.DialogFilter
		description string
	}{
		{
			definition: store.DialogFilter{
				Title: "Unread", Contacts: true, NonContacts: true, Groups: true,
				Broadcasts: true, Bots: true, ExcludeRead: true,
			},
			description: "Chats with unread messages",
		},
		{
			definition:  store.DialogFilter{Title: "Personal", Contacts: true, NonContacts: true},
			description: "Private chats",
		},
	}
	usedIDs := make(map[int]struct{}, len(existing))
	for _, filter := range existing {
		usedIDs[filter.ID] = struct{}{}
	}

	suggested := make([]tg.DialogFilterSuggested, 0, len(candidates))
	for _, candidate := range candidates {
		if hasSuggestedDialogFilter(existing, candidate.definition) {
			continue
		}
		id := nextDialogFilterID(usedIDs)
		if id == 0 {
			break
		}
		usedIDs[id] = struct{}{}
		filter := dialogFilterTemplateToTL(candidate.definition, id)
		suggested = append(suggested, tg.DialogFilterSuggested{
			Filter: filter, Description: candidate.description,
		})
	}
	return suggested
}

func hasSuggestedDialogFilter(existing []store.DialogFilter, candidate store.DialogFilter) bool {
	for _, filter := range existing {
		if strings.EqualFold(strings.TrimSpace(filter.Title), candidate.Title) ||
			(filter.Contacts == candidate.Contacts &&
				filter.NonContacts == candidate.NonContacts &&
				filter.Groups == candidate.Groups &&
				filter.Broadcasts == candidate.Broadcasts &&
				filter.Bots == candidate.Bots &&
				filter.ExcludeMuted == candidate.ExcludeMuted &&
				filter.ExcludeRead == candidate.ExcludeRead &&
				filter.ExcludeArchived == candidate.ExcludeArchived &&
				len(filter.PinnedPeers) == 0 && len(filter.IncludePeers) == 0 &&
				len(filter.ExcludePeers) == 0) {
			return true
		}
	}
	return false
}

func nextDialogFilterID(used map[int]struct{}) int {
	for id := 2; id <= 255; id++ {
		if _, exists := used[id]; !exists {
			return id
		}
	}
	return 0
}

func dialogFilterTemplateToTL(filter store.DialogFilter, id int) *tg.DialogFilter {
	out := &tg.DialogFilter{
		ID: id, Title: tg.TextWithEntities{Text: filter.Title},
		Contacts: filter.Contacts, NonContacts: filter.NonContacts,
		Groups: filter.Groups, Broadcasts: filter.Broadcasts, Bots: filter.Bots,
		ExcludeMuted: filter.ExcludeMuted, ExcludeRead: filter.ExcludeRead,
		ExcludeArchived: filter.ExcludeArchived, TitleNoanimate: filter.TitleNoanimate,
	}
	out.SetFlags()
	return out
}

func (h *handlers) dialogFilterPeerLists(r *mtproto.Request, filter *tg.DialogFilter) ([]store.DialogFilterPeer, []store.DialogFilterPeer, []store.DialogFilterPeer, error) {
	lists := [][]tg.InputPeerClass{filter.PinnedPeers, filter.IncludePeers, filter.ExcludePeers}
	parsed := [3][]store.DialogFilterPeer{}
	var invalid bool
	all := make([]store.DialogFilterPeer, 0, len(filter.PinnedPeers)+len(filter.IncludePeers)+len(filter.ExcludePeers))
	for listIndex, peers := range lists {
		parsed[listIndex] = make([]store.DialogFilterPeer, 0, len(peers))
		for _, peer := range peers {
			peerType, peerID, err := h.inputPeer(peer, r.UserID)
			if err != nil {
				invalid = true
				continue
			}
			ref := store.DialogFilterPeer{Type: peerType, ID: peerID}
			parsed[listIndex] = append(parsed[listIndex], ref)
			all = append(all, ref)
		}
	}
	if invalid {
		return nil, nil, nil, errPeerIDInvalid
	}
	accessible, err := h.store.AccessibleDialogFilterPeers(r.Ctx, r.UserID, all, h.now())
	if err != nil {
		h.log.Error("authorize dialog filter input peers", "user_id", r.UserID, "err", err)
		return nil, nil, nil, errInternal
	}
	for _, peer := range all {
		if !accessible[peer] {
			return nil, nil, nil, errPeerIDInvalid
		}
	}
	return parsed[0], parsed[1], parsed[2], nil
}

func normalizeDialogFilterPeers(pinned, included, excluded []store.DialogFilterPeer) ([]store.DialogFilterPeer, []store.DialogFilterPeer, []store.DialogFilterPeer) {
	seen := make(map[store.DialogFilterPeer]struct{}, len(pinned)+len(included)+len(excluded))
	normalize := func(peers []store.DialogFilterPeer) []store.DialogFilterPeer {
		out := make([]store.DialogFilterPeer, 0, len(peers))
		for _, peer := range peers {
			if _, ok := seen[peer]; ok {
				continue
			}
			seen[peer] = struct{}{}
			out = append(out, peer)
		}
		return out
	}
	return normalize(pinned), normalize(included), normalize(excluded)
}

func accessibleDialogFilterPeers(peers []store.DialogFilterPeer, accessible map[store.DialogFilterPeer]bool) []store.DialogFilterPeer {
	return slices.DeleteFunc(peers, func(peer store.DialogFilterPeer) bool { return !accessible[peer] })
}

func (h *handlers) dialogFilterToTL(filter store.DialogFilter, ownerID int64) *tg.DialogFilter {
	entities := make([]tg.MessageEntityClass, len(filter.Entities))
	for i, entity := range filter.Entities {
		entities[i] = &tg.MessageEntityCustomEmoji{Offset: entity.Offset, Length: entity.Length, DocumentID: entity.DocumentID}
	}
	out := &tg.DialogFilter{
		ID:              filter.ID,
		Title:           tg.TextWithEntities{Text: filter.Title, Entities: entities},
		Contacts:        filter.Contacts,
		NonContacts:     filter.NonContacts,
		Groups:          filter.Groups,
		Broadcasts:      filter.Broadcasts,
		Bots:            filter.Bots,
		ExcludeMuted:    filter.ExcludeMuted,
		ExcludeRead:     filter.ExcludeRead,
		ExcludeArchived: filter.ExcludeArchived,
		TitleNoanimate:  filter.TitleNoanimate,
		PinnedPeers:     h.dialogFilterPeersToTL(filter.PinnedPeers, ownerID),
		IncludePeers:    h.dialogFilterPeersToTL(filter.IncludePeers, ownerID),
		ExcludePeers:    h.dialogFilterPeersToTL(filter.ExcludePeers, ownerID),
	}
	if filter.Emoticon != "" {
		out.SetEmoticon(filter.Emoticon)
	}
	if filter.Color != nil {
		out.SetColor(*filter.Color)
	}
	out.SetFlags()
	return out
}

func (h *handlers) dialogFilterPeersToTL(peers []store.DialogFilterPeer, ownerID int64) []tg.InputPeerClass {
	out := make([]tg.InputPeerClass, len(peers))
	for i, peer := range peers {
		switch peer.Type {
		case store.PeerTypeChat:
			out[i] = &tg.InputPeerChat{ChatID: peer.ID}
		case store.PeerTypeChannel:
			out[i] = &tg.InputPeerChannel{ChannelID: peer.ID, AccessHash: h.peers.Derive(ownerID, peerhash.KindChannel, peer.ID)}
		case store.PeerTypeUser:
			if peer.ID == ownerID {
				out[i] = &tg.InputPeerSelf{}
			} else {
				out[i] = &tg.InputPeerUser{UserID: peer.ID, AccessHash: h.peers.Derive(ownerID, peerhash.KindUser, peer.ID)}
			}
		}
	}
	return out
}

func validateDialogFilterEntities(title tg.TextWithEntities) ([]store.DialogFilterEntity, error) {
	units := len(utf16.Encode([]rune(title.Text)))
	boundaries := make(map[int]struct{}, units+1)
	boundaries[0] = struct{}{}
	position := 0
	for _, character := range title.Text {
		position += utf16.RuneLen(character)
		boundaries[position] = struct{}{}
	}
	entities := make([]store.DialogFilterEntity, len(title.Entities))
	for i, raw := range title.Entities {
		entity, ok := raw.(*tg.MessageEntityCustomEmoji)
		if !ok || entity.Offset < 0 || entity.Length <= 0 || entity.Length > units || entity.Offset > units-entity.Length || entity.DocumentID <= 0 {
			return nil, errEntityBoundsInvalid
		}
		if _, ok := boundaries[entity.Offset]; !ok {
			return nil, errEntityBoundsInvalid
		}
		if _, ok := boundaries[entity.Offset+entity.Length]; !ok {
			return nil, errEntityBoundsInvalid
		}
		entities[i] = store.DialogFilterEntity{Offset: entity.Offset, Length: entity.Length, DocumentID: entity.DocumentID}
	}
	return entities, nil
}
