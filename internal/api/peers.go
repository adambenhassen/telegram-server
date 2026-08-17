package api

import (
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// peerUserID resolves an input peer to a 1:1 user id, validating that
// access_hash was derived for (viewerID, peer). Only user peers are in scope;
// anything else is PEER_ID_INVALID.
func (h *handlers) peerUserID(peer tg.InputPeerClass, viewerID int64) (int64, error) {
	p, ok := peer.(*tg.InputPeerUser)
	if !ok {
		return 0, errPeerIDInvalid
	}
	if p.UserID == 0 {
		return 0, errPeerIDInvalid
	}
	wantHash := h.peers.Derive(viewerID, peerhash.KindUser, p.UserID)
	if p.AccessHash != wantHash {
		return 0, errPeerIDInvalid
	}
	return p.UserID, nil
}

// peerToTL names a stored peer on the wire. peer_id alone is ambiguous — chat
// ids and user ids come from different sequences — so peer_type decides.
func peerToTL(peerType store.PeerType, peerID int64) tg.PeerClass {
	switch peerType {
	case store.PeerTypeChat:
		return &tg.PeerChat{ChatID: peerID}
	case store.PeerTypeChannel:
		return &tg.PeerChannel{ChannelID: peerID}
	}
	return &tg.PeerUser{UserID: peerID}
}

// inputPeer classifies a client-supplied input peer. InputPeerChat carries no
// access hash at all, so this only decodes the id: membership in the chat is the
// entire authorization boundary and every caller MUST check it separately.
// InputPeerChannel validates that access_hash was derived for (viewerID, channelID).
// Anything that is none of InputPeerUser, InputPeerChat or InputPeerChannel is
// PEER_ID_INVALID.
func (h *handlers) inputPeer(peer tg.InputPeerClass, viewerID int64) (store.PeerType, int64, error) {
	if c, ok := peer.(*tg.InputPeerChat); ok {
		if c.ChatID == 0 {
			return 0, 0, errPeerIDInvalid
		}
		return store.PeerTypeChat, c.ChatID, nil
	}
	if c, ok := peer.(*tg.InputPeerChannel); ok {
		// Channel access_hash must be derived for (viewerID, channelID). M1
		// placeholder (access_hash == id) is no longer accepted.
		if c.ChannelID == 0 {
			return 0, 0, errPeerIDInvalid
		}
		wantHash := h.peers.Derive(viewerID, peerhash.KindChannel, c.ChannelID)
		if c.AccessHash != wantHash {
			return 0, 0, errPeerIDInvalid
		}
		return store.PeerTypeChannel, c.ChannelID, nil
	}
	id, err := h.peerUserID(peer, viewerID)
	if err != nil {
		return 0, 0, err
	}
	return store.PeerTypeUser, id, nil
}

// replyPeerIsDest reports whether p names the same peer as (destType, destID).
// selfID is the caller's user id, used to resolve InputPeerSelf. No database
// access is performed, so a foreign peer's existence cannot be probed.
//
// InputPeerUserFromMessage and InputPeerChannelFromMessage always return false:
// they carry a message reference, not just an id, so they cannot be compared
// structurally to the destination. This is the only reading criterion 4 permits
// — accepting them as a destination match would require a message lookup.
func replyPeerIsDest(p tg.InputPeerClass, destType store.PeerType, destID, selfID int64) bool {
	switch v := p.(type) {
	case *tg.InputPeerEmpty:
		return true
	case *tg.InputPeerSelf:
		return destType == store.PeerTypeUser && destID == selfID
	case *tg.InputPeerUser:
		return destType == store.PeerTypeUser && destID == v.UserID
	case *tg.InputPeerChat:
		return destType == store.PeerTypeChat && destID == v.ChatID
	case *tg.InputPeerChannel:
		return destType == store.PeerTypeChannel && destID == v.ChannelID
	default:
		return false
	}
}

// inputUserID resolves an InputUserClass to a user id. InputUserSelf is selfID;
// InputUser is validated against the derived access hash for (viewerID, peer).
// Anything else is PEER_ID_INVALID.
func (h *handlers) inputUserID(u tg.InputUserClass, viewerID int64) (int64, error) {
	switch v := u.(type) {
	case *tg.InputUserSelf:
		if viewerID == 0 {
			return 0, errPeerIDInvalid
		}
		return viewerID, nil
	case *tg.InputUser:
		return h.peerUserID(&tg.InputPeerUser{UserID: v.UserID, AccessHash: v.AccessHash}, viewerID)
	default:
		return 0, errPeerIDInvalid
	}
}
