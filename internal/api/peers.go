package api

import (
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/store"
)

// peerUserID resolves an input peer to a 1:1 user id, validating the M1
// placeholder access hash (access_hash == user_id). Only user peers are in M4
// scope; anything else is PEER_ID_INVALID.
func peerUserID(peer tg.InputPeerClass) (int64, error) {
	p, ok := peer.(*tg.InputPeerUser)
	if !ok {
		return 0, errPeerIDInvalid
	}
	if p.AccessHash != p.UserID || p.UserID == 0 {
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
// Anything that is none of InputPeerUser, InputPeerChat or InputPeerChannel is
// PEER_ID_INVALID.
func inputPeer(peer tg.InputPeerClass) (store.PeerType, int64, error) {
	if c, ok := peer.(*tg.InputPeerChat); ok {
		if c.ChatID == 0 {
			return 0, 0, errPeerIDInvalid
		}
		return store.PeerTypeChat, c.ChatID, nil
	}
	if c, ok := peer.(*tg.InputPeerChannel); ok {
		// The M1 placeholder scheme, the same shape peerUserID validates for
		// users: access_hash == id. It is NOT an authorization boundary — channel
		// ids are dense and the hash is derivable from the id, so anyone can name
		// any channel here. Admission is the invite hash and the membership row,
		// never this check.
		if c.ChannelID == 0 || c.AccessHash != c.ChannelID {
			return 0, 0, errPeerIDInvalid
		}
		return store.PeerTypeChannel, c.ChannelID, nil
	}
	id, err := peerUserID(peer)
	if err != nil {
		return 0, 0, err
	}
	return store.PeerTypeUser, id, nil
}

// inputUserID resolves an InputUserClass to a user id. InputUserSelf is selfID;
// InputUser is validated against the M1 placeholder access_hash == user_id, the
// same check peerUserID makes. Anything else is PEER_ID_INVALID.
func inputUserID(u tg.InputUserClass, selfID int64) (int64, error) {
	switch v := u.(type) {
	case *tg.InputUserSelf:
		if selfID == 0 {
			return 0, errPeerIDInvalid
		}
		return selfID, nil
	case *tg.InputUser:
		return peerUserID(&tg.InputPeerUser{UserID: v.UserID, AccessHash: v.AccessHash})
	default:
		return 0, errPeerIDInvalid
	}
}
