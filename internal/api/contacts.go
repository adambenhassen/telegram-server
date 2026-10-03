package api

import (
	"errors"
	"slices"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/telegram/query/hasher"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/store"
)

func (h *handlers) contactMutationError(op string, userID int64, err error) error {
	switch {
	case errors.Is(err, store.ErrInvalidContact):
		return errPeerIDInvalid
	case errors.Is(err, store.ErrContactLimit):
		return errContactsTooMuch
	default:
		h.log.Error(op, "user_id", userID, "err", err)
		return errInternal
	}
}

// handleAddContact serves contacts.addContact. The caller's directed edge is
// the only mutation; the request's phone and custom names are deliberately
// ignored, and the peer is rendered from the global user record.
func (h *handlers) handleAddContact(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsAddContactRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	targetID, err := h.resolveTarget(r.Ctx, req.ID, r.UserID, "add contact target")
	if err != nil {
		return nil, err
	}
	if _, err := h.store.AddContact(r.Ctx, r.UserID, targetID); err != nil {
		return nil, h.contactMutationError("add contact", r.UserID, err)
	}

	target, ok, err := h.store.UserByID(r.Ctx, targetID)
	if err != nil {
		h.log.Error("add contact user", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !ok {
		return nil, errPeerIDInvalid
	}
	wireUsers, err := h.usersToTL(r.Ctx, []store.User{target}, r.UserID, false)
	if err != nil {
		h.log.Error("add contact peer state", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{&tg.UpdatePeerSettings{
			Peer:     &tg.PeerUser{UserID: targetID},
			Settings: tg.PeerSettings{},
		}},
		Users: []tg.UserClass{wireUsers[0]},
		Chats: []tg.ChatClass{},
		Date:  int(time.Now().Unix()),
	}, nil
}

// handleDeleteContacts validates every peer before removing any edge. The
// store then applies the selected removals in one transaction under the
// existing owner lock.
func (h *handlers) handleDeleteContacts(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsDeleteContactsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if len(req.ID) > maxPeerDialogs {
		return nil, errLimitInvalid
	}

	ids := make([]int64, 0, len(req.ID))
	seen := make(map[int64]bool, len(req.ID))
	for _, input := range req.ID {
		targetID, err := h.resolveTarget(r.Ctx, input, r.UserID, "delete contact target")
		if err != nil {
			return nil, err
		}
		if targetID == r.UserID {
			return nil, errPeerIDInvalid
		}
		if !seen[targetID] {
			seen[targetID] = true
			ids = append(ids, targetID)
		}
	}
	if len(ids) == 0 {
		return noUpdates(), nil
	}
	usersByID, err := h.store.UsersByID(r.Ctx, ids)
	if err != nil {
		h.log.Error("delete contacts users", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	for _, id := range ids {
		if _, ok := usersByID[id]; !ok {
			return nil, errPeerIDInvalid
		}
	}
	if _, err := h.store.RemoveContacts(r.Ctx, r.UserID, ids); err != nil {
		return nil, h.contactMutationError("delete contacts", r.UserID, err)
	}
	users := make([]tg.UserClass, len(ids))
	for i, id := range ids {
		users[i] = h.userToTL(usersByID[id], r.UserID, false, store.Contact{})
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{},
		Users:   users,
		Chats:   []tg.ChatClass{},
		Date:    int(time.Now().Unix()),
	}, nil
}

// handleGetContacts returns the caller's directed contact list. Hashes cover
// sorted contact ids only, matching Telegram's pagination hash scheme.
func (h *handlers) handleGetContacts(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsGetContactsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}

	contacts, total, err := h.store.Contacts(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get contacts", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	hash := contactListHash(contacts)
	if req.Hash != 0 && req.Hash == hash {
		return &tg.ContactsContactsNotModified{}, nil
	}

	ids := make([]int64, len(contacts))
	for i, contact := range contacts {
		ids[i] = contact.UserID
	}
	usersByID, err := h.store.UsersByID(r.Ctx, ids)
	if err != nil {
		h.log.Error("get contact users", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	wireContacts := make([]tg.Contact, len(contacts))
	wireUsers := make([]tg.UserClass, len(contacts))
	for i, contact := range contacts {
		user, ok := usersByID[contact.UserID]
		if !ok {
			h.log.Error("get contact user missing", "user_id", r.UserID, "contact_id", contact.UserID)
			return nil, errInternal
		}
		wireContacts[i] = tg.Contact{UserID: contact.UserID, Mutual: contact.Mutual}
		wireUser := h.userToTL(user, r.UserID, false, contact)
		wireUsers[i] = wireUser
	}
	return &tg.ContactsContacts{
		Contacts:   wireContacts,
		SavedCount: total,
		Users:      wireUsers,
	}, nil
}

// handleGetContactIDs returns the caller's contact ids using the same hash as
// contacts.getContacts. This TL method represents an unchanged list as an
// empty int vector.
func (h *handlers) handleGetContactIDs(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.ContactsGetContactIDsRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	contacts, _, err := h.store.Contacts(r.Ctx, r.UserID)
	if err != nil {
		h.log.Error("get contact ids", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if req.Hash != 0 && req.Hash == contactListHash(contacts) {
		return &tg.IntVector{Elems: []int{}}, nil
	}
	ids := make([]int, len(contacts))
	for i, contact := range contacts {
		ids[i] = int(contact.UserID)
	}
	return &tg.IntVector{Elems: ids}, nil
}

func contactListHash(contacts []store.Contact) int64 {
	ids := make([]int64, len(contacts))
	for i, contact := range contacts {
		ids[i] = contact.UserID
	}
	slices.Sort(ids)
	var hash hasher.Hasher
	for _, id := range ids {
		hash.Update(uint32(id)) //nolint:gosec // Telegram's contact hash consumes ids as uint32 values.
	}
	return hash.Sum()
}
