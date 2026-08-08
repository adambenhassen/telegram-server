package api

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"math/big"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store"
)

const (
	// dhVersion labels the parameter set getDhConfig serves. p and g are fixed
	// protocol constants, so the version only ever changes if the group does —
	// which would invalidate every in-flight handshake.
	dhVersion = 1
	// maxDhRandomLength bounds the CSPRNG bytes one getDhConfig may ask for.
	// random_length is client input on an authenticated endpoint and is
	// allocated before anything else happens, so an unclamped one is a memory
	// amplifier. 256 is the largest a client has any use for: it matches the
	// group size.
	maxDhRandomLength = 256
	// dhValueLen is the exact byte length of g_a and g_b: the 2048-bit group,
	// left-zero-padded, as every MTProto wire integer in this group is.
	dhValueLen = 256
)

// dhPrime is the group modulus, shared with SRP rather than re-declared: they
// are the same Telegram-canonical 2048-bit safe prime, and two copies could
// drift apart.
var dhPrime = new(big.Int).SetBytes(srp.PBytes())

// validateDHValue rejects a client-supplied g_a or g_b that is not a usable
// group element. It enforces the exact wire length, the mandatory 1 < x < p-1
// bound, and the recommended tighter 2^(2048-64) bound, all through gotd's own
// check so the server accepts exactly what a gotd client will produce.
//
// The tighter bound is the one that matters: a value just above 1 or just below
// p-1 yields a shared secret drawn from a tiny set, so accepting it would let
// either party fix the key the other derives.
func validateDHValue(v []byte) error {
	if len(v) != dhValueLen {
		return errDHValueInvalid
	}
	x := new(big.Int).SetBytes(v)
	// CheckDHParams validates its g_a and g_b arguments identically; passing the
	// one value as both applies the same checks to it once.
	if err := crypto.CheckDHParams(dhPrime, big.NewInt(srp.G), x, x); err != nil {
		return errDHValueInvalid
	}
	return nil
}

// secretChatHash is the access_hash a viewer carries for a secret chat. Chat ids
// come from a dense sequence, so the hash is the only thing standing between a
// client and naming a chat it was never shown.
func (h *handlers) secretChatHash(viewerID int64, chatID int32) int64 {
	return h.peers.Derive(viewerID, peerhash.KindSecret, int64(chatID))
}

// notifyEncryption tells userID that chatID changed state (best-effort). The
// payload carries no key material: the receiving replica reloads the row and
// renders it for that viewer.
func (h *handlers) notifyEncryption(ctx context.Context, userID int64, chatID int32) {
	if err := h.store.Notify(ctx, store.ChannelEncryption, store.EncryptionPayload(userID, int64(chatID))); err != nil {
		h.log.Error("notify encryption", "user_id", userID, "chat_id", chatID, "err", err)
	}
}

// handleGetDhConfig serves messages.getDhConfig: the fixed group parameters plus
// a fresh block of CSPRNG bytes the client mixes into its own entropy.
//
// p and g are compiled-in constants, never generated per request or per deploy:
// a client validates them against the canonical group, and a server-chosen group
// is exactly the position from which the key exchange can be weakened.
func (h *handlers) handleGetDhConfig(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesGetDhConfigRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if req.RandomLength < 0 || req.RandomLength > maxDhRandomLength {
		return nil, errRandomLengthInvalid
	}
	random := make([]byte, req.RandomLength)
	if _, err := rand.Read(random); err != nil {
		h.log.Error("dh config random", "err", err)
		return nil, errInternal
	}
	// A client that already holds this version needs the random bytes only.
	if req.Version == dhVersion {
		return &tg.MessagesDhConfigNotModified{Random: random}, nil
	}
	return &tg.MessagesDhConfig{
		G:       srp.G,
		P:       srp.PBytes(),
		Version: dhVersion,
		Random:  random,
	}, nil
}

// handleRequestEncryption serves messages.requestEncryption: it records the
// initiator's g_a and tells the responder a chat is waiting.
//
// random_id is the client's own dedup token and is deliberately not the chat id,
// despite the TL comment saying it doubles as one. The id is server-allocated:
// letting a client choose it would let one account occupy another's future ids
// and probe which are taken.
func (h *handlers) handleRequestEncryption(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesRequestEncryptionRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	participantID, err := h.inputUserID(req.UserID, r.UserID)
	if err != nil {
		return nil, err
	}
	if participantID == r.UserID {
		return nil, errUserIDInvalid
	}
	if err := validateDHValue(req.GA); err != nil {
		return nil, err
	}
	if _, ok, err := h.store.UserByID(r.Ctx, participantID); err != nil {
		return nil, err
	} else if !ok {
		return nil, errUserIDInvalid
	}

	gAHash := sha256.Sum256(req.GA)
	chat, isDedup, err := h.store.CreateSecretChatRequest(r.Ctx, r.UserID, participantID, req.GA, gAHash[:], int64(req.RandomID))
	if errors.Is(err, store.ErrSecretChatsTooMany) {
		return nil, errPeerFlood
	}
	if err != nil {
		return nil, err
	}
	// Dedup hit: the row already exists, push was already sent. Return the
	// existing waiting state without a second notification.
	if isDedup {
		return &tg.EncryptedChatWaiting{
			ID:            int(chat.ID),
			AccessHash:    h.secretChatHash(r.UserID, chat.ID),
			Date:          int(chat.Date.Unix()),
			AdminID:       chat.AdminID,
			ParticipantID: chat.ParticipantID,
		}, nil
	}

	h.notifyEncryption(r.Ctx, participantID, chat.ID)
	return &tg.EncryptedChatWaiting{
		ID:            int(chat.ID),
		AccessHash:    h.secretChatHash(r.UserID, chat.ID),
		Date:          int(chat.Date.Unix()),
		AdminID:       chat.AdminID,
		ParticipantID: chat.ParticipantID,
	}, nil
}

// handleAcceptEncryption serves messages.acceptEncryption: the responder's g_b
// and key fingerprint complete the exchange.
//
// The row is read first only to tell apart the rejections a client can act on
// differently. The guarded UPDATE is the authoritative transition: it, not the
// read, is what makes a replayed accept fail rather than rewrite the fingerprint
// under a chat the initiator has already keyed.
func (h *handlers) handleAcceptEncryption(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesAcceptEncryptionRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	chatID, err := h.secretChatID(req.Peer, r.UserID)
	if err != nil {
		return nil, err
	}
	if err := validateDHValue(req.GB); err != nil {
		return nil, err
	}

	existing, err := h.store.SecretChatByID(r.Ctx, chatID)
	if errors.Is(err, store.ErrSecretChatNotFound) {
		return nil, errEncryptionIDInvalid
	}
	if err != nil {
		return nil, err
	}
	// Only the responder may accept. The initiator gets the same error as a
	// stranger: a chat it admins is one it already knows about, so nothing is
	// hidden from it, and one error keeps the id space unprobeable.
	if r.UserID != existing.ParticipantID {
		return nil, errEncryptionIDInvalid
	}
	if existing.State == store.SecretChatDiscarded {
		return nil, errEncryptionAlreadyDeclined
	}

	chat, err := h.store.AcceptSecretChat(r.Ctx, chatID, r.UserID, req.GB, req.KeyFingerprint)
	if errors.Is(err, store.ErrSecretChatStale) {
		return nil, h.staleAcceptError(r.Ctx, chatID)
	}
	if err != nil {
		return nil, err
	}

	h.notifyEncryption(r.Ctx, chat.AdminID, chat.ID)
	return &tg.EncryptedChat{
		ID:             int(chat.ID),
		AccessHash:     h.secretChatHash(r.UserID, chat.ID),
		Date:           int(chat.Date.Unix()),
		AdminID:        chat.AdminID,
		ParticipantID:  chat.ParticipantID,
		GAOrB:          chat.GAOrB,
		KeyFingerprint: chat.KeyFingerprint,
	}, nil
}

// staleAcceptError names which terminal state rejected the accept. The guard
// matches no row for either of them, so the state has to be re-read: the
// preflight check above only sees a discard that had already committed when the
// handler started, and a discard landing between that read and the UPDATE would
// otherwise be reported as an already-accepted chat the client could then wait
// forever to key.
//
// It is on the error path only, so the successful accept still costs one
// statement. A reload that fails is returned as itself rather than guessed at.
func (h *handlers) staleAcceptError(ctx context.Context, chatID int32) error {
	chat, err := h.store.SecretChatByID(ctx, chatID)
	if errors.Is(err, store.ErrSecretChatNotFound) {
		// Nothing deletes a secret chat, so this is unreachable today; report it
		// as the naming failure rather than inventing a lifecycle answer.
		return errEncryptionIDInvalid
	}
	if err != nil {
		return err
	}
	if chat.State == store.SecretChatDiscarded {
		return errEncryptionAlreadyDeclined
	}
	return errEncryptionAlreadyAccepted
}

// handleDiscardEncryption serves messages.discardEncryption. Either party may
// discard, from 'requested' or 'active'; 'discarded' is terminal, so a repeat is
// a success that pushes nothing.
//
// chat_id arrives bare, with no access hash to check, so membership is the
// entire authorization boundary — and an absent chat and one the caller is not
// in must report the same error, or a dense id sequence becomes enumerable.
//
// delete_history is accepted and ignored: this milestone stores no encrypted
// messages, so there is no history to delete. It becomes meaningful with
// sendEncryptedMessage (MAIN-139).
func (h *handlers) handleDiscardEncryption(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesDiscardEncryptionRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	existing, err := h.store.SecretChatByID(r.Ctx, int32(req.ChatID)) //nolint:gosec // chat_id is int32 on the wire
	if errors.Is(err, store.ErrSecretChatNotFound) {
		return nil, errEncryptionIDInvalid
	}
	if err != nil {
		return nil, err
	}
	if !existing.Party(r.UserID) {
		return nil, errEncryptionIDInvalid
	}

	discarded := &tg.EncryptedChatDiscarded{ID: int(existing.ID)}
	chat, err := h.store.DiscardSecretChat(r.Ctx, existing.ID)
	if errors.Is(err, store.ErrSecretChatStale) {
		// Already discarded, by this caller or the other party. Idempotent
		// success, and no second push: the other side has been told once.
		return discarded, nil
	}
	if err != nil {
		return nil, err
	}
	h.notifyEncryption(r.Ctx, chat.Other(r.UserID), chat.ID)
	return discarded, nil
}

// maxEncryptedData is the hard size cap on the opaque ciphertext field of
// messages.sendEncryptedMessage. Validated before any DB work. 512 KB matches
// the standard Telegram document-chunk ceiling and is large enough for any
// real secret-chat message while still bounding the in-memory allocation a
// single authenticated request can force.
const maxEncryptedData = 512 * 1024

// handleSendEncryptedMessage serves messages.sendEncryptedMessage (#44fa7a15):
// accept an opaque ciphertext, increment the recipient's qts, store the event,
// and push updateNewEncryptedMessage to the recipient's live connections.
//
// Validation order follows the issue spec exactly. The access_hash check is
// last so that a mismatch and a membership failure both surface as
// CHAT_ID_INVALID, preventing id enumeration via distinguishable errors.
func (h *handlers) handleSendEncryptedMessage(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSendEncryptedRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	// 1. Size cap before any DB work.
	if len(req.Data) > maxEncryptedData {
		return nil, errMessageTooLong
	}
	// 2. Load chat by id (no access_hash yet).
	chatID := int32(req.Peer.ChatID) //nolint:gosec // chat_id is int32 on the wire
	chat, err := h.store.SecretChatByID(r.Ctx, chatID)
	if errors.Is(err, store.ErrSecretChatNotFound) {
		return nil, errEncryptionIDInvalid
	}
	if err != nil {
		return nil, err
	}
	// 3. Chat must be active.
	if chat.State != store.SecretChatActive {
		return nil, errEncryptionDeclined
	}
	// 4. Caller must be a party.
	if !chat.Party(r.UserID) {
		return nil, errChatForbidden
	}
	// 5. Verify access_hash. Do not reveal which check failed.
	if req.Peer.AccessHash != h.secretChatHash(r.UserID, chatID) {
		return nil, errEncryptionIDInvalid
	}
	// 6. Derive recipient (never from the request).
	recipientID := chat.Other(r.UserID)

	event, dup, err := h.store.SendEncryptedMessage(r.Ctx, store.EncryptedSend{
		RecipientID: recipientID,
		ChatID:      chatID,
		RandomID:    req.RandomID,
		Data:        req.Data,
	})
	if err != nil {
		return nil, err
	}
	// Rate limit after dedupe: a retry with an already-stored random_id
	// returns the stored event without consuming a token.
	if !dup {
		if err := h.checkRateLimit(r, "message_send", h.rateLimitMessageSend); err != nil {
			return nil, err
		}
		h.notifyEncryptedMsg(r.Ctx, recipientID, event.Qts)
	}
	return &tg.MessagesSentEncryptedMessage{
		Date: int(event.Date.Unix()),
	}, nil
}

// secretChatID resolves an InputEncryptedChat, validating that access_hash was
// derived for (viewerID, chatID).
func (h *handlers) secretChatID(peer tg.InputEncryptedChat, viewerID int64) (int32, error) {
	if peer.ChatID == 0 {
		return 0, errEncryptionIDInvalid
	}
	id := int32(peer.ChatID) //nolint:gosec // chat_id is int32 on the wire
	if peer.AccessHash != h.secretChatHash(viewerID, id) {
		return 0, errEncryptionIDInvalid
	}
	return id, nil
}

// encryptedChatFor renders a stored chat as the viewer must see it. The two
// parties see different constructors from the same row: only the responder is
// shown encryptedChatRequested, because g_a is disclosed by the request it is
// answering and nothing else needs it.
func (h *handlers) encryptedChatFor(chat store.SecretChat, viewerID int64) tg.EncryptedChatClass {
	hash := h.secretChatHash(viewerID, chat.ID)
	date := int(chat.Date.Unix())
	switch chat.State {
	case store.SecretChatRequested:
		if viewerID != chat.ParticipantID {
			return &tg.EncryptedChatWaiting{
				ID: int(chat.ID), AccessHash: hash, Date: date,
				AdminID: chat.AdminID, ParticipantID: chat.ParticipantID,
			}
		}
		return &tg.EncryptedChatRequested{
			ID: int(chat.ID), AccessHash: hash, Date: date,
			AdminID: chat.AdminID, ParticipantID: chat.ParticipantID,
			GA: chat.GA,
		}
	case store.SecretChatActive:
		return &tg.EncryptedChat{
			ID: int(chat.ID), AccessHash: hash, Date: date,
			AdminID: chat.AdminID, ParticipantID: chat.ParticipantID,
			GAOrB: chat.GAOrB, KeyFingerprint: chat.KeyFingerprint,
		}
	default:
		return &tg.EncryptedChatDiscarded{ID: int(chat.ID)}
	}
}
