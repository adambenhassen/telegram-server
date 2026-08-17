package api

import (
	"errors"
	"log/slog"
	"net/netip"
	"sync"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/srp"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type handlers struct {
	store *store.Store
	// peers derives the per-viewer peer access_hash. Every emission and
	// verification site in this package goes through it. Nothing constructs
	// a peer access hash anywhere else.
	peers *peerhash.Deriver
	cfg   *tg.Config
	dcID  int
	log   *slog.Logger
	srp   *srp.ChallengeStore
	// now reads the server clock. help.getConfig stamps its time fields from it
	// per response, so a long-lived process never serves a config dated at boot.
	now func() time.Time
	// logLoginCodes gates the one log call that carries credential material.
	logLoginCodes bool
	// maxFileBytes is the per-file upload cap the save handlers enforce.
	maxFileBytes int64
	// blobs holds assembled file bodies; the files table holds their metadata.
	blobs blob.Store
	// maxUserStorageBytes is the account-lifetime stored-bytes cap assembly
	// checks before it allocates a file row.
	maxUserStorageBytes int64
	// downloads holds the user ids with a getFile in flight, capped at one each.
	// Bounding concurrency is what makes the download path safe without a tuned
	// rate limit: it composes with maxUserConns (internal/mtproto/sessions.go:18)
	// into a ceiling on how much disk read and egress one account can demand at
	// once, and unlike a rate it needs no load data nobody has measured yet.
	//
	// Lock order: downloadsMu is a leaf. It is taken and released around a map
	// operation and is never held across a store call, a blob read, or a socket
	// write, so it cannot participate in a cycle with any existing lock.
	downloadsMu sync.Mutex
	downloads   map[int64]bool
	// rateLimitMessageSend limits all client-visible message sends (1:1, chat,
	// channel post, media send, forward, encrypted) to one shared budget.
	rateLimitMessageSend store.RateLimitConfig
	// rateLimitCreateChat limits messages.createChat per account.
	rateLimitCreateChat store.RateLimitConfig
	// rateLimitAddChatUser limits messages.addChatUser per account.
	rateLimitAddChatUser store.RateLimitConfig
	// rateLimitCreateChannel limits channels.createChannel per account.
	rateLimitCreateChannel store.RateLimitConfig
	// rateLimitSearchMessages limits messages.search per account.
	rateLimitSearchMessages store.RateLimitConfig
	// rateLimitSearchContacts limits contacts.search per account.
	rateLimitSearchContacts store.RateLimitConfig
	// rateLimitSearchGlobal limits messages.searchGlobal per account. It is a
	// budget of its own rather than a share of the messages.search one: a global
	// search reads every dialog the caller is in, so one call is not the same
	// unit of work as a search inside a named peer.
	rateLimitSearchGlobal store.RateLimitConfig
	// rateLimitSaveFilePart limits upload.saveFilePart and upload.saveBigFilePart
	// to one shared budget per account: both write the same parts table.
	rateLimitSaveFilePart store.RateLimitConfig
	// rateLimitSendCodeIP limits auth.sendCode per client network. It is the
	// one limit here that is not keyed on an account: sendCode is
	// unauthenticated, so the connection's address is the only subject there is.
	rateLimitSendCodeIP store.SendCodeIPLimits
	// rateLimitSignInFailIP limits failed auth.signIn attempts per client
	// network. Keyed on the connection's address, not the identifier.
	rateLimitSignInFailIP store.RateLimitConfig
	// rateLimitCheckPassword limits failed auth.checkPassword attempts per
	// account. Charged only on failed SRP proofs.
	rateLimitCheckPassword store.RateLimitConfig
	// rateLimitCheckPasswordIP limits failed auth.checkPassword attempts per
	// client network. Keyed on the connection's address. Charged only on failures.
	rateLimitCheckPasswordIP store.RateLimitConfig
	// rateLimitGetPasswordIP limits account.getPassword calls per client network,
	// but only for unauthenticated callers (pending state).
	rateLimitGetPasswordIP store.RateLimitConfig
	// rateLimitSignUpIP limits auth.signUp calls per client network. Applied
	// only when TG_REGISTRATION=open.
	rateLimitSignUpIP store.RateLimitConfig
	// rateLimitPasswordProof limits account.getPasswordSettings and
	// account.updatePasswordSettings (proof-required path) per account, on one
	// shared budget. Both call consumeAndVerify against the same secret.
	rateLimitPasswordProof store.RateLimitConfig
	// rateLimitGetPassword limits account.getPassword per account for fully
	// authorized callers (r.UserID != 0 && hasPw). Provisional accounts are
	// not subject to this limit.
	rateLimitGetPassword store.RateLimitConfig
	// registrationMode controls whether auth.signUp is available.
	registrationMode config.RegistrationMode
}

type methodFunc func(req *mtproto.Request) (bin.Encoder, error)

// revokeFunc is a methodFunc that also returns work to run once the reply is on
// the wire. Exactly one revocation needs it: the one whose eviction closes the
// socket the reply goes out on. Nothing orders a Postgres round trip against a
// local socket write, so emitting first would let a successful logOut or
// self-reset surface to the client as a transport error.
//
// Every other revocation emits inside the handler instead, keeping the
// notification published before the client can observe success — see
// selfRevocation.
type revokeFunc func(req *mtproto.Request) (res bin.Encoder, afterReply func(), err error)

// selfRevocation reports whether keyID is the key the request arrived on, which
// is what decides when the eviction may be published.
//
// Deferring it is a cost, not a preference: while the notification waits for the
// reply, a client that has already seen success can trigger an update whose own
// NOTIFY commits first, and a replica then delivers that update to the socket
// being revoked before the evict reaches it. Every live socket still holding the
// revoked key is exposed for that window, on this replica and on every other —
// including one held by whoever the revocation is aimed at, which is the whole
// point of revoking. What makes the delay acceptable is not who is exposed but
// that the ceiling does not move: the delete has already committed, so each of
// those sockets still dies at its next frame or at the read timeout. Evict only
// accelerates that, so a delayed one forfeits the acceleration, never the
// guarantee. Only the request that revokes its own socket pays it.
//
// Publishing first and deferring only this connection's close would need the
// close and the reply write to agree on which of them is in flight. Nothing but
// writeMu can decide that, and the evict path may not take writeMu: it runs on
// the single listener goroutine, where waiting on a write would stall every
// user's delivery.
func selfRevocation(r *mtproto.Request, keyID int64) bool {
	return keyID == mtproto.AuthKeyIDInt64(r.AuthKeyID)
}

// New builds the RPC handler: dispatcher wrapped with UnpackInvoke so
// invokeWithLayer/initConnection wrappers are peeled before dispatch.
//
// peers derives the per-viewer peer access hashes. It is required, and a nil one
// is a programming error rather than a runtime condition, so it stops the server
// at startup instead of surfacing as a nil dereference on the first peer emitted.
func New(s *store.Store, dcID int, cfg *tg.Config, log *slog.Logger, logLoginCodes bool, maxFileBytes int64, blobs blob.Store, maxUserStorageBytes int64, peers *peerhash.Deriver, rateLimits config.RateLimitsConfig, registrationMode config.RegistrationMode) mtproto.Handler {
	if peers == nil {
		panic("api: nil peer hash deriver")
	}
	h := &handlers{
		peers:                    peers,
		store:                    s,
		cfg:                      cfg,
		dcID:                     dcID,
		log:                      log,
		srp:                      srp.NewChallengeStore(srp.DefaultTTL),
		now:                      time.Now,
		logLoginCodes:            logLoginCodes,
		maxFileBytes:             maxFileBytes,
		blobs:                    blobs,
		maxUserStorageBytes:      maxUserStorageBytes,
		downloads:                map[int64]bool{},
		rateLimitMessageSend:     rateLimits.MessageSend,
		rateLimitCreateChat:      rateLimits.CreateChat,
		rateLimitAddChatUser:     rateLimits.AddChatUser,
		rateLimitCreateChannel:   rateLimits.CreateChannel,
		rateLimitSearchMessages:  rateLimits.SearchMessages,
		rateLimitSearchContacts:  rateLimits.SearchContacts,
		rateLimitSearchGlobal:    rateLimits.SearchGlobal,
		rateLimitSaveFilePart:    rateLimits.SaveFilePart,
		rateLimitSendCodeIP:      rateLimits.SendCodeIP,
		rateLimitSignInFailIP:    rateLimits.SignInFailIP,
		rateLimitCheckPassword:   rateLimits.CheckPassword,
		rateLimitCheckPasswordIP: rateLimits.CheckPasswordIP,
		rateLimitGetPasswordIP:   rateLimits.GetPasswordIP,
		rateLimitSignUpIP:        rateLimits.SignUpIP,
		rateLimitPasswordProof:   rateLimits.PasswordProof,
		rateLimitGetPassword:     rateLimits.GetPassword,
		registrationMode:         registrationMode,
	}
	d := mtproto.NewDispatcher()
	register(d, tg.HelpGetConfigRequestTypeID, h.handleGetConfig)
	register(d, tg.AuthSendCodeRequestTypeID, h.handleSendCode)
	register(d, tg.AuthSignInRequestTypeID, h.handleSignIn)
	register(d, tg.AuthSignUpRequestTypeID, h.handleSignUp)
	registerRevoke(d, tg.AuthLogOutRequestTypeID, h.handleLogOut)
	register(d, tg.UsersGetUsersRequestTypeID, h.handleGetUsers)
	register(d, tg.AccountGetAuthorizationsRequestTypeID, h.handleGetAuthorizations)
	registerRevoke(d, tg.AccountResetAuthorizationRequestTypeID, h.handleResetAuthorization)
	register(d, tg.AccountGetPasswordRequestTypeID, h.handleGetPassword)
	register(d, tg.AccountUpdateStatusRequestTypeID, h.handleUpdateStatus)
	register(d, tg.AccountUpdateUsernameRequestTypeID, h.handleUpdateUsername)
	register(d, tg.AuthCheckPasswordRequestTypeID, h.handleCheckPassword)
	register(d, tg.AccountUpdatePasswordSettingsRequestTypeID, h.handleUpdatePasswordSettings)
	register(d, tg.AccountGetPasswordSettingsRequestTypeID, h.handleGetPasswordSettings)
	register(d, tg.UpdatesGetStateRequestTypeID, h.handleGetState)
	register(d, tg.UpdatesGetDifferenceRequestTypeID, h.handleGetDifference)
	register(d, tg.UpdatesGetChannelDifferenceRequestTypeID, h.handleGetChannelDifference)
	register(d, tg.MessagesSendMessageRequestTypeID, h.handleSendMessage)
	register(d, tg.MessagesGetDialogsRequestTypeID, h.handleGetDialogs)
	register(d, tg.MessagesGetHistoryRequestTypeID, h.handleGetHistory)
	register(d, tg.MessagesReadHistoryRequestTypeID, h.handleReadHistory)
	register(d, tg.MessagesEditMessageRequestTypeID, h.handleEditMessage)
	register(d, tg.MessagesDeleteMessagesRequestTypeID, h.handleDeleteMessages)
	register(d, tg.MessagesSetTypingRequestTypeID, h.handleSetTyping)
	register(d, tg.MessagesSendReactionRequestTypeID, h.handleSendReaction)
	register(d, tg.MessagesGetMessagesReactionsRequestTypeID, h.handleGetMessagesReactions)
	register(d, tg.MessagesForwardMessagesRequestTypeID, h.handleForwardMessages)
	register(d, tg.MessagesUpdatePinnedMessageRequestTypeID, h.handleUpdatePinnedMessage)
	register(d, tg.MessagesCreateChatRequestTypeID, h.handleCreateChat)
	register(d, tg.MessagesEditChatTitleRequestTypeID, h.handleEditChatTitle)
	register(d, tg.MessagesAddChatUserRequestTypeID, h.handleAddChatUser)
	register(d, tg.MessagesDeleteChatUserRequestTypeID, h.handleDeleteChatUser)
	register(d, tg.ChannelsGetMessagesRequestTypeID, h.handleGetChannelMessages)
	register(d, tg.MessagesExportChatInviteRequestTypeID, h.handleExportChatInvite)
	register(d, tg.MessagesCheckChatInviteRequestTypeID, h.handleCheckChatInvite)
	register(d, tg.MessagesImportChatInviteRequestTypeID, h.handleImportChatInvite)
	register(d, revokeExportedChatInviteTypeID, h.handleRevokeExportedChatInvite)
	register(d, tg.MessagesSendMediaRequestTypeID, h.handleSendMedia)
	register(d, tg.ChannelsCreateChannelRequestTypeID, h.handleCreateChannel)
	register(d, tg.ChannelsGetChannelsRequestTypeID, h.handleGetChannels)
	register(d, tg.ChannelsJoinChannelRequestTypeID, h.handleJoinChannel)
	register(d, tg.ChannelsLeaveChannelRequestTypeID, h.handleLeaveChannel)
	register(d, tg.ChannelsEditAdminRequestTypeID, h.handleEditAdmin)
	register(d, tg.ChannelsEditBannedRequestTypeID, h.handleEditBanned)
	register(d, tg.ChannelsUpdateUsernameRequestTypeID, h.handleEditChannelUsername)
	register(d, tg.UploadSaveFilePartRequestTypeID, h.handleSaveFilePart)
	register(d, tg.UploadSaveBigFilePartRequestTypeID, h.handleSaveBigFilePart)
	register(d, tg.UploadGetFileRequestTypeID, h.handleGetFile)
	register(d, tg.ContactsResolvePhoneRequestTypeID, h.handleResolvePhone)
	register(d, tg.ContactsResolveUsernameRequestTypeID, h.handleResolveUsername)
	register(d, tg.ContactsSearchRequestTypeID, h.handleContactsSearch)
	register(d, tg.MessagesGetDhConfigRequestTypeID, h.handleGetDhConfig)
	register(d, tg.MessagesRequestEncryptionRequestTypeID, h.handleRequestEncryption)
	register(d, tg.MessagesAcceptEncryptionRequestTypeID, h.handleAcceptEncryption)
	register(d, tg.MessagesDiscardEncryptionRequestTypeID, h.handleDiscardEncryption)
	register(d, tg.MessagesSendEncryptedRequestTypeID, h.handleSendEncryptedMessage)
	register(d, tg.MessagesReceivedQueueRequestTypeID, h.handleReceivedQueue)
	register(d, tg.MessagesSearchRequestTypeID, h.handleSearch)
	register(d, tg.MessagesSearchGlobalRequestTypeID, h.handleSearchGlobal)
	d.Fallback(mtproto.HandlerFunc(h.handleUnknownGated))
	return mtproto.UnpackInvoke(d)
}

// checkRateLimit checks the per-account rate limit for the given surface.
// Returns nil when allowed, or a FLOOD_WAIT error when denied.
func (h *handlers) checkRateLimit(r *mtproto.Request, surface string, cfg store.RateLimitConfig) error {
	result, err := h.store.CheckRateLimit(r.Ctx, r.UserID, surface, cfg)
	if err != nil {
		h.log.Error("rate limit check", "user_id", r.UserID, "surface", surface, "err", err)
		return errInternal
	}
	if result != nil {
		return FloodWaitError(int(result.Wait / time.Second))
	}
	return nil
}

// checkAndChargeRateLimitIP atomically checks and charges the per-IP rate limit
// for the given surface. Single atomic INSERT-ON-CONFLICT, so concurrent
// bursts cannot all pass before any charge. Returns nil when allowed, or a
// FLOOD_WAIT error when denied.
func (h *handlers) checkAndChargeRateLimitIP(r *mtproto.Request, surface string, cfg store.RateLimitConfig) error {
	if !cfg.Enabled() {
		return nil
	}
	key, ok := store.IPBucketKey(r.ClientAddr)
	if !ok {
		return FloodWaitError(int(cfg.Window / time.Second))
	}
	subjectID, err := keyToSubjectID(key)
	if err != nil {
		h.log.Error("rate limit: convert IP to subject", "err", err)
		return errInternal
	}
	result, err := h.store.CheckRateLimit(r.Ctx, subjectID, surface, cfg)
	if err != nil {
		h.log.Error("rate limit: IP check-and-charge", "err", err)
		return errInternal
	}
	if result != nil {
		return FloodWaitError(int(result.Wait / time.Second))
	}
	return nil
}

// reserveRateLimitIP atomically reserves a token for the per-IP rate limit.
// Returns a reservation on success, a denial result on rejection, or an error
// on storage failure. Used by checkPassword for the reserve-then-refund pattern.
func (h *handlers) reserveRateLimitIP(r *mtproto.Request, surface string, cfg store.RateLimitConfig) (*store.RateLimitReservation, *store.RateLimitResult, error) {
	if !cfg.Enabled() {
		return nil, nil, nil
	}
	key, ok := store.IPBucketKey(r.ClientAddr)
	if !ok {
		return nil, nil, FloodWaitError(int(cfg.Window / time.Second))
	}
	subjectID, err := keyToSubjectID(key)
	if err != nil {
		h.log.Error("rate limit: convert IP to subject", "err", err)
		return nil, nil, errInternal
	}
	return h.store.ReserveRateLimit(r.Ctx, subjectID, surface, cfg)
}

// refundRateLimitIP refunds a previously reserved per-IP rate-limit token.
// No-op when the reservation is nil.
func (h *handlers) refundRateLimitIP(r *mtproto.Request, surface string, res *store.RateLimitReservation) error {
	if res == nil {
		return nil
	}
	key, ok := store.IPBucketKey(r.ClientAddr)
	if !ok {
		return nil
	}
	subjectID, err := keyToSubjectID(key)
	if err != nil {
		h.log.Error("rate limit: convert IP to subject", "err", err)
		return err
	}
	return h.store.RefundRateLimit(r.Ctx, subjectID, surface, res)
}

// provisionalAllowList holds the method IDs that a provisional session may call.
// A provisional session is a username-mode account with no verifier: it can set
// its password, check password state, or log out — but nothing else.
var provisionalAllowList = map[uint32]bool{
	tg.HelpGetConfigRequestTypeID:                 true,
	tg.AccountGetPasswordRequestTypeID:            true,
	tg.AccountUpdatePasswordSettingsRequestTypeID: true,
	tg.AuthLogOutRequestTypeID:                    true,
}

// provisionalBlocked reports whether req hits the provisional gate for a
// registered method with the given TL constructor id. It is called only by
// registerRevoke; the handleUnknownGated fallback uses its own inline check
// (req.UserID != 0 && req.Provisional).
func provisionalBlocked(id uint32, req *mtproto.Request) bool {
	return req.UserID != 0 && req.Provisional && !provisionalAllowList[id]
}

// keyToSubjectID derives a deterministic int64 subject ID from an IP bucket
// prefix, so per-IP rate limits can use the same CheckRateLimit surface as
// per-account limits.
func keyToSubjectID(key netip.Prefix) (int64, error) {
	h := fnv1a64(key.String())
	return int64(h), nil //nolint:gosec // only used as a rate-limit subject ID
}

// fnv1a64 computes the FNV-1a 64-bit hash of s.
func fnv1a64(s string) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := range len(s) {
		h ^= uint64(s[i])
		h *= prime64
	}
	return h
}

func register(d *mtproto.Dispatcher, id uint32, fn methodFunc) {
	registerRevoke(d, id, func(req *mtproto.Request) (bin.Encoder, func(), error) {
		res, err := fn(req)
		return res, nil, err
	})
}

// registerRevoke registers fn and runs its afterReply hook once the reply write
// has been attempted, whether or not that write succeeded: the revocation it
// announces has already committed, so it must propagate either way.
// The provisional gate is applied here so it covers both register and
// registerRevoke callers (including auth.logOut and account.resetAuthorization).
func registerRevoke(d *mtproto.Dispatcher, id uint32, fn revokeFunc) {
	d.HandleFunc(id, func(c *mtproto.Conn, req *mtproto.Request) error {
		// Provisional gate: blocks all authorized RPCs except the allow-list.
		// Does not apply when UserID == 0 (unauthenticated keys already
		// handled per-method).
		if provisionalBlocked(id, req) {
			return c.SendErr(req, errAuthKeyUnreg)
		}
		res, afterReply, err := fn(req)
		if err != nil {
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) {
				rpc = errInternal
			}
			return c.SendErr(req, rpc)
		}
		sendErr := c.SendResult(req, res)
		if afterReply != nil {
			afterReply()
		}
		return sendErr
	})
}
