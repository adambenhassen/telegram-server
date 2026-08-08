package api

import (
	"context"
	"io"
	"log/slog"
	"math/big"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/peerhash"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// Test-only aliases exposing unexported helpers to the external api_test package.
var (
	ValidatePhone  = validatePhone
	VerifyToRPC    = verifyToRPC
	NewSentCode    = newSentCode
	SelfRevocation = selfRevocation
)

// LogIssuedCodeForTest drives the gated login-code log line for the external
// api_test package, without needing a store or a database.
func LogIssuedCodeForTest(log *slog.Logger, logLoginCodes bool, phone, code string) {
	h := &handlers{log: log, logLoginCodes: logLoginCodes}
	h.logIssuedCode(phone, code)
}

// TestMaxFileBytes is the per-file upload cap test handlers run with. It is the
// production default, so MaxFileParts is the same 200 a real server enforces.
const TestMaxFileBytes int64 = 100 << 20

// MaxFileParts exposes the derived part-index bound for the external api_test
// package, for a handler built with TestMaxFileBytes.
func MaxFileParts() int {
	return testHandlers(nil).maxFileParts()
}

func testHandlers(s *store.Store) *handlers {
	return &handlers{
		store:        s,
		log:          slog.New(slog.DiscardHandler),
		maxFileBytes: TestMaxFileBytes,
		downloads:    map[int64]bool{},
		peers:        pgtest.PeerDeriver(),
	}
}

// MaxDownloadChunk exposes the per-reply download cap to the api_test package.
const MaxDownloadChunk = maxDownloadChunk

// GetFileSeqForTest returns a getFile bound to ONE handlers value, so
// successive calls share the in-flight download slot. GetFileForTest builds a
// fresh handler per call and therefore cannot observe a leaked slot.
func GetFileSeqForTest(
	s *store.Store, blobs blob.Store,
) func(int64, *tg.UploadGetFileRequest) (bin.Encoder, error) {
	h := testHandlers(s)
	h.blobs = blobs
	return func(userID int64, req *tg.UploadGetFileRequest) (bin.Encoder, error) {
		var buf bin.Buffer
		if err := req.Encode(&buf); err != nil {
			return nil, err
		}
		return h.handleGetFile(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
	}
}

// DownloadSlotForTest exposes the per-account in-flight download slot, which
// needs no store.
func DownloadSlotForTest() (begin func(int64) bool, end func(int64)) {
	h := testHandlers(nil)
	return h.beginDownload, h.endDownload
}

// GetFileForTest encodes req and invokes handleGetFile for the caller against
// blobs.
func GetFileForTest(
	s *store.Store, userID int64, blobs blob.Store, req *tg.UploadGetFileRequest,
) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	h := testHandlers(s)
	h.blobs = blobs
	return h.handleGetFile(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SaveFilePartForTest encodes req and invokes handleSaveFilePart for the caller.
func SaveFilePartForTest(s *store.Store, userID int64, req *tg.UploadSaveFilePartRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSaveFilePart(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SaveFilePartCappedForTest is SaveFilePartForTest against a handler built with
// maxFileBytes, so a test can reach the per-file and per-user caps without
// uploading a hundred megabytes.
func SaveFilePartCappedForTest(s *store.Store, userID int64, maxFileBytes int64, req *tg.UploadSaveFilePartRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	h := &handlers{store: s, log: slog.New(slog.DiscardHandler), maxFileBytes: maxFileBytes}
	return h.handleSaveFilePart(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SaveBigFilePartForTest encodes req and invokes handleSaveBigFilePart.
func SaveBigFilePartForTest(s *store.Store, userID int64, req *tg.UploadSaveBigFilePartRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSaveBigFilePart(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// BuildUpdatesForTest exposes buildUpdates for the external api_test package.
func BuildUpdatesForTest(s *store.Store, userID int64, fromPts int) ([]tg.UpdateClass, []tg.UserClass, store.State, error) {
	b, err := testHandlers(s).buildUpdates(context.Background(), userID, fromPts)
	return b.ups, b.users, b.state, err
}

// GetStateForTest exposes handleGetState for the external api_test package.
func GetStateForTest(s *store.Store, userID int64) (bin.Encoder, error) {
	return testHandlers(s).handleGetState(&mtproto.Request{Ctx: context.Background(), UserID: userID})
}

// PeerUserID exposes the peer-resolution guard for the external api_test package.
// Uses the test deriver from pgtest.
func PeerUserID(peer tg.InputPeerClass, viewerID int64) (int64, error) {
	return testHandlers(nil).peerUserID(peer, viewerID)
}

// Peer-resolution and wire-mapping helpers, exposed for the external api_test
// package. PeerToTL, ChatToTL and ChannelToTL are pure and need no store.
// InputPeer and InputUserID use the test deriver from pgtest.
var (
	PeerToTL       = peerToTL
	ChatToTL       = chatToTL
	UserStatusToTL = userStatusToTL
)

// UserToTL exposes userToTL for the external api_test package.
// Uses the test deriver from pgtest.
func UserToTL(u store.User, viewerID int64, self bool) *tg.User {
	return testHandlers(nil).userToTL(u, viewerID, self)
}

// ChannelToTL exposes channelToTL for the external api_test package.
// Uses the test deriver from pgtest.
func ChannelToTL(c store.Channel, m store.ChannelMember, member bool, viewerID int64) tg.ChatClass {
	return testHandlers(nil).channelToTL(c, m, member, viewerID)
}

func InputPeer(peer tg.InputPeerClass, viewerID int64) (store.PeerType, int64, error) {
	return testHandlers(nil).inputPeer(peer, viewerID)
}

func InputUserID(u tg.InputUserClass, viewerID int64) (int64, error) {
	return testHandlers(nil).inputUserID(u, viewerID)
}

// DeriveUserHash returns the access_hash viewerID carries for peerID, using the
// test deriver. Tests use it to construct valid input peers.
func DeriveUserHash(viewerID, peerID int64) int64 {
	return pgtest.PeerDeriver().Derive(viewerID, peerhash.KindUser, peerID)
}

// DeriveChannelHash returns the access_hash viewerID carries for channelID,
// using the test deriver. Tests use it to construct valid input peers.
func DeriveChannelHash(viewerID, channelID int64) int64 {
	return pgtest.PeerDeriver().Derive(viewerID, peerhash.KindChannel, channelID)
}

// InputPeerChannel builds a valid InputPeerChannel for channelID as seen by viewerID.
func InputPeerChannel(viewerID, channelID int64) *tg.InputPeerChannel {
	return &tg.InputPeerChannel{ChannelID: channelID, AccessHash: DeriveChannelHash(viewerID, channelID)}
}

// InputChannel builds a valid InputChannel for channelID as seen by viewerID.
func InputChannel(viewerID, channelID int64) *tg.InputChannel {
	return &tg.InputChannel{ChannelID: channelID, AccessHash: DeriveChannelHash(viewerID, channelID)}
}

// InputPeerUser builds a valid InputPeerUser for peerID as seen by viewerID.
func InputPeerUser(viewerID, peerID int64) *tg.InputPeerUser {
	return &tg.InputPeerUser{UserID: peerID, AccessHash: DeriveUserHash(viewerID, peerID)}
}

// InputUser builds a valid InputUser for peerID as seen by viewerID.
func InputUser(viewerID, peerID int64) *tg.InputUser {
	return &tg.InputUser{UserID: peerID, AccessHash: DeriveUserHash(viewerID, peerID)}
}

// MessageToTL maps a media-free row, which is every row a pure mapper test
// builds by hand. Media is hydrated from the store, so it is asserted through
// the read paths instead.
func MessageToTL(m store.Message, createUsers []int64) tg.MessageClass {
	return messageToTL(m, createUsers, nil, nil, nil)
}

// DocumentToTL exposes the file-to-wire mapper for the external api_test package.
func DocumentToTL(dcID int, f store.File) *tg.Document {
	return (&handlers{dcID: dcID}).documentToTL(f)
}

// BuildUpdatesChatsForTest exposes the user and chat lists a batch carries
// alongside its updates, which BuildUpdatesForTest partly omits.
func BuildUpdatesChatsForTest(s *store.Store, userID int64, fromPts int) ([]tg.UpdateClass, []tg.UserClass, []tg.ChatClass, error) {
	b, err := testHandlers(s).buildUpdates(context.Background(), userID, fromPts)
	return b.ups, b.users, b.chats, err
}

// LoadChatsForTest exposes loadChats for the external api_test package.
func LoadChatsForTest(s *store.Store, ids []int64, viewerID int64) ([]tg.ChatClass, error) {
	set := make(map[int64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return testHandlers(s).loadChats(context.Background(), set, viewerID)
}

// LoadChannelsForTest exposes loadChannels for the external api_test package.
func LoadChannelsForTest(s *store.Store, ids []int64, viewerID int64) ([]tg.ChatClass, error) {
	set := make(map[int64]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return testHandlers(s).loadChannels(context.Background(), set, viewerID)
}

// GetChannelMessagesForTest encodes req and invokes handleGetChannelMessages
// for the caller.
func GetChannelMessagesForTest(s *store.Store, userID int64, req *tg.ChannelsGetMessagesRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetChannelMessages(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetDifferenceForTest encodes req and invokes handleGetDifference for the caller.
func GetDifferenceForTest(s *store.Store, userID int64, req *tg.UpdatesGetDifferenceRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetDifference(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetHistoryForTest encodes req and invokes handleGetHistory for the caller.
func GetHistoryForTest(s *store.Store, userID int64, req *tg.MessagesGetHistoryRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetHistory(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetDialogsForTest invokes handleGetDialogs for the caller.
func GetDialogsForTest(s *store.Store, userID int64) (bin.Encoder, error) {
	var buf bin.Buffer
	req := &tg.MessagesGetDialogsRequest{OffsetPeer: &tg.InputPeerEmpty{}}
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetDialogs(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetDialogsPageForTest encodes req and invokes handleGetDialogs for the caller,
// so a test can drive Limit and OffsetID.
func GetDialogsPageForTest(s *store.Store, userID int64, req *tg.MessagesGetDialogsRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleGetDialogs(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ReadHistoryForTest encodes req and invokes handleReadHistory for the caller.
func ReadHistoryForTest(s *store.Store, userID int64, req *tg.MessagesReadHistoryRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleReadHistory(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SetTypingForTest encodes req and invokes handleSetTyping for the caller.
func SetTypingForTest(s *store.Store, userID int64, req *tg.MessagesSetTypingRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSetTyping(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// LogOutForTest invokes handleLogOut for a request arriving on authKeyID,
// handing back the evict announcement unrun so a test can assert whether it was
// already published by the time the handler returned.
func LogOutForTest(s *store.Store, authKeyID [8]byte) (bin.Encoder, func(), error) {
	var buf bin.Buffer
	if err := (&tg.AuthLogOutRequest{}).Encode(&buf); err != nil {
		return nil, nil, err
	}
	return testHandlers(s).handleLogOut(&mtproto.Request{
		Ctx: context.Background(), AuthKeyID: authKeyID, Buf: &buf,
	})
}

// ResetAuthorizationForTest invokes handleResetAuthorization for userID against
// hash, on a request arriving on authKeyID, with the same unrun announcement.
func ResetAuthorizationForTest(s *store.Store, userID int64, authKeyID [8]byte, hash int64) (bin.Encoder, func(), error) {
	var buf bin.Buffer
	if err := (&tg.AccountResetAuthorizationRequest{Hash: hash}).Encode(&buf); err != nil {
		return nil, nil, err
	}
	return testHandlers(s).handleResetAuthorization(&mtproto.Request{
		Ctx: context.Background(), UserID: userID, AuthKeyID: authKeyID, Buf: &buf,
	})
}

// SendMessageForTest encodes req and invokes handleSendMessage for the caller.
func SendMessageForTest(s *store.Store, userID int64, req *tg.MessagesSendMessageRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSendMessage(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SendMediaForTest encodes req and invokes handleSendMedia for the caller,
// against blobs and the account-lifetime storage cap maxUserStorageBytes.
func SendMediaForTest(
	s *store.Store, userID int64, blobs blob.Store, maxUserStorageBytes int64,
	req *tg.MessagesSendMediaRequest,
) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	h := testHandlers(s)
	h.blobs, h.maxUserStorageBytes = blobs, maxUserStorageBytes
	return h.handleSendMedia(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// TestMaxUserStorageBytes is the account-lifetime stored-bytes cap media tests
// run with unless they are reaching for the quota rejection.
const TestMaxUserStorageBytes int64 = 2 << 30

// SanitizeMIME and SanitizeFileName expose the two boundary sanitizers. Both
// are pure and need no store.
var (
	SanitizeMIME     = sanitizeMIME
	SanitizeFileName = sanitizeFileName
)

// NewPartsReaderForTest builds the streaming reader over an in-flight upload's
// parts, for the external api_test package.
func NewPartsReaderForTest(s *store.Store, userID, fileID int64, total int) io.Reader {
	return &partsReader{ctx: context.Background(), store: s, userID: userID, fileID: fileID, total: total}
}

// EditMessageForTest encodes req and invokes handleEditMessage for the caller.
func EditMessageForTest(s *store.Store, userID int64, req *tg.MessagesEditMessageRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleEditMessage(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ResolvePhoneForTest invokes handleResolvePhone for the caller against the given
// request buffer.
func ResolvePhoneForTest(s *store.Store, userID int64, req *tg.ContactsResolvePhoneRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleResolvePhone(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ResolveUsernameForTest invokes handleResolveUsername for the caller against the
// given request.
func ResolveUsernameForTest(s *store.Store, userID int64, req *tg.ContactsResolveUsernameRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleResolveUsername(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// InputEncryptedChat builds a valid InputEncryptedChat for chatID as seen by
// viewerID, using the test deriver.
func InputEncryptedChat(viewerID int64, chatID int32) tg.InputEncryptedChat {
	return tg.InputEncryptedChat{
		ChatID:     int(chatID),
		AccessHash: pgtest.PeerDeriver().Derive(viewerID, peerhash.KindSecret, int64(chatID)),
	}
}

// GetDhConfigForTest encodes req and invokes handleGetDhConfig. It needs no
// store: the group parameters are compiled in.
func GetDhConfigForTest(req *tg.MessagesGetDhConfigRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(nil).handleGetDhConfig(&mtproto.Request{Ctx: context.Background(), Buf: &buf})
}

// RequestEncryptionForTest encodes req and invokes handleRequestEncryption.
func RequestEncryptionForTest(s *store.Store, userID int64, req *tg.MessagesRequestEncryptionRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleRequestEncryption(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// AcceptEncryptionForTest encodes req and invokes handleAcceptEncryption.
func AcceptEncryptionForTest(s *store.Store, userID int64, req *tg.MessagesAcceptEncryptionRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleAcceptEncryption(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// DiscardEncryptionForTest encodes req and invokes handleDiscardEncryption.
func DiscardEncryptionForTest(s *store.Store, userID int64, req *tg.MessagesDiscardEncryptionRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleDiscardEncryption(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// EncryptedChatFor exposes the per-viewer rendering the push path uses, so a
// test can assert what each party is shown without a live socket.
func EncryptedChatFor(chat store.SecretChat, viewerID int64) tg.EncryptedChatClass {
	return testHandlers(nil).encryptedChatFor(chat, viewerID)
}

// SendEncryptedMessageForTest encodes req and invokes handleSendEncryptedMessage.
func SendEncryptedMessageForTest(s *store.Store, userID int64, req *tg.MessagesSendEncryptedRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSendEncryptedMessage(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// DHPrime returns the group modulus, so a test can build g_a values that sit
// inside and outside the accepted range.
func DHPrime() *big.Int { return new(big.Int).Set(dhPrime) }

// MaxDhRandomLength exposes the getDhConfig random_length clamp.
const MaxDhRandomLength = maxDhRandomLength

// DhVersion exposes the served parameter-set version.
const DhVersion = dhVersion

// StaleAcceptErrorForTest exposes the terminal-state mapping acceptEncryption
// applies when its guarded UPDATE matched no row, so a test can pin which error
// each terminal state produces without having to win a race.
func StaleAcceptErrorForTest(s *store.Store, chatID int32) error {
	return testHandlers(s).staleAcceptError(context.Background(), chatID)
}

// UpdateStatusForTest invokes handleUpdateStatus for userID with the given Offline value.
func UpdateStatusForTest(s *store.Store, userID int64, offline bool) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := (&tg.AccountUpdateStatusRequest{Offline: offline}).Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleUpdateStatus(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// UpdateUsernameForTest invokes handleUpdateUsername for userID with the given username.
func UpdateUsernameForTest(s *store.Store, userID int64, username string) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := (&tg.AccountUpdateUsernameRequest{Username: username}).Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleUpdateUsername(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// ClaimChannelUsernameForTest claims a username for a channel atomically
// (both usernames table and channels.username column), so api_test can exercise
// the username resolution path without the RPC that claims channel usernames
// (out of scope for this ticket).
func ClaimChannelUsernameForTest(s *store.Store, channelID int64, handle string) error {
	return s.ClaimChannelUsername(context.Background(), channelID, handle)
}

// ContactsSearchForTest invokes handleContactsSearch for the caller against the
// given request.
func ContactsSearchForTest(s *store.Store, userID int64, req *tg.ContactsSearchRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleContactsSearch(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SearchForTest invokes handleSearch for the caller against the given request.
func SearchForTest(s *store.Store, userID int64, req *tg.MessagesSearchRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleSearch(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// SetUserFirstNameForTest updates a user's first_name directly, so tests can
// seed searchable names. The name_tsv column is GENERATED ALWAYS, so Postgres
// recomputes it automatically. dsn is the database connection string.
func SetUserFirstNameForTest(dsn string, userID int64, firstName string) error {
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		return err
	}
	defer pool.Close()
	_, err = pool.Exec(context.Background(),
		"UPDATE users SET first_name = $1 WHERE id = $2", firstName, userID)
	return err
}
