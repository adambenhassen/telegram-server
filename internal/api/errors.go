// Package api implements the MTProto RPC method handlers.
package api

import "github.com/gotd/td/tgerr"

func rpcErr(code int, msg string) *tgerr.Error {
	return tgerr.New(code, msg)
}

var (
	errPhoneInvalid  = rpcErr(400, "PHONE_NUMBER_INVALID")
	errCodeInvalid   = rpcErr(400, "PHONE_CODE_INVALID")
	errCodeExpired   = rpcErr(400, "PHONE_CODE_EXPIRED")
	errInternal      = rpcErr(500, "INTERNAL")
	errMethodNotImpl = rpcErr(400, "INPUT_METHOD_INVALID")
	errAuthKeyUnreg  = rpcErr(401, "AUTH_KEY_UNREGISTERED")
	// errHashInvalid rejects account.resetAuthorization for a session hash that is
	// not one of the caller's own auth keys, so a user cannot revoke another's.
	errHashInvalid = rpcErr(400, "HASH_INVALID")
	// errFloodWait rate-limits code resends. Telegram signals resend backoff
	// with FLOOD_WAIT_<seconds>; 60 matches the store's resendCooldown.
	errFloodWait = rpcErr(420, "FLOOD_WAIT_60")
	// errSessionPasswordNeeded is returned by signIn when the account has 2FA:
	// the client must complete the SRP password step via checkPassword.
	errSessionPasswordNeeded = rpcErr(401, "SESSION_PASSWORD_NEEDED")
	// errPasswordHashInvalid rejects a bad SRP proof (wrong password) on
	// checkPassword, updatePasswordSettings, and getPasswordSettings.
	errPasswordHashInvalid = rpcErr(400, "PASSWORD_HASH_INVALID")
	// errSRPIDInvalid rejects an unknown, expired, or already-consumed SRP
	// challenge id.
	errSRPIDInvalid = rpcErr(400, "SRP_ID_INVALID")
	// errNewSaltInvalid rejects malformed salts in a new-password set/change.
	errNewSaltInvalid = rpcErr(400, "NEW_SALT_INVALID")
	// errNewPasswordBad rejects a missing/malformed new verifier on set/change.
	errNewPasswordBad = rpcErr(400, "NEW_PASSWORD_BAD")
	// errMessageIDInvalid rejects edit/delete of an absent or non-owned message.
	errMessageIDInvalid = rpcErr(400, "MESSAGE_ID_INVALID")
	// errPeerIDInvalid rejects an unresolvable or unauthorized input peer.
	errPeerIDInvalid = rpcErr(400, "PEER_ID_INVALID")
	// errChatTitleInvalid rejects an empty, whitespace-only or over-length chat
	// title on createChat and editChatTitle.
	errChatTitleInvalid = rpcErr(400, "CHAT_TITLE_EMPTY")
	// errMessageEmpty rejects message text the server cannot store — a NUL byte or
	// an invalid UTF-8 sequence.
	errMessageEmpty = rpcErr(400, "MESSAGE_EMPTY")
	// errUsersTooMuch rejects a chat that would exceed the participant limit.
	errUsersTooMuch = rpcErr(400, "USERS_TOO_MUCH")
	// errChannelsTooMuch rejects a join that would exceed the per-account
	// channel cap. Distinct from USERS_TOO_MUCH because both are only reachable
	// with a hash the caller already holds.
	errChannelsTooMuch = rpcErr(400, "CHANNELS_TOO_MUCH")
	// errFilePartInvalid rejects a part index that is negative or past the
	// per-file maximum, and a zero file id.
	errFilePartInvalid = rpcErr(400, "FILE_PART_INVALID")
	// errFilePartTooBig rejects a part over the 512 KiB protocol maximum, and a
	// file whose parts would exceed TG_MAX_FILE_BYTES.
	errFilePartTooBig = rpcErr(400, "FILE_PART_TOO_BIG")
	// errStorageQuota rejects an upload that would take the account past its
	// outstanding-bytes cap. FLOOD_WAIT is the closest Telegram signal: the
	// condition clears on its own once the account assembles or its parts expire.
	errStorageQuota = rpcErr(420, "FLOOD_WAIT_60")
	// errMediaInvalid rejects a media type M5 does not serve, and an input file
	// whose parts are missing or inconsistent.
	errMediaInvalid = rpcErr(400, "MEDIA_INVALID")
	// errFileQuota rejects an upload that would take the account past its total
	// stored-bytes cap.
	errFileQuota = rpcErr(400, "STORAGE_CHECK_FAILED")
	// errLocationInvalid rejects every upload.getFile the server will not serve:
	// an unknown file id, a wrong access hash, a file whose bytes were never
	// stored, a caller who owns no live message referencing it, a location type
	// M5 does not serve, and a range outside the file. They are ONE error on
	// purpose. files.id is dense BIGSERIAL, so two distinguishable errors would
	// turn the download path into an existence-and-enumeration oracle over every
	// file on the server.
	errLocationInvalid = rpcErr(400, "LOCATION_INVALID")
	// errBannedRightsInvalid rejects a channels.editBanned rights struct that
	// revokes something other than view_messages. M7 stores no partial
	// restriction, so there is nothing to write for one, and treating it as an
	// unban would clear a live ban for a caller who asked to tighten it. Decided
	// on the client's own input before any row is read, like errUntilDateInvalid.
	errBannedRightsInvalid = rpcErr(400, "BANNED_RIGHTS_INVALID")
	// errUntilDateInvalid rejects a channels.editBanned whose until_date has
	// already passed. It is decided entirely on the client's own input, before
	// any channel or participant is read, so it tells a caller nothing about the
	// channel and may be distinct from errPeerIDInvalid.
	errUntilDateInvalid = rpcErr(400, "UNTIL_DATE_INVALID")
	// errDownloadBusy rejects a second concurrent download from one account.
	// FLOOD_WAIT is the right signal: the condition clears on its own as soon as
	// the in-flight request finishes.
	errDownloadBusy = rpcErr(420, "FLOOD_WAIT_1")
	// errLookupFloodWait rejects a contacts.resolvePhone that would take the
	// caller past their per-account lookup quota.
	errLookupFloodWait = rpcErr(420, "FLOOD_WAIT_86400")
	// errRandomLengthInvalid rejects a getDhConfig random_length outside
	// [0, maxDhRandomLength], before any allocation is made for it.
	errRandomLengthInvalid = rpcErr(400, "RANDOM_LENGTH_INVALID")
	// errDHValueInvalid rejects a g_a or g_b that is the wrong length or outside
	// the safe range for the group. Telegram signals both as DH_G_A_INVALID, on
	// requestEncryption and acceptEncryption alike.
	errDHValueInvalid = rpcErr(400, "DH_G_A_INVALID")
	// errEncryptionIDInvalid is every rejection that depends on naming a secret
	// chat: an id with no row, an access hash not derived for the caller, a chat
	// the caller is not a party to, and an accept attempted by the initiator.
	// They are ONE error on purpose — secret chat ids are a dense sequence, so a
	// distinguishable set would make the id space enumerable.
	errEncryptionIDInvalid = rpcErr(400, "ENCRYPTION_ID_INVALID")
	// errEncryptionAlreadyAccepted rejects a replayed acceptEncryption. The
	// caller is a party to the chat and already knows it exists, so unlike the
	// rejections above this one may be distinct.
	errEncryptionAlreadyAccepted = rpcErr(400, "ENCRYPTION_ALREADY_ACCEPTED")
	// errEncryptionAlreadyDeclined rejects an accept of a discarded chat.
	errEncryptionAlreadyDeclined = rpcErr(400, "ENCRYPTION_ALREADY_DECLINED")
	// errUserIDInvalid rejects a requestEncryption whose target is the caller
	// themselves, and one whose target has no account.
	errUserIDInvalid = rpcErr(400, "USER_ID_INVALID")
	// errPeerFlood rejects a requestEncryption that would take the caller past
	// their outstanding-request cap. The condition clears as the responder
	// answers or the caller discards, so it is a flood signal, not a hard limit.
	errPeerFlood = rpcErr(400, "PEER_FLOOD")
	// errPhoneNotOccupied is the byte-identical response for a phone lookup that
	// finds no account and any target-side refusal — indistinguishable by design.
	errPhoneNotOccupied = rpcErr(400, "PHONE_NOT_OCCUPIED")
	// errMessageTooLong rejects an encrypted payload or a search query over the
	// server-side size cap.
	errMessageTooLong = rpcErr(400, "MESSAGE_TOO_LONG")
	// errEncryptionDeclined rejects a send to a secret chat that is not active —
	// either still in the 'requested' state or already 'discarded'.
	errEncryptionDeclined = rpcErr(400, "ENCRYPTION_DECLINED")
	// errChatForbidden rejects a send from an account that is not a party to the
	// named secret chat.
	errChatForbidden = rpcErr(403, "CHAT_FORBIDDEN")
	// errChatAdminRequired rejects a pin/unpin by a non-admin in a group chat or
	// channel.
	errChatAdminRequired = rpcErr(400, "CHAT_ADMIN_REQUIRED")
	// errUsernameInvalid rejects a username that fails validation: wrong length,
	// invalid characters, digit/underscore leading, or a reserved handle.
	errUsernameInvalid = rpcErr(400, "USERNAME_INVALID")
	// errUsernameOccupied rejects a username already claimed by another account.
	errUsernameOccupied = rpcErr(400, "USERNAME_OCCUPIED")
	// errUsernameFloodWait rejects a username change that would exceed the
	// per-account rate limit.
	errUsernameFloodWait = rpcErr(420, "FLOOD_WAIT_86400")
	// errUsernameNotOccupied is the response for a username lookup that finds
	// no account — indistinguishable from a private channel or a handle that
	// was cleared, by the non-oracle invariant.
	errUsernameNotOccupied = rpcErr(400, "USERNAME_NOT_OCCUPIED")
	// errUsernameLookupFloodWait rejects a contacts.resolveUsername that would
	// take the caller past their per-account username lookup quota.
	errUsernameLookupFloodWait = rpcErr(420, "FLOOD_WAIT_86400")
	// errSearchQueryEmpty rejects messages.search with an empty query string.
	errSearchQueryEmpty = rpcErr(400, "SEARCH_QUERY_EMPTY")
	// errInputFilterInvalid rejects messages.search with an unsupported filter type.
	errInputFilterInvalid = rpcErr(400, "INPUT_FILTER_INVALID")
)
