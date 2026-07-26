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
	// errUsersTooMuch rejects a chat that would exceed the participant limit.
	errUsersTooMuch = rpcErr(400, "USERS_TOO_MUCH")
)
