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
)
