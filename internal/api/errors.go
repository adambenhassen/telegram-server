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
)
