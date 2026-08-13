package api

import (
	"fmt"
	"sync"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// typeNames resolves TL constructor ids to schema names, built once. tg.TypesMap
// allocates the whole registry on every call, which is thousands of entries, so
// it must not run per request.
var typeNames = sync.OnceValue(tg.TypesMap)

// methodName is the schema name of a constructor id, or "unknown" when the
// pinned layer has none. An id with no name is a finding rather than a gap in
// the log: it means the caller is on a different layer than the one this server
// is built against, or is calling something added after the pin.
func methodName(id uint32) string {
	if name, ok := typeNames()[id]; ok {
		return name
	}
	return "unknown"
}

// handleUnknown answers every method with no registered handler, and is the only
// record of what a client asked this server for and did not get. It names the
// method as well as its constructor id and the error the caller received: a
// constructor id alone has to be resolved against the schema by hand before it
// says anything, and the error is what decides whether the client retries,
// degrades, or gives up.
func (h *handlers) handleUnknown(_ *mtproto.Conn, req *mtproto.Request) error {
	id, err := req.Buf.PeekID()
	if err != nil {
		h.log.Warn("method not implemented: peek id failed", "err", err)
		return errMethodNotImpl
	}
	h.log.Warn("method not implemented",
		"type_id", fmt.Sprintf("%#x", id),
		"method", methodName(id),
		"error_code", errMethodNotImpl.Code,
		"error", errMethodNotImpl.Message,
	)
	return errMethodNotImpl
}

// handleUnknownGated is the fallback handler that applies the provisional gate
// before delegating to handleUnknown. Unregistered methods are not in the
// allow-list by definition, so a provisional session gets AUTH_KEY_UNREGISTERED
// instead of INPUT_METHOD_INVALID.
func (h *handlers) handleUnknownGated(c *mtproto.Conn, req *mtproto.Request) error {
	id, err := req.Buf.PeekID()
	if err == nil && provisionalBlocked(id, req) {
		return c.SendErr(req, errAuthKeyUnreg)
	}
	return h.handleUnknown(c, req)
}

// ProvisionalBlocked exposes the gate predicate for tests. It is the same
// function called by both registerRevoke and handleUnknownGated, not a copy.
var ProvisionalBlocked = provisionalBlocked
