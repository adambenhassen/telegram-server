package api

import (
	"errors"
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

// errMethodNotImplBurst ends a connection that has spent the whole of what one
// may spend on methods this server does not implement. It is deliberately not
// an RPC error: a peer past the ceiling is owed no answer, and writing one is
// the work the bound exists to stop. Returning it takes the connection down
// through the serve loop, which is where a socket is closed — never here, and
// never while the budget is being charged.
var errMethodNotImplBurst = errors.New("unimplemented-method ceiling")

// handleUnknown answers every method with no registered handler, and is the only
// record of what a client asked this server for and did not get. It names the
// method as well as its constructor id and the error the caller received: a
// constructor id alone has to be resolved against the schema by hand before it
// says anything, and the error is what decides whether the client retries,
// degrades, or gives up.
//
// Both of those cost something a caller can ask for as fast as it can write
// frames, and no bound above here counts them: the pre-auth and per-key caps
// bound concurrency rather than rate, and every store-backed rate limit is
// keyed on a method that reaches a registered handler. So the connection's own
// budget is charged first, and it decides which answer this call gets and
// whether the line describing it is emitted at all.
func (h *handlers) handleUnknown(c *mtproto.Conn, req *mtproto.Request) error {
	answer := errMethodNotImpl
	switch c.ChargeUnimplemented() {
	case mtproto.UnimplementedClose:
		return errMethodNotImplBurst
	case mtproto.UnimplementedFloodWait:
		answer = errMethodNotImplFlood
	case mtproto.UnimplementedAnswer:
	}

	id, err := req.Buf.PeekID()
	if err != nil {
		if suppressed, ok := c.LogUnimplemented(); ok {
			h.log.Warn("method not implemented: peek id failed", "err", err, "suppressed", suppressed)
		}
		return answer
	}
	if suppressed, ok := c.LogUnimplemented(); ok {
		h.log.Warn("method not implemented",
			"type_id", fmt.Sprintf("%#x", id),
			"method", methodName(id),
			"error_code", answer.Code,
			"error", answer.Message,
			"suppressed", suppressed,
		)
	}
	return answer
}

// handleUnknownGated is the fallback handler that applies the provisional gate
// before delegating to handleUnknown. Unregistered methods are not in the
// allow-list by definition, so a provisional session gets AUTH_KEY_UNREGISTERED
// instead of INPUT_METHOD_INVALID.
func (h *handlers) handleUnknownGated(c *mtproto.Conn, req *mtproto.Request) error {
	if req.UserID != 0 && req.Provisional {
		return c.SendErr(req, errAuthKeyUnreg)
	}
	return h.handleUnknown(c, req)
}
