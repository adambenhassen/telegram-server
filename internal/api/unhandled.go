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
		// The burst ends the connection on this call, so no later line on it
		// comes to carry the sampler's pending count. The drop writes it —
		// the serve loop flushes the conn before it closes the socket — and
		// this call is the one it stands for, so it is owed here too.
		if suppressed, ok := c.LogUnimplemented(); ok {
			h.log.Warn("method not implemented",
				"error_code", answer.Code,
				"error", answer.Message,
				"suppressed", suppressed,
			)
		}
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
// to handleUnknown. Unregistered methods are not in the allow-list by
// definition, so a provisional session gets AUTH_KEY_UNREGISTERED instead of
// INPUT_METHOD_INVALID.
//
// The gate only changes the in-band answer, never the charging: it charges
// the connection's shared unimplemented-method budget itself, the same counter
// handleUnknown charges on the non-provisional path, and that counter decides
// the back-off and the close. A second counter here would let a connection
// alternate the two paths and get a fresh allowance per path, which is the
// flood MAIN-350 bounded wearing a different error string.
func (h *handlers) handleUnknownGated(c *mtproto.Conn, req *mtproto.Request) error {
	if req.UserID != 0 && req.Provisional {
		// Inside the budget the provisional answer keeps its precedence over the
		// not-implemented one, so the client re-authenticates rather than giving
		// up. Past the budget the connection is on the non-provisional schedule:
		// the back-off and the close both say what is true about the connection,
		// not about the session, and a provisional session that loops is the same
		// loop a non-provisional one is. Every verdict samples the line the way
		// handleUnknown does, so a burst on this path owes the drop the same
		// count one on the other path does.
		answer := errAuthKeyUnreg
		isClose := false
		switch v := c.ChargeUnimplemented(); v {
		case mtproto.UnimplementedClose:
			isClose = true
		case mtproto.UnimplementedFloodWait:
			answer = errMethodNotImplFlood
		case mtproto.UnimplementedAnswer:
		}
		if suppressed, ok := c.LogUnimplemented(); ok {
			id, _ := req.Buf.PeekID() //nolint:errcheck // dispatcher already validated the id
			h.log.Warn("method not implemented",
				"type_id", fmt.Sprintf("%#x", id),
				"method", methodName(id),
				"error_code", answer.Code,
				"error", answer.Message,
				"suppressed", suppressed,
			)
		}
		if isClose {
			return errMethodNotImplBurst
		}
		return answer
	}
	return h.handleUnknown(c, req)
}
