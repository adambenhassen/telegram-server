package api

import (
	"errors"
	"fmt"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/teagramhq/teagram-server/internal/mtproto"
)

func (h *handlers) handleInvokeAfterMsgRefusal(c *mtproto.Conn, req *mtproto.Request, innerID uint32, reason mtproto.InvokeAfterMsgRefusal, verdict mtproto.UnimplementedVerdict) error {
	mutation := innerID == tg.MessagesUpdateDialogFilterRequestTypeID || innerID == tg.MessagesUpdateDialogFiltersOrderRequestTypeID
	if mutation && req.UserID != 0 && !req.Provisional {
		rateErr := h.checkDialogFilterRateLimit(req)
		h.dialogFilterSync.RequesterRepair(c, req)
		if rateErr != nil {
			if rpc, ok := errors.AsType[*tgerr.Error](rateErr); ok {
				return c.SendErr(req, rpc)
			}
			return c.SendErr(req, errInternal)
		}
	}
	if provisionalBlocked(innerID, req) {
		return c.SendErr(req, errAuthKeyUnreg)
	}
	if reason == mtproto.InvokeAfterMsgWaitFailed {
		return c.SendErr(req, errMsgWaitFailed)
	}
	if verdict == mtproto.UnimplementedClose {
		if suppressed, ok := c.LogUnimplementedClose(); ok {
			h.log.Warn("invokeAfterMsg dependency unavailable",
				"type_id", fmt.Sprintf("%#x", innerID),
				"method", methodName(innerID),
				"error", errMethodNotImplBurst.Error(),
				"suppressed", suppressed,
			)
		}
		return errMethodNotImplBurst
	}
	answer := errMsgWaitTimeout
	if verdict == mtproto.UnimplementedFloodWait {
		answer = errMethodNotImplFlood
	}
	if suppressed, ok := c.LogUnimplemented(methodName(innerID)); ok {
		h.log.Warn("invokeAfterMsg dependency unavailable",
			"type_id", fmt.Sprintf("%#x", innerID),
			"method", methodName(innerID),
			"error_code", answer.Code,
			"error", answer.Message,
			"suppressed", suppressed,
		)
	}
	return c.SendErr(req, answer)
}
