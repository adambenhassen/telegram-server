package api

import (
	"context"
	"errors"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

func TestHandleGetPeerDialogsRejectsTruncatedInputBeforeStorage(t *testing.T) {
	var buf bin.Buffer
	buf.PutID(tg.MessagesGetPeerDialogsRequestTypeID)

	_, err := testHandlers(nil).handleGetPeerDialogs(&mtproto.Request{
		Ctx:    context.Background(),
		UserID: 1,
		Buf:    &buf,
	})
	if !errors.Is(err, errMethodNotImpl) {
		t.Fatalf("error = %v, want %v", err, errMethodNotImpl)
	}
}
