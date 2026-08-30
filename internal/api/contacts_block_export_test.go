package api

import (
	"context"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// BlockForTest encodes req and invokes handleContactsBlock for the caller.
func BlockForTest(s *store.Store, userID int64, req *tg.ContactsBlockRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleContactsBlock(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// UnblockForTest encodes req and invokes handleContactsUnblock for the caller.
func UnblockForTest(s *store.Store, userID int64, req *tg.ContactsUnblockRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleContactsUnblock(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}

// GetBlockedForTest encodes req and invokes handleContactsGetBlocked for the caller.
func GetBlockedForTest(s *store.Store, userID int64, req *tg.ContactsGetBlockedRequest) (bin.Encoder, error) {
	var buf bin.Buffer
	if err := req.Encode(&buf); err != nil {
		return nil, err
	}
	return testHandlers(s).handleContactsGetBlocked(&mtproto.Request{Ctx: context.Background(), UserID: userID, Buf: &buf})
}
