package mtproto_test

import (
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// encodeReq encodes obj into a Request buffer positioned at its constructor ID.
func encodeReq(t *testing.T, obj bin.Encoder) *mtproto.Request {
	t.Helper()
	var b bin.Buffer
	if err := obj.Encode(&b); err != nil {
		t.Fatalf("encode: %v", err)
	}
	return &mtproto.Request{Buf: &b}
}

func TestDispatcherUnknownIDUsesFallback(t *testing.T) {
	d := mtproto.NewDispatcher()

	var handledConfig, handledFallback bool
	d.HandleFunc(tg.HelpGetConfigRequestTypeID, func(_ *mtproto.Conn, _ *mtproto.Request) error {
		handledConfig = true
		return nil
	})
	d.Fallback(mtproto.HandlerFunc(func(_ *mtproto.Conn, _ *mtproto.Request) error {
		handledFallback = true
		return nil
	}))

	// Registered ID goes to its handler.
	if err := d.OnMessage(nil, encodeReq(t, &tg.HelpGetConfigRequest{})); err != nil {
		t.Fatalf("OnMessage config: %v", err)
	}
	if !handledConfig || handledFallback {
		t.Fatalf("registered id: config=%v fallback=%v", handledConfig, handledFallback)
	}

	// Unregistered ID goes to the fallback.
	handledConfig, handledFallback = false, false
	if err := d.OnMessage(nil, encodeReq(t, &tg.UsersGetUsersRequest{})); err != nil {
		t.Fatalf("OnMessage users: %v", err)
	}
	if handledConfig || !handledFallback {
		t.Fatalf("unregistered id: config=%v fallback=%v", handledConfig, handledFallback)
	}
}

func TestDispatcherUnknownIDNoFallbackErrors(t *testing.T) {
	d := mtproto.NewDispatcher()
	if err := d.OnMessage(nil, encodeReq(t, &tg.HelpGetConfigRequest{})); err == nil {
		t.Fatal("expected error for unregistered id without fallback")
	}
}

func TestUnpackInvokePeelsWrappers(t *testing.T) {
	// invokeWithLayer -> initConnection -> invokeWithoutUpdates -> help.getConfig
	wrapped := &tg.InvokeWithLayerRequest{
		Layer: 100,
		Query: &tg.InitConnectionRequest{
			APIID:          1,
			DeviceModel:    "test",
			SystemVersion:  "1.0",
			AppVersion:     "1.0",
			SystemLangCode: "en",
			LangPack:       "",
			LangCode:       "en",
			Query: &tg.InvokeWithoutUpdatesRequest{
				Query: &tg.HelpGetConfigRequest{},
			},
		},
	}

	var gotID uint32
	next := mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
		id, err := req.Buf.PeekID()
		if err != nil {
			return err
		}
		gotID = id
		return nil
	})

	if err := mtproto.UnpackInvoke(next).OnMessage(nil, encodeReq(t, wrapped)); err != nil {
		t.Fatalf("UnpackInvoke: %v", err)
	}
	if gotID != tg.HelpGetConfigRequestTypeID {
		t.Fatalf("peeled id = %#x, want %#x", gotID, tg.HelpGetConfigRequestTypeID)
	}
}

func TestUnpackInvokePassesThroughPlainRequest(t *testing.T) {
	var gotID uint32
	next := mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
		id, err := req.Buf.PeekID()
		if err != nil {
			return err
		}
		gotID = id
		return nil
	})

	if err := mtproto.UnpackInvoke(next).OnMessage(nil, encodeReq(t, &tg.HelpGetConfigRequest{})); err != nil {
		t.Fatalf("UnpackInvoke: %v", err)
	}
	if gotID != tg.HelpGetConfigRequestTypeID {
		t.Fatalf("id = %#x, want %#x", gotID, tg.HelpGetConfigRequestTypeID)
	}
}
