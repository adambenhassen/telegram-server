package mtproto_test

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"

	"github.com/teagramhq/teagram-server/internal/mtproto"
)

func TestUnpackInvokeWithAfterMsgKeepsBoundSystemLangCodePerConnection(t *testing.T) {
	t.Parallel()

	next := mtproto.HandlerFunc(func(_ *mtproto.Conn, req *mtproto.Request) error {
		id, err := req.Buf.PeekID()
		if err != nil {
			return err
		}
		if id != tg.HelpGetConfigRequestTypeID {
			t.Errorf("inner request id = %#x, want help.getConfig", id)
		}
		return nil
	})
	handler := mtproto.UnpackInvokeWithAfterMsg(next, nil)

	first := mtproto.NewTestConn(&recordingFrameConn{}, crypto.AuthKey{})
	second := mtproto.NewTestConn(&recordingFrameConn{}, crypto.AuthKey{})
	validHints := []struct {
		conn *mtproto.Conn
		hint string
		want string
	}{
		{conn: first, hint: "en-US", want: "en-US"},
		{conn: second, hint: "en_GB", want: "en_GB"},
	}
	var wg sync.WaitGroup
	errs := make(chan error, len(validHints))
	for _, test := range validHints {
		wg.Go(func() {
			var b bin.Buffer
			wrapped := &tg.InvokeWithLayerRequest{
				Layer: tg.Layer,
				Query: &tg.InitConnectionRequest{
					SystemLangCode: test.hint,
					Query:          &tg.HelpGetConfigRequest{},
				},
			}
			if err := wrapped.Encode(&b); err != nil {
				errs <- fmt.Errorf("encode initConnection: %w", err)
				return
			}
			if err := handler.OnMessage(test.conn, &mtproto.Request{Buf: &b}); err != nil {
				errs <- fmt.Errorf("unwrap initConnection: %w", err)
				return
			}
			if got := test.conn.SystemLangCodeHint(); got != test.want {
				errs <- fmt.Errorf("connection hint after %q = %q, want %q", test.hint, got, test.want)
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}

	for _, test := range []struct {
		conn *mtproto.Conn
		hint string
		want string
	}{
		{conn: first, hint: strings.Repeat("a", 33), want: ""},
		{conn: second, hint: "en-US!", want: ""},
		{conn: second, hint: "日本", want: ""},
	} {
		var b bin.Buffer
		wrapped := &tg.InvokeWithLayerRequest{
			Layer: tg.Layer,
			Query: &tg.InitConnectionRequest{
				SystemLangCode: test.hint,
				Query:          &tg.HelpGetConfigRequest{},
			},
		}
		if err := wrapped.Encode(&b); err != nil {
			t.Fatalf("encode initConnection for %q: %v", test.hint, err)
		}
		if err := handler.OnMessage(test.conn, &mtproto.Request{Buf: &b}); err != nil {
			t.Fatalf("unwrap initConnection for %q: %v", test.hint, err)
		}
		if got := test.conn.SystemLangCodeHint(); got != test.want {
			t.Errorf("connection hint after %q = %q, want %q", test.hint, got, test.want)
		}
	}
	if got := mtproto.NewTestConn(&recordingFrameConn{}, crypto.AuthKey{}).SystemLangCodeHint(); got != "" {
		t.Errorf("missing initConnection hint = %q, want empty", got)
	}
}
