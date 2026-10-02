//nolint:testpackage // The test drives the unexported Langpack dispatcher registration.
package api

import (
	"context"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/catalog"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

type langpackBudgetKeyStore struct {
	key crypto.AuthKey
}

func (s *langpackBudgetKeyStore) Save(context.Context, crypto.AuthKey) error { return nil }
func (s *langpackBudgetKeyStore) Touch(context.Context, [8]byte) error       { return nil }
func (s *langpackBudgetKeyStore) Get(context.Context, [8]byte) (crypto.AuthKey, int64, bool, bool, error) {
	return s.key, 0, false, true, nil
}

type langpackBudgetFrameConn struct {
	frames [][]byte
	next   int
	sent   [][]byte
	closed bool
}

func (c *langpackBudgetFrameConn) Recv(_ context.Context, b *bin.Buffer) error {
	if c.next == len(c.frames) {
		return io.EOF
	}
	b.ResetTo(slices.Clone(c.frames[c.next]))
	c.next++
	return nil
}

func (c *langpackBudgetFrameConn) Send(_ context.Context, b *bin.Buffer) error {
	c.sent = append(c.sent, slices.Clone(b.Buf))
	return nil
}

func (c *langpackBudgetFrameConn) Close() error {
	c.closed = true
	return nil
}

// TestLangpackDispatcherBudgetOnServeConn drives every registered anonymous
// Langpack RPC through the real dispatcher and MTProto serve loop. A nil store
// with a prepared snapshot makes successful full-pack reads and the denied
// flood path fail loudly if either starts consulting persistent storage.
func TestLangpackDispatcherBudgetOnServeConn(t *testing.T) {
	t.Parallel()

	key := langpackBudgetTestKey()
	keys := &langpackBudgetKeyStore{key: key}
	h := &handlers{
		store: nil,
		langpack: newLangpackService(&catalog.Snapshot{Packs: []catalog.Pack{{
			Pack:          catalog.PackTDesktop,
			LanguageCode:  catalog.LanguageEnglish,
			Name:          "English",
			NativeName:    "English",
			PluralCode:    "en",
			Version:       1,
			OldestVersion: 1,
			Entries:       []catalog.Entry{{Key: "HELLO", Value: "Hello"}},
			Changes:       []catalog.Change{{Version: 1, Entry: catalog.Entry{Key: "HELLO", Value: "Hello"}}},
		}}}, 2),
	}
	d := mtproto.NewDispatcher()
	registerLangpackMethods(d, h)
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, d, nil)

	calls := []bin.Encoder{
		&tg.LangpackGetLanguagesRequest{LangPack: catalog.PackTDesktop},
		&tg.LangpackGetLangPackRequest{LangPack: catalog.PackTDesktop, LangCode: catalog.LanguageEnglish},
		&tg.LangpackGetStringsRequest{LangPack: catalog.PackTDesktop, LangCode: catalog.LanguageEnglish, Keys: []string{"HELLO"}},
		&tg.LangpackGetDifferenceRequest{LangPack: catalog.PackTDesktop, LangCode: catalog.LanguageEnglish, FromVersion: 0},
		&tg.HelpGetNearestDCRequest{},
	}
	frames := make([][]byte, 300)
	for i := range frames {
		frames[i] = langpackBudgetClientFrame(t, key, int64(i+1)<<32, calls[i%len(calls)])
	}
	conn := &langpackBudgetFrameConn{frames: frames}
	serveErr := srv.ServeConn(context.Background(), conn)
	if serveErr == nil || errors.Is(serveErr, io.EOF) {
		t.Fatalf("ServeConn = %v, want the langpack rejection ceiling", serveErr)
	}
	if conn.next != 256 {
		t.Fatalf("server read %d RPCs, want the 256th call to close this connection", conn.next)
	}
	if !conn.closed {
		t.Fatal("serve loop returned at the langpack ceiling without closing the transport")
	}
	if len(conn.sent) != 256 { // one session-created frame plus 255 RPC replies
		t.Fatalf("server wrote %d frames, want the session-created frame and 255 replies", len(conn.sent))
	}

	replies := langpackBudgetReplies(t, conn.sent, key)
	for call := 1; call <= 64; call++ {
		result := langpackBudgetResult(t, replies, int64(call)<<32)
		var rpcError mt.RPCError
		if err := rpcError.Decode(&bin.Buffer{Buf: result.Result}); err == nil {
			t.Fatalf("call %d returned RPC error %d %q before the budget", call, rpcError.ErrorCode, rpcError.ErrorMessage)
		}
	}
	for i := range calls {
		result := langpackBudgetResult(t, replies, int64(i+1)<<32)
		switch i {
		case 0:
			var got tg.LangPackLanguageVector
			if err := got.Decode(&bin.Buffer{Buf: result.Result}); err != nil || len(got.Elems) != 1 || got.Elems[0].LangCode != catalog.LanguageEnglish {
				t.Fatalf("getLanguages result = (%+v, %v), want the prepared English catalog", got, err)
			}
		case 1:
			var got tg.LangPackDifference
			if err := got.Decode(&bin.Buffer{Buf: result.Result}); err != nil || len(got.Strings) != 1 {
				t.Fatalf("getLangPack result = (%+v, %v), want the full prepared pack", got, err)
			}
		case 2:
			var got tg.LangPackStringClassVector
			if err := got.Decode(&bin.Buffer{Buf: result.Result}); err != nil || len(got.Elems) != 1 {
				t.Fatalf("getStrings result = (%+v, %v), want one prepared string", got, err)
			}
		case 3:
			var got tg.LangPackDifference
			if err := got.Decode(&bin.Buffer{Buf: result.Result}); err != nil || len(got.Strings) != 1 {
				t.Fatalf("getDifference result = (%+v, %v), want one prepared change", got, err)
			}
		case 4:
			var got tg.NearestDC
			if err := got.Decode(&bin.Buffer{Buf: result.Result}); err != nil || got.ThisDC != 2 || got.NearestDC != 2 {
				t.Fatalf("getNearestDC result = (%+v, %v), want configured DC 2", got, err)
			}
		}
	}

	for call := 65; call < 256; call++ {
		flood := langpackBudgetResult(t, replies, int64(call)<<32)
		var rpcError mt.RPCError
		if err := rpcError.Decode(&bin.Buffer{Buf: flood.Result}); err != nil {
			t.Fatalf("decode call %d result: %v", call, err)
		}
		if rpcError.ErrorCode != 420 || rpcError.ErrorMessage != "FLOOD_WAIT_30" {
			t.Fatalf("call %d RPC error = %d %q, want 420 FLOOD_WAIT_30", call, rpcError.ErrorCode, rpcError.ErrorMessage)
		}
	}
	if langpackBudgetHasResult(t, replies, 256<<32) {
		t.Fatal("call 256 received an RPC reply after the connection ceiling")
	}

	// A fresh transport gets its own budget and proves the ceiling closed only
	// the connection that exhausted it.
	other := &langpackBudgetFrameConn{frames: [][]byte{
		langpackBudgetClientFrame(t, key, 1<<32, &tg.LangpackGetLangPackRequest{LangPack: catalog.PackTDesktop, LangCode: catalog.LanguageEnglish}),
	}}
	if err := srv.ServeConn(context.Background(), other); !errors.Is(err, io.EOF) {
		t.Fatalf("second ServeConn = %v, want EOF after its successful request", err)
	}
	otherReplies := langpackBudgetReplies(t, other.sent, key)
	result := langpackBudgetResult(t, otherReplies, 1<<32)
	var full tg.LangPackDifference
	if err := full.Decode(&bin.Buffer{Buf: result.Result}); err != nil || len(full.Strings) != 1 {
		t.Fatalf("second connection full-pack result = (%+v, %v), want one prepared string", full, err)
	}
}

func langpackBudgetTestKey() crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i)
	}
	return raw.WithID()
}

func langpackBudgetClientFrame(t *testing.T, key crypto.AuthKey, msgID int64, body bin.Encoder) []byte {
	t.Helper()
	var b bin.Buffer
	if err := body.Encode(&b); err != nil {
		t.Fatalf("encode client request: %v", err)
	}
	data := crypto.EncryptedMessageData{
		SessionID:              42,
		MessageID:              msgID,
		MessageDataLen:         int32(b.Len()), //nolint:gosec // test requests are small and bounded
		MessageDataWithPadding: b.Copy(),
	}
	if err := crypto.NewClientCipher(crypto.DefaultRand()).Encrypt(key, data, &b); err != nil {
		t.Fatalf("encrypt client request: %v", err)
	}
	return b.Copy()
}

func langpackBudgetReplies(t *testing.T, frames [][]byte, key crypto.AuthKey) [][]byte {
	t.Helper()
	cipher := crypto.NewClientCipher(crypto.DefaultRand())
	replies := make([][]byte, 0, len(frames))
	for _, frame := range frames {
		message := &crypto.EncryptedMessage{}
		if err := message.DecodeWithoutCopy(&bin.Buffer{Buf: frame}); err != nil {
			t.Fatalf("decode server frame: %v", err)
		}
		decrypted, err := cipher.Decrypt(key, message)
		if err != nil {
			t.Fatalf("decrypt server frame: %v", err)
		}
		replies = append(replies, slices.Clone(decrypted.Data()))
	}
	return replies
}

func langpackBudgetResult(t *testing.T, replies [][]byte, requestID int64) *proto.Result {
	t.Helper()
	for _, reply := range replies {
		var result proto.Result
		if err := result.Decode(&bin.Buffer{Buf: reply}); err == nil && result.RequestMessageID == requestID {
			return &result
		}
	}
	t.Fatalf("no RPC result for request %d", requestID)
	return nil
}

func langpackBudgetHasResult(t *testing.T, replies [][]byte, requestID int64) bool {
	t.Helper()
	for _, reply := range replies {
		var result proto.Result
		if err := result.Decode(&bin.Buffer{Buf: reply}); err == nil && result.RequestMessageID == requestID {
			return true
		}
	}
	return false
}
