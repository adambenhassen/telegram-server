package mtproto

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/netip"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
)

type exchangeLogCapture struct{ record slog.Record }

func (*exchangeLogCapture) Enabled(context.Context, slog.Level) bool { return true }
func (s *exchangeLogCapture) Handle(_ context.Context, record slog.Record) error {
	s.record = record.Clone()
	return nil
}
func (s *exchangeLogCapture) WithAttrs([]slog.Attr) slog.Handler { return s }
func (s *exchangeLogCapture) WithGroup(string) slog.Handler      { return s }

type exchangeScriptConn struct {
	recv [][]byte
	sent [][]byte
}

func (c *exchangeScriptConn) Recv(_ context.Context, b *bin.Buffer) error {
	if len(c.recv) == 0 {
		return io.EOF
	}
	b.ResetTo(c.recv[0])
	c.recv = c.recv[1:]
	return nil
}

func (c *exchangeScriptConn) Send(_ context.Context, b *bin.Buffer) error {
	c.sent = append(c.sent, b.Copy())
	return nil
}

func (c *exchangeScriptConn) Close() error { return nil }

func TestExchangeConnRejectsNonzeroAuthKeyDuringExchange(t *testing.T) {
	keyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	frame := append([]byte(nil), keyID[:]...)
	handshake := make([]byte, 8)
	conn := &exchangeScriptConn{recv: [][]byte{frame, handshake}}
	var reported [][8]byte
	logSink := &exchangeLogCapture{}
	server := New(exchange.PrivateKey{}, 2, NewMemoryAuthKeyStore(), nil, slog.New(logSink))
	exchange := exchangeConn{
		Conn: conn,
		onRejected: func(id [8]byte) {
			if len(conn.sent) != 1 {
				t.Errorf("rejection callback ran after %d replies, want one", len(conn.sent))
			}
			reported = append(reported, id)
			server.logAuthKeyNotFound(authKeyExchangeNonzeroID, id, netip.MustParseAddr("192.0.2.10"))
		},
	}

	var received bin.Buffer
	if err := exchange.Recv(context.Background(), &received); err != nil {
		t.Fatalf("receive handshake after rejection: %v", err)
	}
	var wantReply bin.Buffer
	wantReply.PutInt32(-404)
	if len(conn.sent) != 1 || !bytes.Equal(conn.sent[0], wantReply.Buf) {
		t.Fatalf("sent replies = %v, want one -404", conn.sent)
	}
	if len(reported) != 1 || reported[0] != keyID {
		t.Fatalf("reported auth key IDs = %v, want %x", reported, keyID)
	}
	attrs := make(map[string]any)
	logSink.record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	if attrs["reason"] != "exchange_nonzero_id" || attrs["auth_key_id"] != "0102030405060708" || attrs["peer_addr"] != "192.0.2.10" {
		t.Fatalf("exchange diagnostic attrs = %v", attrs)
	}
	if _, ok := attrs["event_time"]; !ok {
		t.Fatal("exchange diagnostic has no event_time")
	}
	if _, ok := attrs["suppressed"]; !ok {
		t.Fatal("exchange diagnostic has no suppressed count")
	}
	if got := received.Copy(); string(got) != string(handshake) {
		t.Fatalf("received frame = %x, want following handshake %x", got, handshake)
	}
}
