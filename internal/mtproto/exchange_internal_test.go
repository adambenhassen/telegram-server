package mtproto

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
)

type exchangeLogCapture struct {
	record  slog.Record
	records int
}

func (*exchangeLogCapture) Enabled(context.Context, slog.Level) bool { return true }
func (s *exchangeLogCapture) Handle(_ context.Context, record slog.Record) error {
	s.record = record.Clone()
	s.records++
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

func TestExchangeConnReturns404ForLookupMiss(t *testing.T) {
	keyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	frame := append([]byte(nil), keyID[:]...)
	handshake := make([]byte, 8)
	conn := &exchangeScriptConn{recv: [][]byte{frame, frame, frame, handshake}}
	var reported [][8]byte
	logSink := &exchangeLogCapture{}
	server := New(exchange.PrivateKey{}, 2, NewMemoryAuthKeyStore(), nil, slog.New(logSink))
	exchange := exchangeConn{
		Conn: conn,
		keys: NewMemoryAuthKeyStore(),
		onLookupMiss: func(id [8]byte) {
			if len(conn.sent) != len(reported)+1 {
				t.Errorf("lookup-miss callback ran after %d replies for %d misses", len(conn.sent), len(reported))
			}
			reported = append(reported, id)
			server.logAuthKeyNotFound(authKeyExchangeLookupMiss, id, netip.MustParseAddr("192.0.2.10"))
		},
	}

	var received bin.Buffer
	if err := exchange.Recv(context.Background(), &received); err != nil {
		t.Fatalf("receive handshake after rejection: %v", err)
	}
	var wantReply bin.Buffer
	wantReply.PutInt32(-404)
	if len(conn.sent) != 3 {
		t.Fatalf("sent replies = %d, want three -404 replies", len(conn.sent))
	}
	for i, sent := range conn.sent {
		if !bytes.Equal(sent, wantReply.Buf) {
			t.Fatalf("reply %d = %v, want -404", i, sent)
		}
	}
	if len(reported) != 3 || reported[0] != keyID || reported[1] != keyID || reported[2] != keyID {
		t.Fatalf("reported auth key IDs = %v, want %x", reported, keyID)
	}
	attrs := make(map[string]any)
	logSink.record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	if logSink.records != 1 {
		t.Fatalf("exchange lookup miss log records = %d, want one sampled line", logSink.records)
	}
	if attrs["reason"] != "exchange_lookup_miss" || attrs["auth_key_id"] != "0102030405060708" || attrs["peer_addr"] != "192.0.2.10" {
		t.Fatalf("exchange diagnostic attrs = %v", attrs)
	}
	if _, ok := attrs["event_time"]; !ok {
		t.Fatal("exchange diagnostic has no event_time")
	}
	if _, ok := attrs["suppressed"]; !ok {
		t.Fatal("exchange diagnostic has no suppressed count")
	}
	server.exchangeLookupMissLog.last.Store(time.Now().Add(-preAuthLogInterval).UnixNano())
	server.logAuthKeyNotFound(authKeyExchangeLookupMiss, keyID, netip.MustParseAddr("192.0.2.10"))
	if logSink.records != 2 {
		t.Fatalf("exchange lookup miss log records after sample window = %d, want two", logSink.records)
	}
	var suppressed int64
	logSink.record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "suppressed" {
			suppressed = attr.Value.Int64()
		}
		return true
	})
	if suppressed != 2 {
		t.Fatalf("exchange lookup miss suppressed count = %d, want 2", suppressed)
	}
	if got := received.Copy(); string(got) != string(handshake) {
		t.Fatalf("received frame = %x, want following handshake %x", got, handshake)
	}
}
