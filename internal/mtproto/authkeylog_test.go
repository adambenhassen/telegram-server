package mtproto_test

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

type authKeyLogRecord struct {
	message string
	attrs   map[string]any
}

type authKeyLogSink struct {
	mu      sync.Mutex
	records []authKeyLogRecord
	changed chan struct{}
	attrs   []slog.Attr
}

func newAuthKeyLogSink() *authKeyLogSink {
	return &authKeyLogSink{changed: make(chan struct{}, 1)}
}

func (s *authKeyLogSink) Enabled(context.Context, slog.Level) bool { return true }

func (s *authKeyLogSink) Handle(_ context.Context, record slog.Record) error {
	attrs := make(map[string]any)
	s.mu.Lock()
	for _, attr := range s.attrs {
		attrs[attr.Key] = attr.Value.Any()
	}
	record.Attrs(func(attr slog.Attr) bool {
		attrs[attr.Key] = attr.Value.Any()
		return true
	})
	s.records = append(s.records, authKeyLogRecord{message: record.Message, attrs: attrs})
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
	return nil
}

func (s *authKeyLogSink) WithAttrs(attrs []slog.Attr) slog.Handler {
	s.mu.Lock()
	s.attrs = append(s.attrs, attrs...)
	s.mu.Unlock()
	return s
}

func (s *authKeyLogSink) WithGroup(string) slog.Handler { return s }

func (s *authKeyLogSink) snapshot() []authKeyLogRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]authKeyLogRecord(nil), s.records...)
}

type failingAuthKeyStore struct{ err error }

func (s failingAuthKeyStore) Save(context.Context, crypto.AuthKey) error { return nil }
func (s failingAuthKeyStore) Get(context.Context, [8]byte) (crypto.AuthKey, int64, bool, bool, error) {
	return crypto.AuthKey{}, 0, false, false, s.err
}
func (s failingAuthKeyStore) Touch(context.Context, [8]byte) error { return nil }

func (s *authKeyLogSink) waitFor(t *testing.T, count int) []authKeyLogRecord {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		if records := s.snapshot(); len(records) >= count {
			return records
		}
		select {
		case <-s.changed:
		case <-deadline.C:
			t.Fatalf("logged records = %d, want at least %d", len(s.snapshot()), count)
		}
	}
}

func TestUnknownAuthKeyLogsAreSampledAcrossConnections(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sink := newAuthKeyLogSink()
	server := mtproto.New(exchange.PrivateKey{}, 2, mtproto.NewMemoryAuthKeyStore(), nil, slog.New(sink))
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})

	const (
		connections = 4
		framesEach  = 5
	)
	keyID := [8]byte{1, 2, 3, 4, 5, 6, 7, 8}
	for range connections {
		raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		conn, err := transport.Abridged.Handshake(raw)
		if err != nil {
			t.Fatalf("transport handshake: %v", err)
		}
		for range framesEach {
			var frame bin.Buffer
			frame.Put(make([]byte, 16))
			copy(frame.Buf[:8], keyID[:])
			if err := conn.Send(ctx, &frame); err != nil {
				t.Fatalf("send unknown-key frame: %v", err)
			}
			var response bin.Buffer
			err := conn.Recv(ctx, &response)
			var protocolErr *codec.ProtocolErr
			if !errors.As(err, &protocolErr) || protocolErr.Code != codec.CodeAuthKeyNotFound {
				t.Fatalf("receive unknown-key response = %v, want -404", err)
			}
		}
		if err := raw.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
	}

	records := sink.waitFor(t, 1)
	if len(records) != 1 {
		t.Fatalf("diagnostic records = %d, want one within the sampling window", len(records))
	}
	record := records[0]
	if record.attrs["reason"] != "lookup_miss" {
		t.Fatalf("reason = %v, want lookup_miss", record.attrs["reason"])
	}
	if record.attrs["auth_key_id"] != hex.EncodeToString(keyID[:]) {
		t.Fatalf("auth_key_id = %v, want %s", record.attrs["auth_key_id"], hex.EncodeToString(keyID[:]))
	}
	if record.attrs["peer_addr"] != "127.0.0.1" {
		t.Fatalf("peer_addr = %v, want 127.0.0.1", record.attrs["peer_addr"])
	}
	if _, ok := record.attrs["event_time"]; !ok {
		t.Fatal("diagnostic has no event_time")
	}
	if _, ok := record.attrs["suppressed"]; !ok {
		t.Fatal("diagnostic has no suppressed count")
	}
}

func TestAuthKeyLookupErrorClosesWithoutReplyOrDecryptDetailsInLogs(t *testing.T) {
	t.Parallel()

	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sink := newAuthKeyLogSink()
	server := mtproto.New(exchange.PrivateKey{}, 2,
		failingAuthKeyStore{err: errors.New("stored key decrypt error: SECRET-DECRYPT-DETAIL")}, nil, slog.New(sink))
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})

	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		t.Fatalf("transport handshake: %v", err)
	}
	var frame bin.Buffer
	frame.Put(make([]byte, 16))
	frame.Buf[0] = 1
	if err := conn.Send(ctx, &frame); err != nil {
		t.Fatalf("send lookup frame: %v", err)
	}
	var response bin.Buffer
	err = conn.Recv(ctx, &response)
	var protocolErr *codec.ProtocolErr
	if err == nil || errors.As(err, &protocolErr) {
		t.Fatalf("lookup error response = %v, want connection close without -404", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	records := sink.waitFor(t, 1)
	for _, record := range records {
		logged := record.message + " " + fmt.Sprint(record.attrs)
		if strings.Contains(logged, "SECRET-DECRYPT-DETAIL") || strings.Contains(strings.ToLower(logged), "decrypt") {
			t.Fatalf("lookup failure log exposes decrypt details: %s", logged)
		}
	}
}
