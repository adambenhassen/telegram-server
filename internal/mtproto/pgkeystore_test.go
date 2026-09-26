package mtproto_test

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/mt"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestFreshConnectionsRejectAbsentAndDeletedPersistedAuthKeys(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 60*time.Second)
	defer cancel()

	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	st, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(blobs))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if err := st.Close(); err != nil {
			t.Errorf("close store: %v", err)
		}
	})

	user, err := st.CreateUser(ctx, "+15551239992")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	var rawKey crypto.Key
	for i := range rawKey {
		rawKey[i] = byte(i + 1)
	}
	key := rawKey.WithID()
	keys := mtproto.NewPgAuthKeyStore(st)
	if err := keys.Save(ctx, key); err != nil {
		t.Fatalf("save auth key: %v", err)
	}
	if err := st.BindAuthKeyUser(ctx, key.IntID(), user.ID); err != nil {
		t.Fatalf("bind auth key: %v", err)
	}
	initial, ok, err := st.AuthKeyByID(ctx, key.IntID())
	if err != nil || !ok {
		t.Fatalf("load auth key before connection: ok=%v err=%v", ok, err)
	}
	if initial.UserID != user.ID {
		t.Fatalf("auth key bound to user %d, want %d", initial.UserID, user.ID)
	}

	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverCtx, stopServer := context.WithCancel(ctx)
	server := mtproto.New(exchange.PrivateKey{}, 2, keys, nil, nil)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serverCtx, listener) }()
	t.Cleanup(func() {
		stopServer()
		if err := <-serveDone; err != nil {
			t.Errorf("serve: %v", err)
		}
	})

	// A lookup hit cannot advance last_seen_at unless the frame's MAC validates.
	invalidFrame := clientFrame(t, key, 42, 1<<32, &mt.PingRequest{PingID: 1})
	invalidFrame[len(invalidFrame)-1] ^= 1
	if err := sendFrameExpectClose(ctx, listener.Addr().String(), invalidFrame); err != nil {
		t.Fatalf("invalid encrypted frame: %v", err)
	}
	afterInvalid, ok, err := st.AuthKeyByID(ctx, key.IntID())
	if err != nil || !ok {
		t.Fatalf("load auth key after invalid frame: ok=%v err=%v", ok, err)
	}
	if !afterInvalid.LastSeenAt.Equal(initial.LastSeenAt) {
		t.Fatalf("last_seen_at changed on an unauthenticated frame: before=%s after=%s", initial.LastSeenAt, afterInvalid.LastSeenAt)
	}

	var absentRaw crypto.Key
	for i := range absentRaw {
		absentRaw[i] = byte(255 - i)
	}
	absent := absentRaw.WithID()
	if absent.ID == key.ID {
		t.Fatal("test auth keys unexpectedly share an ID")
	}
	if err := sendAuthKeyIDAndExpect404(ctx, listener.Addr().String(), absent.ID); err != nil {
		t.Fatalf("absent key response: %v", err)
	}
	if _, ok, err := st.AuthKeyByID(ctx, absent.IntID()); err != nil || ok {
		t.Fatalf("absent key was created: ok=%v err=%v", ok, err)
	}

	if err := st.DeleteAuthKey(ctx, key.IntID()); err != nil {
		t.Fatalf("delete auth key: %v", err)
	}
	if err := sendAuthKeyIDAndExpect404(ctx, listener.Addr().String(), key.ID); err != nil {
		t.Fatalf("deleted key response: %v", err)
	}
	if _, ok, err := st.AuthKeyByID(ctx, key.IntID()); err != nil || ok {
		t.Fatalf("deleted key was restored: ok=%v err=%v", ok, err)
	}
	userKeys, err := st.AuthKeysByUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("auth keys by user: %v", err)
	}
	if len(userKeys) != 0 {
		t.Fatalf("deleted key was rebound or restored: got %d auth keys", len(userKeys))
	}
}

func sendFrameExpectClose(ctx context.Context, addr string, frame []byte) error {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer raw.Close() //nolint:errcheck // this test only inspects the server response.
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		return err
	}
	var request bin.Buffer
	request.ResetTo(frame)
	if err := conn.Send(ctx, &request); err != nil {
		return err
	}
	var response bin.Buffer
	err = conn.Recv(ctx, &response)
	var protocolErr *codec.ProtocolErr
	if err == nil || errors.As(err, &protocolErr) {
		return errors.New("server replied instead of closing after invalid encrypted frame")
	}
	return nil
}

func sendAuthKeyIDAndExpect404(ctx context.Context, addr string, id [8]byte) error {
	raw, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	defer raw.Close() //nolint:errcheck // the protocol error is the assertion.
	conn, err := transport.Abridged.Handshake(raw)
	if err != nil {
		return err
	}
	var request bin.Buffer
	request.Put(make([]byte, 16))
	copy(request.Buf[:8], id[:])
	if err := conn.Send(ctx, &request); err != nil {
		return err
	}
	var response bin.Buffer
	err = conn.Recv(ctx, &response)
	var protocolErr *codec.ProtocolErr
	if !errors.As(err, &protocolErr) || protocolErr.Code != codec.CodeAuthKeyNotFound {
		return errors.New("server did not return -404 for absent auth key")
	}
	return nil
}
