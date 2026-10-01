package api_test

import (
	"context"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/bin"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

type dialogFilterRecoveryTransport struct {
	blocked bool
	entered chan struct{}
	sent    chan struct{}
}

func (t *dialogFilterRecoveryTransport) Send(ctx context.Context, _ *bin.Buffer) error {
	if t.blocked {
		close(t.entered)
		<-ctx.Done()
		return ctx.Err()
	}
	select {
	case t.sent <- struct{}{}:
	default:
	}
	return nil
}

func (*dialogFilterRecoveryTransport) Recv(context.Context, *bin.Buffer) error {
	return errors.New("unused")
}

func (*dialogFilterRecoveryTransport) Close() error { return nil }

func TestDialogFilterRecoveryStalledSocketDoesNotBlockAnotherConnection(t *testing.T) {
	registry := mtproto.NewSessionRegistry()
	syncState := api.NewDialogFilterSync()
	ownerID := int64(901)

	stalledTransport := &dialogFilterRecoveryTransport{
		blocked: true,
		entered: make(chan struct{}),
	}
	readyTransport := &dialogFilterRecoveryTransport{sent: make(chan struct{}, 1)}
	for _, transport := range []*dialogFilterRecoveryTransport{stalledTransport, readyTransport} {
		conn := mtproto.NewTestConn(transport, testKey())
		conn.SetOwner(ownerID)
		if !registry.Add(ownerID, conn) {
			t.Fatal("registry rejected recovery connection")
		}
		syncState.EnsureBinding(conn, &mtproto.Request{UserID: ownerID, SessionID: 123})
		if !conn.AcknowledgeDialogFilterDifference(ownerID, 123, true) {
			t.Fatal("initial difference was not acknowledged")
		}
	}
	syncState.OwnerInvalidation(registry, ownerID)

	updater := api.NewUpdaterWithDialogFilterSync(nil, registry, slog.New(slog.DiscardHandler), nil, syncState)
	stop := updater.StartDialogFilterRecovery(context.Background())
	t.Cleanup(stop)

	select {
	case <-stalledTransport.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("stalled connection did not receive its recovery push")
	}
	select {
	case <-readyTransport.sent:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("one stalled socket prevented another connection's recovery push")
	}
	stop()
}
