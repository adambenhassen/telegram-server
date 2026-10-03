package api_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/gotd/td/bin"

	"github.com/teagramhq/teagram-server/internal/api"
	"github.com/teagramhq/teagram-server/internal/mtproto"
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

func TestDialogFilterRecoveryQueuesPendingConnectionAheadOfIdleConnections(t *testing.T) {
	for _, idleConnections := range []int{640, 6399} {
		t.Run(fmt.Sprintf("%d idle connections", idleConnections), func(t *testing.T) {
			registry := mtproto.NewSessionRegistry()
			syncState := api.NewDialogFilterSync()
			targetOwner := int64(10_000 + idleConnections)
			targetTransport := &dialogFilterRecoveryTransport{sent: make(chan struct{}, 1)}
			var target *mtproto.Conn

			for i := range idleConnections + 1 {
				ownerID := int64(10_000 + i)
				transport := &dialogFilterRecoveryTransport{}
				if ownerID == targetOwner {
					transport = targetTransport
				}
				conn := mtproto.NewTestConn(transport, testKey())
				conn.SetOwner(ownerID)
				if !registry.Add(ownerID, conn) {
					t.Fatalf("registry rejected connection for owner %d", ownerID)
				}
				syncState.EnsureBinding(conn, &mtproto.Request{UserID: ownerID, SessionID: 123})
				if ownerID == targetOwner {
					target = conn
				}
			}
			if target == nil || !target.AcknowledgeDialogFilterDifference(targetOwner, 123, true) {
				t.Fatal("target connection did not complete its initial difference")
			}
			syncState.OwnerInvalidation(registry, targetOwner)

			updater := api.NewUpdaterWithDialogFilterSync(nil, registry, slog.New(slog.DiscardHandler), nil, syncState)
			stop := updater.StartDialogFilterRecovery(context.Background())
			t.Cleanup(stop)

			select {
			case <-targetTransport.sent:
			case <-time.After(3 * time.Second):
				t.Fatalf("pending connection waited behind %d idle registry connections", idleConnections)
			}
		})
	}
}
