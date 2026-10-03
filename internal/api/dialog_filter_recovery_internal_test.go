package api

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"

	"github.com/teagramhq/teagram-server/internal/mtproto"
)

type recoveryRebindTransport struct {
	sent chan struct{}
}

func (t *recoveryRebindTransport) Send(context.Context, *bin.Buffer) error {
	select {
	case t.sent <- struct{}{}:
	default:
	}
	return nil
}

func (*recoveryRebindTransport) Recv(context.Context, *bin.Buffer) error {
	return errors.New("unused")
}

func (*recoveryRebindTransport) Close() error { return nil }

func recoveryTestKey() crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	return raw.WithID()
}

func TestDialogFilterRecoveryAcknowledgementsDoNotQueueIdleBindings(t *testing.T) {
	tests := []struct {
		name            string
		idleConnections int
		acknowledge     func(*DialogFilterSync, *mtproto.Conn, *mtproto.Request, DialogFilterCapture)
	}{
		{
			name:            "successful folder fetch with 641 connections",
			idleConnections: 640,
			acknowledge: func(syncState *DialogFilterSync, conn *mtproto.Conn, req *mtproto.Request, captured DialogFilterCapture) {
				syncState.AcknowledgeFetch(conn, req, captured)
			},
		},
		{
			name:            "first difference with 6400 connections",
			idleConnections: 6399,
			acknowledge: func(syncState *DialogFilterSync, conn *mtproto.Conn, req *mtproto.Request, captured DialogFilterCapture) {
				syncState.AcknowledgeDifference(conn, req, captured, true)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := mtproto.NewSessionRegistry()
			syncState := NewDialogFilterSync()
			register := func(ownerID int64, transport *recoveryRebindTransport) *mtproto.Conn {
				conn := mtproto.NewTestConn(transport, recoveryTestKey())
				conn.SetOwner(ownerID)
				conn.SetSession(123)
				if !registry.Add(ownerID, conn) {
					t.Fatalf("registry rejected connection for owner %d", ownerID)
				}
				req := &mtproto.Request{UserID: ownerID, SessionID: 123}
				captured := syncState.Capture(conn, req)
				tt.acknowledge(syncState, conn, req, captured)
				if _, _, _, pending, ok := conn.DialogFilterRecoverySnapshot(ownerID, 123, 0); !ok || pending {
					t.Fatalf("owner %d acknowledgement left recovery pending: ok=%t pending=%t", ownerID, ok, pending)
				}
				return conn
			}

			for i := range tt.idleConnections {
				register(int64(10_000+i), &recoveryRebindTransport{})
			}
			targetOwner := int64(10_000 + tt.idleConnections)
			targetTransport := &recoveryRebindTransport{sent: make(chan struct{}, 1)}
			register(targetOwner, targetTransport)
			syncState.OwnerInvalidation(registry, targetOwner)

			updater := NewUpdaterWithDialogFilterSync(nil, registry, slog.New(slog.DiscardHandler), nil, syncState)
			t.Cleanup(func() { updater.recoveryWG.Wait() })
			updater.recoverDialogFilters(context.Background(), time.Now())
			select {
			case <-targetTransport.sent:
			case <-time.After(time.Second):
				t.Fatalf("pending target waited behind %d successfully acknowledged idle connections", tt.idleConnections)
			}
		})
	}
}

type recoveryBlockingTransport struct {
	entered     chan struct{}
	release     chan struct{}
	sent        chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func (t *recoveryBlockingTransport) Send(ctx context.Context, _ *bin.Buffer) error {
	t.enteredOnce.Do(func() { close(t.entered) })
	select {
	case <-t.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case t.sent <- struct{}{}:
	default:
	}
	return nil
}

func (*recoveryBlockingTransport) Recv(context.Context, *bin.Buffer) error {
	return errors.New("unused")
}

func (*recoveryBlockingTransport) Close() error { return nil }

func (t *recoveryBlockingTransport) unblock() {
	t.releaseOnce.Do(func() { close(t.release) })
}

func TestDialogFilterRecoveryUpdaterFencesClaimAcrossSessionRebind(t *testing.T) {
	const ownerID = 902
	transport := &recoveryRebindTransport{sent: make(chan struct{}, 1)}
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	conn := mtproto.NewTestConn(transport, raw.WithID())
	conn.SetOwner(ownerID)
	registry := mtproto.NewSessionRegistry()
	if !registry.Add(ownerID, conn) {
		t.Fatal("registry rejected recovery connection")
	}
	syncState := NewDialogFilterSync()
	syncState.EnsureBinding(conn, &mtproto.Request{UserID: ownerID, SessionID: 123})
	if !conn.AcknowledgeDialogFilterDifference(ownerID, 123, true) {
		t.Fatal("initial difference was not acknowledged")
	}
	syncState.OwnerInvalidation(registry, ownerID)

	claimed := make(chan struct{}, 1)
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseWrite := func() { releaseOnce.Do(func() { close(release) }) }
	updater := NewUpdaterWithDialogFilterSync(nil, registry, slog.New(slog.DiscardHandler), nil, syncState)
	updater.recoveryClaimHook = func(*mtproto.Conn) {
		claimed <- struct{}{}
		<-release
	}
	stop := updater.StartDialogFilterRecovery(context.Background())
	t.Cleanup(stop)
	t.Cleanup(releaseWrite)

	select {
	case <-claimed:
	case <-time.After(3 * time.Second):
		t.Fatal("updater did not claim the recovery attempt")
	}
	conn.SetSession(124)
	releaseWrite()

	select {
	case <-transport.sent:
		t.Fatal("claimed session A recovery reached the socket after rebinding to session B")
	case <-time.After(250 * time.Millisecond):
	}
	stop()
}

func TestDialogFilterRecoveryRetainsCandidatesWhenWriteSlotsAreSaturated(t *testing.T) {
	const ownerID = 903
	registry := mtproto.NewSessionRegistry()
	syncState := NewDialogFilterSync()
	transports := make([]*recoveryBlockingTransport, 17)
	for i := range transports {
		transports[i] = &recoveryBlockingTransport{
			entered: make(chan struct{}),
			release: make(chan struct{}),
			sent:    make(chan struct{}, 1),
		}
		conn := mtproto.NewTestConn(transports[i], recoveryTestKey())
		conn.SetOwner(ownerID)
		conn.SetSession(int64(123 + i))
		if !registry.Add(ownerID, conn) {
			t.Fatalf("registry rejected recovery connection %d", i)
		}
		syncState.EnsureBinding(conn, &mtproto.Request{UserID: ownerID, SessionID: int64(123 + i)})
		if !conn.AcknowledgeDialogFilterDifference(ownerID, int64(123+i), true) {
			t.Fatalf("connection %d did not complete its initial difference", i)
		}
	}
	syncState.OwnerInvalidation(registry, ownerID)

	updater := NewUpdaterWithDialogFilterSync(nil, registry, slog.New(slog.DiscardHandler), nil, syncState)
	t.Cleanup(func() {
		for _, transport := range transports {
			transport.unblock()
		}
		updater.recoveryWG.Wait()
	})
	for i := 16; i < len(transports); i++ {
		transports[i].unblock()
	}

	now := time.Now()
	updater.recoverDialogFilters(context.Background(), now)
	for i := range 16 {
		select {
		case <-transports[i].entered:
		case <-time.After(time.Second):
			t.Fatalf("write slot %d was not filled", i)
		}
	}
	select {
	case <-transports[16].entered:
		t.Fatal("candidate entered transport while all write slots were occupied")
	default:
	}

	updater.recoverDialogFilters(context.Background(), now.Add(time.Second))
	select {
	case <-transports[16].entered:
		t.Fatal("saturated sweep unexpectedly started the queued candidate")
	default:
	}

	for i := range 16 {
		transports[i].unblock()
	}
	updater.recoveryWG.Wait()
	updater.recoverDialogFilters(context.Background(), now.Add(2*time.Second))
	select {
	case <-transports[16].entered:
	case <-time.After(time.Second):
		t.Fatal("queued candidate was lost after write slots became available")
	}
}
