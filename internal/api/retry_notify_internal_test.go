package api

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type retryNotifyTransport struct {
	mu   sync.Mutex
	sent int
	done chan struct{}
}

func (t *retryNotifyTransport) Send(context.Context, *bin.Buffer) error {
	t.mu.Lock()
	t.sent++
	if t.sent == 1 {
		close(t.done)
	}
	t.mu.Unlock()
	return nil
}

func (*retryNotifyTransport) Recv(context.Context, *bin.Buffer) error { return errors.New("unused") }
func (*retryNotifyTransport) Close() error                            { return nil }

var _ transport.Conn = (*retryNotifyTransport)(nil)

func (t *retryNotifyTransport) count() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sent
}

func retryTestKey(seed byte) crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = seed + byte(i)
	}
	return raw.WithID()
}

func TestStoredRetryNotifiesSiblingAfterResultWriteFailure(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		media bool
	}{
		{name: "text"},
		{name: "media", media: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			dsn := pgtest.DSN(t)
			blobs, err := blob.NewLocal(t.TempDir())
			if err != nil {
				t.Fatalf("blob store: %v", err)
			}
			s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(blobs))
			if err != nil {
				t.Fatalf("open store: %v", err)
			}
			t.Cleanup(func() {
				if err := s.Close(); err != nil {
					t.Errorf("close store: %v", err)
				}
			})

			alice, err := s.CreateUser(ctx, "+15557000001")
			if err != nil {
				t.Fatalf("create alice: %v", err)
			}
			bob, err := s.CreateUser(ctx, "+15557000002")
			if err != nil {
				t.Fatalf("create bob: %v", err)
			}

			registry := mtproto.NewSessionRegistry()
			updater := NewUpdater(s, registry, nil, pgtest.PeerDeriver())
			originKey := retryTestKey(1)
			siblingKey := retryTestKey(99)
			originTransport := &retryNotifyTransport{done: make(chan struct{})}
			siblingTransport := &retryNotifyTransport{done: make(chan struct{})}
			originConn := mtproto.NewTestConn(originTransport, originKey)
			originConn.SetOwner(alice.ID)
			siblingConn := mtproto.NewTestConn(siblingTransport, siblingKey)
			siblingConn.SetOwner(alice.ID)
			if !registry.Add(alice.ID, originConn) || !registry.Add(alice.ID, siblingConn) {
				t.Fatal("register sender sessions")
			}
			t.Cleanup(func() {
				registry.Remove(alice.ID, originConn)
				registry.Remove(alice.ID, siblingConn)
			})

			_, stop, err := store.StartListener(ctx, dsn,
				updater.Deliver,
				func(context.Context, int64, int64) {},
				func(context.Context, int64, int64) {},
				func(context.Context, int64) {},
				func(context.Context, int64, int64) {},
				func(context.Context, int64, bool) {},
				func(context.Context, int64, int) {},
				func(context.Context, int64, int64, int64) {},
				func(context.Context, store.PeerType, int64, int32) {},
				nil,
			)
			if err != nil {
				t.Fatalf("start listener: %v", err)
			}
			t.Cleanup(func() { _ = stop() }) //nolint:errcheck // best-effort teardown
			if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
				t.Fatalf("wait for listener: %v", err)
			}

			h := testHandlers(s)
			h.maxUserStorageBytes = 2 << 30
			const randomID int64 = 917001
			var first, retry bin.Encoder
			var firstUpdate, retryUpdate *replyUpdate
			var firstAfter, retryAfter func()
			encode := func(req bin.Encoder) *mtproto.Request {
				var body bin.Buffer
				if err := req.Encode(&body); err != nil {
					t.Fatalf("encode request: %v", err)
				}
				return &mtproto.Request{Ctx: ctx, UserID: alice.ID, AuthKeyID: originKey.ID, Buf: &body}
			}

			if tc.media {
				const fileID int64 = 917002
				if _, err := h.handleSaveFilePart(encode(&tg.UploadSaveFilePartRequest{
					FileID: fileID, FilePart: 0, Bytes: []byte("retry media"),
				})); err != nil {
					t.Fatalf("save media part: %v", err)
				}
				req := &tg.MessagesSendMediaRequest{
					Peer: InputPeerUser(alice.ID, bob.ID),
					Media: &tg.InputMediaUploadedDocument{
						File:     &tg.InputFile{ID: fileID, Parts: 1, Name: "retry.txt"},
						MimeType: "text/plain",
					},
					Message:  "retry media",
					RandomID: randomID,
				}
				first, firstUpdate, firstAfter, err = h.handleSendMediaAfterReply(encode(req))
				if err != nil {
					t.Fatalf("first media send: %v", err)
				}
				retry, retryUpdate, retryAfter, err = h.handleSendMediaAfterReply(encode(req))
			} else {
				req := &tg.MessagesSendMessageRequest{
					Peer: InputPeerUser(alice.ID, bob.ID), Message: "retry text", RandomID: randomID,
				}
				first, firstUpdate, firstAfter, err = h.handleSendMessageAfterReply(encode(req))
				if err != nil {
					t.Fatalf("first text send: %v", err)
				}
				retry, retryUpdate, retryAfter, err = h.handleSendMessageAfterReply(encode(req))
			}
			if err != nil {
				t.Fatalf("stored retry: %v", err)
			}
			if first == nil || firstUpdate == nil || firstAfter == nil {
				t.Fatal("first send did not return reply metadata and hook")
			}
			if retry == nil || retryUpdate == nil || retryAfter == nil {
				t.Fatal("stored retry did not return reply metadata and hook")
			}
			if retryUpdate.pts != firstUpdate.pts {
				t.Fatalf("retry pts = %d, first pts = %d", retryUpdate.pts, firstUpdate.pts)
			}

			// The first result write is deliberately treated as failed: its
			// post-reply hook is not run. The retry is the first successful wire
			// result and must notify the sibling sender session.
			retryAfter()
			select {
			case <-siblingTransport.done:
			case <-time.After(5 * time.Second):
				t.Fatal("sibling sender session received no retry push")
			}
			if got := siblingTransport.count(); got != 1 {
				t.Fatalf("sibling pushes = %d, want 1", got)
			}
			if got := originTransport.count(); got != 0 {
				t.Fatalf("origin pushes = %d, want 0", got)
			}
		})
	}
}
