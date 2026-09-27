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
				first, firstUpdate, firstAfter, err = h.handleSendMediaAfterReplyOnConn(originConn, encode(req))
				if err != nil {
					t.Fatalf("first media send: %v", err)
				}
				for range 70 {
					retry, retryUpdate, retryAfter, err = h.handleSendMediaAfterReplyOnConn(originConn, encode(req))
					if err != nil {
						t.Fatalf("stored media retry: %v", err)
					}
				}
			} else {
				req := &tg.MessagesSendMessageRequest{
					Peer: InputPeerUser(alice.ID, bob.ID), Message: "retry text", RandomID: randomID,
				}
				first, firstUpdate, firstAfter, err = h.handleSendMessageAfterReplyOnConn(originConn, encode(req))
				if err != nil {
					t.Fatalf("first text send: %v", err)
				}
				for range 70 {
					retry, retryUpdate, retryAfter, err = h.handleSendMessageAfterReplyOnConn(originConn, encode(req))
					if err != nil {
						t.Fatalf("stored text retry: %v", err)
					}
				}
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

			// The first result write is deliberately treated as failed. Its
			// fallback is unkeyed, so both live sender sessions receive the
			// committed message even though the origin got no RPC result.
			if firstUpdate.onFailure == nil {
				t.Fatal("first send did not return a result-failure hook")
			}
			firstUpdate.onFailure()
			select {
			case <-siblingTransport.done:
			case <-time.After(5 * time.Second):
				t.Fatal("sibling sender session received no failure push")
			}
			select {
			case <-originTransport.done:
			case <-time.After(5 * time.Second):
				t.Fatal("origin sender session received no failure push")
			}
			if got := siblingTransport.count(); got != 1 {
				t.Fatalf("sibling pushes = %d, want 1", got)
			}
			if got := originTransport.count(); got != 1 {
				t.Fatalf("origin pushes = %d, want 1", got)
			}

			// A successful stored retry uses the keyed hook. Both sessions are
			// already caught up from the failure fallback, so it must not echo
			// the origin or duplicate the sibling push.
			retryAfter()
			time.Sleep(100 * time.Millisecond)
			if got := siblingTransport.count(); got != 1 {
				t.Fatalf("sibling pushes after retry = %d, want 1", got)
			}
			if got := originTransport.count(); got != 1 {
				t.Fatalf("origin pushes after retry = %d, want 1", got)
			}
		})
	}
}

func TestPausedSendSiblingReadHistoryDoesNotEchoOrigin(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dsn := pgtest.DSN(t)
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(blobs))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() }) //nolint:errcheck // teardown

	alice, err := s.CreateUser(ctx, "+15557001001")
	if err != nil {
		t.Fatalf("create alice: %v", err)
	}
	bob, err := s.CreateUser(ctx, "+15557001002")
	if err != nil {
		t.Fatalf("create bob: %v", err)
	}
	if _, _, _, _, err := s.SendMessage(ctx, bob.ID, alice.ID, "before", 917010, 0, 0); err != nil {
		t.Fatalf("seed incoming message: %v", err)
	}

	registry := mtproto.NewSessionRegistry()
	updater := NewUpdater(s, registry, nil, pgtest.PeerDeriver())
	originKey := retryTestKey(21)
	siblingKey := retryTestKey(121)
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
	if !originConn.MarkRPCUpdate(alice.ID, mtproto.AuthKeyIDInt64(originKey.ID), 1) {
		t.Fatal("seed origin watermark")
	}

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
	t.Cleanup(func() { _ = stop() }) //nolint:errcheck // teardown
	if err := store.WaitForNotificationListener(ctx, s, 1); err != nil {
		t.Fatalf("wait for notification listener: %v", err)
	}

	h := testHandlers(s)
	committed := make(chan struct{})
	release := make(chan struct{})
	h.afterSenderCommit = func() {
		close(committed)
		<-release
	}
	encode := func(req bin.Encoder, key crypto.AuthKey) *mtproto.Request {
		var body bin.Buffer
		if err := req.Encode(&body); err != nil {
			t.Fatalf("encode request: %v", err)
		}
		return &mtproto.Request{Ctx: ctx, UserID: alice.ID, AuthKeyID: key.ID, Buf: &body}
	}
	sendReq := encode(&tg.MessagesSendMessageRequest{
		Peer:     InputPeerUser(alice.ID, bob.ID),
		Message:  "paused sender",
		RandomID: 917011,
	}, originKey)
	var result bin.Encoder
	var update *replyUpdate
	var afterReply func()
	var sendErr error
	done := make(chan struct{})
	go func() {
		result, update, afterReply, sendErr = h.handleSendMessageAfterReplyOnConn(originConn, sendReq)
		close(done)
	}()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatalf("send did not reach commit barrier: %v", ctx.Err())
	}

	if _, err := h.handleReadHistory(encode(&tg.MessagesReadHistoryRequest{
		Peer:  InputPeerUser(alice.ID, bob.ID),
		MaxID: 1,
	}, siblingKey)); err != nil {
		t.Fatalf("sibling readHistory: %v", err)
	}
	select {
	case <-siblingTransport.done:
	case <-ctx.Done():
		t.Fatalf("sibling did not receive readHistory push: %v", ctx.Err())
	}
	siblingDeadline := time.Now().Add(5 * time.Second)
	for siblingConn.LastPushedPts() < 3 && time.Now().Before(siblingDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := originTransport.count(); got != 0 {
		t.Fatalf("origin received %d generic pushes while result paused", got)
	}
	if got := originConn.LastPushedPts(); got != 1 {
		t.Fatalf("origin watermark = %d while result paused, want 1", got)
	}
	if got := siblingConn.LastPushedPts(); got != 3 {
		t.Fatalf("sibling watermark = %d, want 3 after readHistory", got)
	}

	close(release)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatalf("send did not finish: %v", ctx.Err())
	}
	if sendErr != nil {
		t.Fatalf("send: %v", sendErr)
	}
	if result == nil || update == nil || afterReply == nil {
		t.Fatal("send did not return result metadata")
	}
	if err := originConn.SendResultAndMarkRPCUpdate(
		&mtproto.Request{Ctx: ctx, UserID: alice.ID, AuthKeyID: originKey.ID, MsgID: 1},
		result, alice.ID, mtproto.AuthKeyIDInt64(originKey.ID), update.pts,
	); err != nil {
		t.Fatalf("send result: %v", err)
	}
	afterReply()
	deadline := time.Now().Add(5 * time.Second)
	for originConn.LastPushedPts() < 3 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := originConn.LastPushedPts(); got != 3 {
		t.Fatalf("origin watermark after result = %d, want 3", got)
	}
	if got := siblingConn.LastPushedPts(); got != 3 {
		t.Fatalf("sibling watermark after result = %d, want 3", got)
	}
}
