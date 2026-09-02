package api_test

import (
	"context"
	"io"
	"log/slog"
	"net/netip"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type pendingReplyTransport struct {
	onSend func()
}

func (t *pendingReplyTransport) Send(context.Context, *bin.Buffer) error {
	t.onSend()
	return nil
}

func (*pendingReplyTransport) Recv(context.Context, *bin.Buffer) error { return io.EOF }
func (*pendingReplyTransport) Close() error                            { return nil }

func TestSignInMarksPendingLoginBeforeReply(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	h := api.New(
		s,
		2,
		&tg.Config{},
		slog.New(slog.DiscardHandler),
		false,
		1,
		nil,
		1,
		pgtest.PeerDeriver(),
		config.RateLimitsConfig{},
		config.RegistrationInvite,
	)

	t.Run("phone", func(t *testing.T) {
		user, err := s.CreateUser(ctx, "+15551299901")
		if err != nil {
			t.Fatal(err)
		}
		if err := s.UpsertPassword(ctx, passwordForTest(user.ID)); err != nil {
			t.Fatal(err)
		}
		hash, code, err := s.IssueCode(ctx, "+15551299901")
		if err != nil {
			t.Fatal(err)
		}
		key := savedKey(ctx, t, s, 11)

		assertSignInMarksPending(t, h, key, &tg.AuthSignInRequest{
			PhoneNumber:   "+15551299901",
			PhoneCodeHash: hash,
			PhoneCode:     code,
		})
	})

	t.Run("username", func(t *testing.T) {
		user, err := s.CreateUsernameUser(ctx, "pendinglogin", "Pending", "Login")
		if err != nil {
			t.Fatal(err)
		}
		if err := api.ClaimUsernameForTest(s, user.ID, "pendinglogin"); err != nil {
			t.Fatal(err)
		}
		if err := s.UpsertPassword(ctx, passwordForTest(user.ID)); err != nil {
			t.Fatal(err)
		}
		hash, _, err := s.IssueCodeForUsername(ctx, "pendinglogin")
		if err != nil {
			t.Fatal(err)
		}
		key := savedKey(ctx, t, s, 12)

		assertSignInMarksPending(t, h, key, &tg.AuthSignInRequest{
			PhoneNumber:   "pendinglogin",
			PhoneCodeHash: hash,
		})
	})
}

func passwordForTest(userID int64) store.UserPassword {
	return store.UserPassword{
		UserID:   userID,
		Salt1:    []byte{1, 2, 3, 4},
		Salt2:    []byte{5, 6, 7, 8},
		Verifier: []byte{9, 10, 11, 12},
	}
}

func assertSignInMarksPending(t *testing.T, h mtproto.Handler, key crypto.AuthKey, signIn *tg.AuthSignInRequest) {
	t.Helper()
	var body bin.Buffer
	if err := signIn.Encode(&body); err != nil {
		t.Fatal(err)
	}

	var conn *mtproto.Conn
	sentPending := false
	transport := &pendingReplyTransport{onSend: func() {
		sentPending = conn.PendingLogin()
	}}
	conn = mtproto.NewTestConn(transport, key)
	if conn.PendingLogin() {
		t.Fatal("new sign-in connection is already pending")
	}

	err := h.OnMessage(conn, &mtproto.Request{
		AuthKeyID:  key.ID,
		ClientAddr: netip.MustParseAddr("10.0.0.20"),
		MsgID:      1 << 32,
		Buf:        &body,
		Ctx:        context.Background(),
	})
	if err != nil {
		t.Fatalf("signIn handler: %v", err)
	}
	if !conn.PendingLogin() {
		t.Fatal("SESSION_PASSWORD_NEEDED did not mark the connection pending")
	}
	if !sentPending {
		t.Fatal("reply was sent before the pending-login marker was set")
	}
}
