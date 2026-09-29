package e2e_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/config"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type helpPollingFixture struct {
	ctx       context.Context
	store     *store.Store
	codes     *codeSink
	newClient func(*session.StorageMemory) *telegram.Client
}

func newHelpPollingFixture(t *testing.T) *helpPollingFixture {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}

	st, err := store.Open(ctx, pgtest.DSN(t), pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newCodeSink()
	blobs, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	handler := api.New(st, dcID, api.DefaultConfig(dcID, "127.0.0.1", 0), codes.Logger(), true, 100<<20, blobs, 2<<30, pgtest.PeerDeriver(), config.RateLimitsConfig{}, config.RegistrationClosed)
	server := mtproto.New(exchange.PrivateKey{RSA: key}, dcID, mtproto.NewPgAuthKeyStore(st), handler, codes.Logger())

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}

	srvCtx, srvCancel := context.WithCancel(ctx)
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(srvCtx, ln) }()
	t.Cleanup(func() {
		srvCancel()
		if serr := <-serveErr; serr != nil && !errors.Is(serr, context.Canceled) {
			t.Errorf("server serve: %v", serr)
		}
	})

	return &helpPollingFixture{
		ctx:   ctx,
		store: st,
		codes: codes,
		newClient: func(sess *session.StorageMemory) *telegram.Client {
			return telegram.NewClient(1, "hash", telegram.Options{
				DC:             dcID,
				DCList:         dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: addr.Port}}},
				PublicKeys:     []telegram.PublicKey{{RSA: &key.PublicKey}},
				Resolver:       dcs.Plain(dcs.PlainOptions{}),
				SessionStorage: sess,
			})
		},
	}
}

// TestHelpTermsAndPromoPolling exercises the successful result shapes and both
// existing rejection paths through real gotd clients.
func TestHelpTermsAndPromoPolling(t *testing.T) {
	t.Parallel()
	f := newHelpPollingFixture(t)

	const phone = "+15551239981"
	seedPhoneUsers(t, f.ctx, f.store, phone)
	flow := auth.NewFlow(
		auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
			func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
				return f.codes.wait(ctx)
			})),
		auth.SendCodeOptions{},
	)

	authSession := &session.StorageMemory{}
	client := f.newClient(authSession)
	var authKeyID int64
	if err := client.Run(f.ctx, func(ctx context.Context) error {
		if err := client.Auth().IfNecessary(ctx, flow); err != nil {
			return fmt.Errorf("login: %w", err)
		}
		user, ok, err := f.store.UserByPhone(ctx, phone)
		if err != nil {
			return fmt.Errorf("lookup logged-in user: %w", err)
		}
		if !ok {
			return errors.New("logged-in user was not persisted")
		}
		keys, err := f.store.AuthKeysByUser(ctx, user.ID)
		if err != nil {
			return fmt.Errorf("lookup logged-in auth keys: %w", err)
		}
		if len(keys) != 1 {
			return fmt.Errorf("logged-in auth key count=%d, want 1", len(keys))
		}
		authKeyID = keys[0].ID

		raw := client.API()
		for range 2 {
			got, err := raw.HelpGetTermsOfServiceUpdate(ctx)
			if err != nil {
				return fmt.Errorf("help.getTermsOfServiceUpdate: %w", err)
			}
			terms, ok := got.(*tg.HelpTermsOfServiceUpdateEmpty)
			if !ok {
				return fmt.Errorf("help.getTermsOfServiceUpdate result = %T, want *tg.HelpTermsOfServiceUpdateEmpty", got)
			}
			if err := assertHelpPollingExpiry(terms.Expires); err != nil {
				return fmt.Errorf("help.getTermsOfServiceUpdate: %w", err)
			}

			gotPromo, err := raw.HelpGetPromoData(ctx)
			if err != nil {
				return fmt.Errorf("help.getPromoData: %w", err)
			}
			promo, ok := gotPromo.(*tg.HelpPromoDataEmpty)
			if !ok {
				return fmt.Errorf("help.getPromoData result = %T, want *tg.HelpPromoDataEmpty", gotPromo)
			}
			if err := assertHelpPollingExpiry(promo.Expires); err != nil {
				return fmt.Errorf("help.getPromoData: %w", err)
			}
		}

		// Repeated authorized polls do not consume the shared unsupported-call
		// budget used by methods that remain unimplemented.
		for i := range 65 {
			if _, err := raw.HelpGetPromoData(ctx); err != nil {
				return fmt.Errorf("authorized help.getPromoData call %d: %w", i+1, err)
			}
		}
		if _, err := raw.HelpGetNearestDC(ctx); err == nil {
			return errors.New("help.getNearestDc succeeded after authorized polling, want INPUT_METHOD_INVALID")
		} else if !tgerr.Is(err, "INPUT_METHOD_INVALID") {
			return fmt.Errorf("help.getNearestDc after authorized polling: %w", err)
		}

		// Trailing request data is malformed for these argument-free methods
		// and must not be mistaken for either successful polling method.
		for _, malformed := range []struct {
			name    string
			request bin.Encoder
		}{
			{name: "help.getTermsOfServiceUpdate", request: malformedHelpPollingRequest{&tg.HelpGetTermsOfServiceUpdateRequest{}}},
			{name: "help.getPromoData", request: malformedHelpPollingRequest{&tg.HelpGetPromoDataRequest{}}},
		} {
			err := client.Invoke(ctx, malformed.request, &helpPollingRawResponse{})
			if err == nil {
				return fmt.Errorf("malformed %s call succeeded", malformed.name)
			}
			if !tgerr.Is(err, "INPUT_METHOD_INVALID") {
				return fmt.Errorf("malformed %s call: %w", malformed.name, err)
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("login and authorized polling: %v", err)
	}
	provisional, err := f.store.CreateUsernameUser(f.ctx, "helppolling", "Help", "Polling")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.store.ClaimUsername(f.ctx, provisional.ID, "helppolling"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.BindAuthKeyUser(f.ctx, authKeyID, provisional.ID); err != nil {
		t.Fatal(err)
	}

	provisionalClient := f.newClient(authSession)
	if err := provisionalClient.Run(f.ctx, func(ctx context.Context) error {
		raw := provisionalClient.API()
		for name, call := range map[string]func(context.Context) error{
			"help.getTermsOfServiceUpdate": func(ctx context.Context) error {
				_, err := raw.HelpGetTermsOfServiceUpdate(ctx)
				return err
			},
			"help.getPromoData": func(ctx context.Context) error {
				_, err := raw.HelpGetPromoData(ctx)
				return err
			},
		} {
			if err := assertHelpPollingRPCError(name, call(ctx), 401, "AUTH_KEY_UNREGISTERED"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("provisional calls: %v", err)
	}

	unboundClient := f.newClient(&session.StorageMemory{})
	if err := unboundClient.Run(f.ctx, func(ctx context.Context) error {
		raw := unboundClient.API()
		for name, call := range map[string]func(context.Context) error{
			"help.getTermsOfServiceUpdate": func(ctx context.Context) error {
				_, err := raw.HelpGetTermsOfServiceUpdate(ctx)
				return err
			},
			"help.getPromoData": func(ctx context.Context) error {
				_, err := raw.HelpGetPromoData(ctx)
				return err
			},
		} {
			if err := assertHelpPollingRPCError(name, call(ctx), 400, "INPUT_METHOD_INVALID"); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("unbound calls: %v", err)
	}
}

func assertHelpPollingExpiry(expires int) error {
	delta := int64(expires) - time.Now().Unix()
	if delta < 3597 || delta > 3601 {
		return fmt.Errorf("expiry delta = %d seconds, want future timestamp about one hour away", delta)
	}
	return nil
}

func assertHelpPollingRPCError(method string, err error, code int, message string) error {
	if err == nil {
		return fmt.Errorf("%s succeeded, want %d %s", method, code, message)
	}
	var rpcErr *tgerr.Error
	if !errors.As(err, &rpcErr) {
		return fmt.Errorf("%s error = %T (%w), want RPC error %d %s", method, err, err, code, message)
	}
	if rpcErr.Code != code || rpcErr.Message != message {
		return fmt.Errorf("%s error = %d %s, want %d %s", method, rpcErr.Code, rpcErr.Message, code, message)
	}
	return nil
}

type helpPollingRawResponse struct{}

func (*helpPollingRawResponse) Decode(*bin.Buffer) error { return nil }

type malformedHelpPollingRequest struct {
	request bin.Encoder
}

func (r malformedHelpPollingRequest) Encode(b *bin.Buffer) error {
	if err := r.request.Encode(b); err != nil {
		return err
	}
	b.PutInt(1)
	return nil
}
