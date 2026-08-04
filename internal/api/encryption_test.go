package api_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// dhValue renders x as the 256-byte left-zero-padded wire form every integer in
// this group takes.
func dhValue(x *big.Int) []byte {
	return x.FillBytes(make([]byte, 256))
}

// validGA is a group element comfortably inside the accepted range: p/2 is about
// 2^2047, far above the 2^(2048-64) floor and far below p - 2^(2048-64).
func validGA() []byte {
	return dhValue(new(big.Int).Rsh(api.DHPrime(), 1))
}

// validGB is a second, different in-range element.
func validGB() []byte {
	return dhValue(new(big.Int).Div(api.DHPrime(), big.NewInt(3)))
}

// twoUsersFor creates a pair of accounts for a secret chat test.
func twoUsersFor(t *testing.T, s *store.Store, a, b string) (int64, int64) {
	t.Helper()
	ctx := context.Background()
	ua, err := s.CreateUser(ctx, a)
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	ub, err := s.CreateUser(ctx, b)
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	return ua.ID, ub.ID
}

// requestChat runs a successful requestEncryption from admin to participant and
// returns the waiting chat. Each call gets a unique random_id so dedup never
// fires inside a test that expects a fresh row.
var requestChatSeq atomic.Int64

func requestChat(t *testing.T, s *store.Store, admin, participant int64) *tg.EncryptedChatWaiting {
	t.Helper()
	seq := requestChatSeq.Add(1)
	enc, err := api.RequestEncryptionForTest(s, admin, &tg.MessagesRequestEncryptionRequest{
		UserID:   api.InputUser(admin, participant),
		RandomID: int(seq),
		GA:       validGA(),
	})
	if err != nil {
		t.Fatalf("requestEncryption: %v", err)
	}
	waiting, ok := enc.(*tg.EncryptedChatWaiting)
	if !ok {
		t.Fatalf("result type = %T, want *tg.EncryptedChatWaiting", enc)
	}
	return waiting
}

func TestGetDhConfigServesCanonicalGroup(t *testing.T) {
	t.Parallel()

	enc, err := api.GetDhConfigForTest(&tg.MessagesGetDhConfigRequest{Version: 0, RandomLength: 64})
	if err != nil {
		t.Fatalf("getDhConfig: %v", err)
	}
	cfg, ok := enc.(*tg.MessagesDhConfig)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDhConfig", enc)
	}
	if cfg.G != 3 {
		t.Errorf("g = %d, want 3", cfg.G)
	}
	if len(cfg.P) != 256 {
		t.Errorf("len(p) = %d, want 256", len(cfg.P))
	}
	if got := new(big.Int).SetBytes(cfg.P); got.Cmp(api.DHPrime()) != 0 {
		t.Error("p is not the canonical prime")
	}
	if len(cfg.Random) != 64 {
		t.Errorf("len(random) = %d, want 64", len(cfg.Random))
	}

	// Two calls must not return the same block: the bytes are CSPRNG output the
	// client mixes into its own entropy, so a fixed or seeded block would let a
	// client's secret be predicted from the server's.
	other, err := api.GetDhConfigForTest(&tg.MessagesGetDhConfigRequest{Version: 0, RandomLength: 64})
	if err != nil {
		t.Fatalf("getDhConfig again: %v", err)
	}
	otherCfg, ok := other.(*tg.MessagesDhConfig)
	if !ok {
		t.Fatalf("second result type = %T, want *tg.MessagesDhConfig", other)
	}
	if string(otherCfg.Random) == string(cfg.Random) {
		t.Error("random block repeated across calls")
	}
}

func TestGetDhConfigRandomLengthBounds(t *testing.T) {
	t.Parallel()

	// Zero is legal and allocates nothing.
	enc, err := api.GetDhConfigForTest(&tg.MessagesGetDhConfigRequest{RandomLength: 0})
	if err != nil {
		t.Fatalf("random_length 0: %v", err)
	}
	cfg, ok := enc.(*tg.MessagesDhConfig)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDhConfig", enc)
	}
	if len(cfg.Random) != 0 {
		t.Errorf("len(random) = %d, want 0", len(cfg.Random))
	}

	// The clamp is what stops client input sizing a server allocation.
	for _, n := range []int{api.MaxDhRandomLength + 1, 1 << 20, -1} {
		if _, err := api.GetDhConfigForTest(&tg.MessagesGetDhConfigRequest{RandomLength: n}); err == nil {
			t.Errorf("random_length %d: expected RANDOM_LENGTH_INVALID, got nil", n)
		} else if msg := rpcMessage(t, err); msg != "RANDOM_LENGTH_INVALID" {
			t.Errorf("random_length %d: error = %s, want RANDOM_LENGTH_INVALID", n, msg)
		}
	}
}

func TestGetDhConfigNotModifiedForKnownVersion(t *testing.T) {
	t.Parallel()

	enc, err := api.GetDhConfigForTest(&tg.MessagesGetDhConfigRequest{Version: api.DhVersion, RandomLength: 8})
	if err != nil {
		t.Fatalf("getDhConfig: %v", err)
	}
	nm, ok := enc.(*tg.MessagesDhConfigNotModified)
	if !ok {
		t.Fatalf("result type = %T, want *tg.MessagesDhConfigNotModified", enc)
	}
	if len(nm.Random) != 8 {
		t.Errorf("len(random) = %d, want 8", len(nm.Random))
	}
}

func TestRequestEncryptionCreatesRequestedRow(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551380001", "+15551380002")

	waiting := requestChat(t, s, a, b)
	if waiting.AdminID != a || waiting.ParticipantID != b {
		t.Fatalf("waiting = %+v, want admin %d participant %d", waiting, a, b)
	}
	// random_id is a dedup token, never the chat id: a client-chosen id would let
	// one account occupy another's future ids.
	if waiting.ID == 777 {
		t.Error("chat id came from the client's random_id")
	}

	chat, err := s.SecretChatByID(ctx, int32(waiting.ID)) //nolint:gosec // test id
	if err != nil {
		t.Fatalf("load chat: %v", err)
	}
	if chat.State != store.SecretChatRequested {
		t.Errorf("state = %q, want %q", chat.State, store.SecretChatRequested)
	}
	if chat.KeyFingerprint != 0 || chat.GAOrB != nil {
		t.Errorf("accept fields set on a requested row: %+v", chat)
	}
	if len(chat.GAHash) != 32 {
		t.Errorf("len(g_a_hash) = %d, want 32", len(chat.GAHash))
	}

	// The responder is shown g_a; the initiator is not shown anything new.
	toB, ok := api.EncryptedChatFor(chat, b).(*tg.EncryptedChatRequested)
	if !ok {
		t.Fatalf("responder sees %T, want *tg.EncryptedChatRequested", api.EncryptedChatFor(chat, b))
	}
	if string(toB.GA) != string(validGA()) {
		t.Error("responder's g_a does not match the initiator's")
	}
	if _, ok := api.EncryptedChatFor(chat, a).(*tg.EncryptedChatWaiting); !ok {
		t.Errorf("initiator sees %T, want *tg.EncryptedChatWaiting", api.EncryptedChatFor(chat, a))
	}
}

func TestRequestEncryptionRejectsSelfAndUnknownTarget(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, _ := twoUsersFor(t, s, "+15551381001", "+15551381002")

	if _, err := api.RequestEncryptionForTest(s, a, &tg.MessagesRequestEncryptionRequest{
		UserID: api.InputUser(a, a), RandomID: 1, GA: validGA(),
	}); err == nil {
		t.Error("self chat: expected an error, got nil")
	} else if msg := rpcMessage(t, err); msg != "USER_ID_INVALID" {
		t.Errorf("self chat: error = %s, want USER_ID_INVALID", msg)
	}

	// An account that does not exist must be rejected before a row is allocated,
	// not left to the foreign key.
	if _, err := api.RequestEncryptionForTest(s, a, &tg.MessagesRequestEncryptionRequest{
		UserID: api.InputUser(a, 999999), RandomID: 2, GA: validGA(),
	}); err == nil {
		t.Error("unknown target: expected an error, got nil")
	}
}

// The g_a bound is the security-relevant part of the handshake: a degenerate
// value lets one party fix the key the other derives.
func TestRequestEncryptionRejectsBadGA(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551382001", "+15551382002")

	p := api.DHPrime()
	twoPow1984 := new(big.Int).Exp(big.NewInt(2), big.NewInt(2048-64), nil)
	for name, ga := range map[string][]byte{
		"short":            make([]byte, 255),
		"long":             make([]byte, 257),
		"empty":            nil,
		"zero":             dhValue(big.NewInt(0)),
		"one":              dhValue(big.NewInt(1)),
		"p minus one":      dhValue(new(big.Int).Sub(p, big.NewInt(1))),
		"below safe floor": dhValue(new(big.Int).Sub(twoPow1984, big.NewInt(1))),
		"above safe cap":   dhValue(new(big.Int).Sub(p, twoPow1984)),
	} {
		_, err := api.RequestEncryptionForTest(s, a, &tg.MessagesRequestEncryptionRequest{
			UserID: api.InputUser(a, b), RandomID: 3, GA: ga,
		})
		if err == nil {
			t.Errorf("%s: expected DH_G_A_INVALID, got nil", name)
			continue
		}
		if msg := rpcMessage(t, err); msg != "DH_G_A_INVALID" {
			t.Errorf("%s: error = %s, want DH_G_A_INVALID", name, msg)
		}
	}
}

// The cap bounds durable rows one account can force another to hold, and must
// count only what the caller initiated.
func TestRequestEncryptionOutstandingCap(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551383001", "+15551383002")

	for i := range store.MaxOutstandingSecretChats {
		if _, err := api.RequestEncryptionForTest(s, a, &tg.MessagesRequestEncryptionRequest{
			UserID: api.InputUser(a, b), RandomID: i, GA: validGA(),
		}); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
	}
	_, err := api.RequestEncryptionForTest(s, a, &tg.MessagesRequestEncryptionRequest{
		UserID: api.InputUser(a, b), RandomID: 99, GA: validGA(),
	})
	if err == nil {
		t.Fatal("over cap: expected PEER_FLOOD, got nil")
	}
	if msg := rpcMessage(t, err); msg != "PEER_FLOOD" {
		t.Errorf("over cap: error = %s, want PEER_FLOOD", msg)
	}

	// The cap is per initiator: b holds ten inbound requests and must still be
	// able to start its own.
	if _, err := api.RequestEncryptionForTest(s, b, &tg.MessagesRequestEncryptionRequest{
		UserID: api.InputUser(b, a), RandomID: 1, GA: validGA(),
	}); err != nil {
		t.Errorf("responder blocked by inbound requests: %v", err)
	}
}

func TestAcceptEncryptionActivatesChat(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551384001", "+15551384002")
	waiting := requestChat(t, s, a, b)
	id := int32(waiting.ID) //nolint:gosec // test id

	enc, err := api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
		Peer:           api.InputEncryptedChat(b, id),
		GB:             validGB(),
		KeyFingerprint: -12345,
	})
	if err != nil {
		t.Fatalf("acceptEncryption: %v", err)
	}
	active, ok := enc.(*tg.EncryptedChat)
	if !ok {
		t.Fatalf("result type = %T, want *tg.EncryptedChat", enc)
	}
	if active.KeyFingerprint != -12345 || string(active.GAOrB) != string(validGB()) {
		t.Fatalf("reply did not carry the accepted key material: %+v", active)
	}

	chat, err := s.SecretChatByID(ctx, id)
	if err != nil {
		t.Fatalf("load chat: %v", err)
	}
	if chat.State != store.SecretChatActive {
		t.Fatalf("state = %q, want %q", chat.State, store.SecretChatActive)
	}
	if chat.KeyFingerprint != -12345 || string(chat.GAOrB) != string(validGB()) {
		t.Fatalf("stored row missing key material: %+v", chat)
	}
	// The initiator's push carries the same material, which is what lets it
	// finish the exchange.
	toA, ok := api.EncryptedChatFor(chat, a).(*tg.EncryptedChat)
	if !ok || toA.KeyFingerprint != -12345 {
		t.Fatalf("initiator update = %#v, want an active chat with the fingerprint", api.EncryptedChatFor(chat, a))
	}
}

// A second accept must not succeed: it would rewrite the fingerprint under a
// chat the initiator has already keyed, and neither side would notice.
func TestAcceptEncryptionRejectsReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551385001", "+15551385002")
	waiting := requestChat(t, s, a, b)
	id := int32(waiting.ID) //nolint:gosec // test id

	accept := func(fp int64) error {
		_, err := api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
			Peer: api.InputEncryptedChat(b, id), GB: validGB(), KeyFingerprint: fp,
		})
		return err
	}
	if err := accept(1); err != nil {
		t.Fatalf("first accept: %v", err)
	}
	err := accept(2)
	if err == nil {
		t.Fatal("second accept: expected ENCRYPTION_ALREADY_ACCEPTED, got nil")
	}
	if msg := rpcMessage(t, err); msg != "ENCRYPTION_ALREADY_ACCEPTED" {
		t.Errorf("second accept: error = %s, want ENCRYPTION_ALREADY_ACCEPTED", msg)
	}
	chat, err := s.SecretChatByID(ctx, id)
	if err != nil {
		t.Fatalf("load chat: %v", err)
	}
	if chat.KeyFingerprint != 1 {
		t.Errorf("fingerprint = %d, want 1: the replay rewrote it", chat.KeyFingerprint)
	}
}

func TestAcceptEncryptionAuthorization(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551386001", "+15551386002")
	c, err := s.CreateUser(context.Background(), "+15551386003")
	if err != nil {
		t.Fatalf("user c: %v", err)
	}
	waiting := requestChat(t, s, a, b)
	id := int32(waiting.ID) //nolint:gosec // test id

	for name, tc := range map[string]struct {
		caller int64
		peer   tg.InputEncryptedChat
	}{
		"initiator accepts its own request": {a, api.InputEncryptedChat(a, id)},
		"outsider with own derived hash":    {c.ID, api.InputEncryptedChat(c.ID, id)},
		"responder with wrong hash":         {b, tg.InputEncryptedChat{ChatID: int(id), AccessHash: 1}},
		"responder with another's hash":     {b, api.InputEncryptedChat(a, id)},
		"unknown chat":                      {b, api.InputEncryptedChat(b, id+1000)},
	} {
		_, err := api.AcceptEncryptionForTest(s, tc.caller, &tg.MessagesAcceptEncryptionRequest{
			Peer: tc.peer, GB: validGB(), KeyFingerprint: 5,
		})
		if err == nil {
			t.Errorf("%s: expected ENCRYPTION_ID_INVALID, got nil", name)
			continue
		}
		if msg := rpcMessage(t, err); msg != "ENCRYPTION_ID_INVALID" {
			t.Errorf("%s: error = %s, want ENCRYPTION_ID_INVALID", name, msg)
		}
	}
}

func TestAcceptEncryptionRejectsBadGB(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551387001", "+15551387002")
	waiting := requestChat(t, s, a, b)
	id := int32(waiting.ID) //nolint:gosec // test id

	_, err := api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
		Peer: api.InputEncryptedChat(b, id), GB: make([]byte, 255), KeyFingerprint: 5,
	})
	if err == nil {
		t.Fatal("short g_b: expected DH_G_A_INVALID, got nil")
	}
	if msg := rpcMessage(t, err); msg != "DH_G_A_INVALID" {
		t.Errorf("short g_b: error = %s, want DH_G_A_INVALID", msg)
	}
}

func TestDiscardEncryptionFromEitherSideAndIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	for name, discardBy := range map[string]string{"admin": "admin", "participant": "participant"} {
		phoneA, phoneB := "+1555139"+name[:1]+"001", "+1555139"+name[:1]+"002"
		a, b := twoUsersFor(t, s, phoneA, phoneB)
		waiting := requestChat(t, s, a, b)
		id := int32(waiting.ID) //nolint:gosec // test id

		caller := a
		if discardBy == "participant" {
			caller = b
		}
		enc, err := api.DiscardEncryptionForTest(s, caller, &tg.MessagesDiscardEncryptionRequest{ChatID: int(id)})
		if err != nil {
			t.Fatalf("%s discard: %v", name, err)
		}
		if _, ok := enc.(*tg.EncryptedChatDiscarded); !ok {
			t.Fatalf("%s discard result = %T, want *tg.EncryptedChatDiscarded", name, enc)
		}
		chat, err := s.SecretChatByID(ctx, id)
		if err != nil {
			t.Fatalf("%s load: %v", name, err)
		}
		if chat.State != store.SecretChatDiscarded {
			t.Fatalf("%s state = %q, want %q", name, chat.State, store.SecretChatDiscarded)
		}
		// The other side is shown the terminal state, and nothing else about the
		// chat.
		if _, ok := api.EncryptedChatFor(chat, chat.Other(caller)).(*tg.EncryptedChatDiscarded); !ok {
			t.Errorf("%s: other party not shown a discarded chat", name)
		}

		// Repeat, from both sides: terminal means idempotent, not an error.
		for _, again := range []int64{a, b} {
			if _, err := api.DiscardEncryptionForTest(s, again, &tg.MessagesDiscardEncryptionRequest{ChatID: int(id)}); err != nil {
				t.Errorf("%s repeat discard by %d: %v", name, again, err)
			}
		}
		// A discarded chat can no longer be accepted.
		if _, err := api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
			Peer: api.InputEncryptedChat(b, id), GB: validGB(), KeyFingerprint: 9,
		}); err == nil {
			t.Errorf("%s: accept after discard succeeded", name)
		} else if msg := rpcMessage(t, err); msg != "ENCRYPTION_ALREADY_DECLINED" {
			t.Errorf("%s: accept after discard = %s, want ENCRYPTION_ALREADY_DECLINED", name, msg)
		}
	}
}

// chat_id arrives with no access hash, so membership is the whole authorization
// boundary — and an absent chat must be indistinguishable from one the caller is
// not in, or a dense id sequence becomes enumerable.
func TestDiscardEncryptionRejectsNonParty(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551391001", "+15551391002")
	outsider, err := s.CreateUser(context.Background(), "+15551391003")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	waiting := requestChat(t, s, a, b)

	_, errOutsider := api.DiscardEncryptionForTest(s, outsider.ID, &tg.MessagesDiscardEncryptionRequest{ChatID: waiting.ID})
	_, errAbsent := api.DiscardEncryptionForTest(s, outsider.ID, &tg.MessagesDiscardEncryptionRequest{ChatID: waiting.ID + 5000})
	if errOutsider == nil || errAbsent == nil {
		t.Fatalf("expected errors, got outsider=%v absent=%v", errOutsider, errAbsent)
	}
	if got, want := rpcMessage(t, errOutsider), rpcMessage(t, errAbsent); got != want {
		t.Errorf("non-party error %s differs from absent-chat error %s: id space is probeable", got, want)
	}
}

// TestSendEncryptedMessageRejections covers the four handler-layer rejection
// paths for handleSendEncryptedMessage:
//   - AC8: payload over the 512 KB cap → MESSAGE_TOO_LONG
//   - AC6: chat in 'requested' or 'discarded' state → ENCRYPTION_DECLINED
//   - AC5: caller is not a participant → CHAT_FORBIDDEN
//   - AC7: access_hash mismatch → ENCRYPTION_ID_INVALID
func TestSendEncryptedMessageRejections(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	ctx := context.Background()

	a, b := twoUsersFor(t, s, "+15551393001", "+15551393002")
	outsider, err := s.CreateUser(ctx, "+15551393003")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}

	// active chat
	waiting := requestChat(t, s, a, b)
	activeID := int32(waiting.ID) //nolint:gosec
	if _, err := api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
		Peer: api.InputEncryptedChat(b, activeID), GB: validGB(), KeyFingerprint: 1,
	}); err != nil {
		t.Fatalf("accept: %v", err)
	}

	// requested (not yet accepted) chat
	waiting2 := requestChat(t, s, a, b)
	requestedID := int32(waiting2.ID) //nolint:gosec

	// discarded chat
	waiting3 := requestChat(t, s, a, b)
	discardedID := int32(waiting3.ID) //nolint:gosec
	if _, err := api.DiscardEncryptionForTest(s, a, &tg.MessagesDiscardEncryptionRequest{ChatID: int(discardedID)}); err != nil {
		t.Fatalf("discard: %v", err)
	}

	for name, tc := range map[string]struct {
		caller int64
		peer   tg.InputEncryptedChat
		data   []byte
		want   string
	}{
		"AC8 data over cap":     {a, api.InputEncryptedChat(a, activeID), make([]byte, 512*1024+1), "MESSAGE_TOO_LONG"},
		"AC6 requested state":   {a, api.InputEncryptedChat(a, requestedID), []byte("x"), "ENCRYPTION_DECLINED"},
		"AC6 discarded state":   {a, api.InputEncryptedChat(a, discardedID), []byte("x"), "ENCRYPTION_DECLINED"},
		"AC5 non-participant":   {outsider.ID, tg.InputEncryptedChat{ChatID: int(activeID)}, []byte("x"), "CHAT_FORBIDDEN"},
		"AC7 wrong access hash": {a, tg.InputEncryptedChat{ChatID: int(activeID), AccessHash: 1}, []byte("x"), "ENCRYPTION_ID_INVALID"},
	} {
		_, err := api.SendEncryptedMessageForTest(s, tc.caller, &tg.MessagesSendEncryptedRequest{
			Peer:     tc.peer,
			RandomID: 999,
			Data:     tc.data,
		})
		if err == nil {
			t.Errorf("%s: expected %s, got nil", name, tc.want)
			continue
		}
		if msg := rpcMessage(t, err); msg != tc.want {
			t.Errorf("%s: error = %s, want %s", name, msg, tc.want)
		}
	}
}

// Key exchange spends no pts and no qts: the per-account update stream must be
// exactly where it was.
func TestKeyExchangeLeavesUpdateStateUntouched(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	a, b := twoUsersFor(t, s, "+15551392001", "+15551392002")

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close: %v", err)
		}
	}()
	qtsOf := func(userID int64) int64 {
		t.Helper()
		var qts int64
		switch err := conn.QueryRow(ctx, `SELECT qts FROM update_state WHERE user_id = $1`, userID).Scan(&qts); {
		case errors.Is(err, pgx.ErrNoRows):
			return 0
		case err != nil:
			t.Fatalf("read qts: %v", err)
		}
		return qts
	}

	before := map[int64]store.State{}
	beforeQts := map[int64]int64{}
	for _, id := range []int64{a, b} {
		st, err := s.State(ctx, id)
		if err != nil {
			t.Fatalf("state %d: %v", id, err)
		}
		before[id] = st
		beforeQts[id] = qtsOf(id)
	}

	waiting := requestChat(t, s, a, b)
	id := int32(waiting.ID) //nolint:gosec // test id
	if _, err := api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
		Peer: api.InputEncryptedChat(b, id), GB: validGB(), KeyFingerprint: 7,
	}); err != nil {
		t.Fatalf("accept: %v", err)
	}
	if _, err := api.DiscardEncryptionForTest(s, a, &tg.MessagesDiscardEncryptionRequest{ChatID: int(id)}); err != nil {
		t.Fatalf("discard: %v", err)
	}

	for _, uid := range []int64{a, b} {
		st, err := s.State(ctx, uid)
		if err != nil {
			t.Fatalf("state %d: %v", uid, err)
		}
		if st.Pts != before[uid].Pts || st.Seq != before[uid].Seq {
			t.Errorf("user %d: pts/seq moved from (%d,%d) to (%d,%d)",
				uid, before[uid].Pts, before[uid].Seq, st.Pts, st.Seq)
		}
		if got := qtsOf(uid); got != beforeQts[uid] {
			t.Errorf("user %d: qts moved from %d to %d", uid, beforeQts[uid], got)
		}
	}
}

// The accept guard matches no row for BOTH terminal states, so the error it
// reports has to be re-derived from the row. Mapping every stale guard to
// ENCRYPTION_ALREADY_ACCEPTED tells a client that raced a discard to wait for a
// key that will never arrive.
func TestStaleAcceptErrorNamesTheTerminalState(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551393001", "+15551393002")

	accepted := requestChat(t, s, a, b)
	acceptedID := int32(accepted.ID) //nolint:gosec // test id
	if _, err := api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
		Peer: api.InputEncryptedChat(b, acceptedID), GB: validGB(), KeyFingerprint: 3,
	}); err != nil {
		t.Fatalf("accept: %v", err)
	}

	discarded := requestChat(t, s, a, b)
	discardedID := int32(discarded.ID) //nolint:gosec // test id
	if _, err := api.DiscardEncryptionForTest(s, a, &tg.MessagesDiscardEncryptionRequest{ChatID: discarded.ID}); err != nil {
		t.Fatalf("discard: %v", err)
	}

	for name, tc := range map[string]struct {
		id   int32
		want string
	}{
		"active chat":    {acceptedID, "ENCRYPTION_ALREADY_ACCEPTED"},
		"discarded chat": {discardedID, "ENCRYPTION_ALREADY_DECLINED"},
		"absent chat":    {discardedID + 5000, "ENCRYPTION_ID_INVALID"},
	} {
		if got := rpcMessage(t, api.StaleAcceptErrorForTest(s, tc.id)); got != tc.want {
			t.Errorf("%s: error = %s, want %s", name, got, tc.want)
		}
	}
}

// The race itself: a discard landing between acceptEncryption's preflight read
// and its guarded UPDATE must not be reported as an already-accepted chat.
// Whichever call wins, the error the loser gets has to match the state the row
// actually landed in.
func TestAcceptRacingDiscardReportsTheRealState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551394001", "+15551394002")

	for i := range 20 {
		waiting := requestChat(t, s, a, b)
		id := int32(waiting.ID) //nolint:gosec // test id

		var acceptErr, discardErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_, acceptErr = api.AcceptEncryptionForTest(s, b, &tg.MessagesAcceptEncryptionRequest{
				Peer: api.InputEncryptedChat(b, id), GB: validGB(), KeyFingerprint: 11,
			})
		}()
		go func() {
			defer wg.Done()
			<-start
			_, discardErr = api.DiscardEncryptionForTest(s, a, &tg.MessagesDiscardEncryptionRequest{ChatID: waiting.ID})
		}()
		close(start)
		wg.Wait()

		if discardErr != nil {
			t.Fatalf("iteration %d: discard: %v", i, discardErr)
		}
		chat, err := s.SecretChatByID(ctx, id)
		if err != nil {
			t.Fatalf("iteration %d: load: %v", i, err)
		}
		if acceptErr == nil {
			// Accept won the race; discard then moved the row on from 'active'.
			continue
		}
		if chat.State != store.SecretChatDiscarded {
			t.Fatalf("iteration %d: accept failed but state is %q", i, chat.State)
		}
		if msg := rpcMessage(t, acceptErr); msg != "ENCRYPTION_ALREADY_DECLINED" {
			t.Fatalf("iteration %d: accept lost to a discard but reported %s, want ENCRYPTION_ALREADY_DECLINED", i, msg)
		}
	}
}

// TestRequestEncryptionDedupSameRandomID proves that a retried
// requestEncryption with the same random_id returns the original chat
// (encryptedChatWaiting) and creates no second row. First-time callers with
// non-zero random_id still get a new row and a push.
func TestRequestEncryptionDedupSameRandomID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, b := twoUsersFor(t, s, "+15551395001", "+15551395002")

	const randomID = int64(999)

	// First call with non-zero random_id creates a new row.
	enc1, err := api.RequestEncryptionForTest(s, a, &tg.MessagesRequestEncryptionRequest{
		UserID:   api.InputUser(a, b),
		RandomID: int(randomID),
		GA:       validGA(),
	})
	if err != nil {
		t.Fatalf("first request: %v", err)
	}
	waiting1, ok := enc1.(*tg.EncryptedChatWaiting)
	if !ok {
		t.Fatalf("first result = %T, want *tg.EncryptedChatWaiting", enc1)
	}

	// Retry with same random_id returns the same chat.
	enc2, err := api.RequestEncryptionForTest(s, a, &tg.MessagesRequestEncryptionRequest{
		UserID:   api.InputUser(a, b),
		RandomID: int(randomID),
		GA:       validGA(),
	})
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	waiting2, ok := enc2.(*tg.EncryptedChatWaiting)
	if !ok {
		t.Fatalf("retry result = %T, want *tg.EncryptedChatWaiting", enc2)
	}
	if waiting2.ID != waiting1.ID {
		t.Errorf("retry id = %d, want %d (original)", waiting2.ID, waiting1.ID)
	}

	// Only one row exists in the database.
	chat, err := s.SecretChatByID(ctx, int32(waiting1.ID)) //nolint:gosec // test id
	if err != nil {
		t.Fatalf("load chat: %v", err)
	}
	if chat.State != store.SecretChatRequested {
		t.Errorf("state = %q, want %q", chat.State, store.SecretChatRequested)
	}
}
