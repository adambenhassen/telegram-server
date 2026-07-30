package e2e_test

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/dcs"
	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestListenerReconnectResumesDelivery terminates the delivery listener's
// Postgres backend under a running server and proves a later message still
// reaches the recipient's socket in real time, on the same process: one dropped
// connection must not end push for the life of the replica.
func TestListenerReconnectResumesDelivery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	port := tcpPort(t, ln)
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	newClient := func(collector *updateCollector) *telegram.Client {
		return telegram.NewClient(1, "hash", telegram.Options{
			DC:            dcID,
			DCList:        dcs.List{Options: []tg.DCOption{{ID: dcID, IPAddress: "127.0.0.1", Port: port}}},
			PublicKeys:    []telegram.PublicKey{{RSA: &key.PublicKey}},
			Resolver:      dcs.Plain(dcs.PlainOptions{}),
			UpdateHandler: collector,
		})
	}
	flowFor := func(phone string) auth.Flow {
		return auth.NewFlow(
			auth.Constant(phone, "", auth.CodeAuthenticatorFunc(
				func(ctx context.Context, _ *tg.AuthSentCode) (string, error) {
					return codes.wait(ctx, phone)
				})),
			auth.SendCodeOptions{},
		)
	}

	collA, collB := newUpdateCollector(), newUpdateCollector()
	clientA, clientB := newClient(collA), newClient(collB)
	const phoneA, phoneB = "+15551280051", "+15551280052"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() { errA <- runInteractive(ctx, clientA, flowFor(phoneA), aID, aCmds) }()
	go func() { errB <- runInteractive(ctx, clientB, flowFor(phoneB), bID, bCmds) }()

	var aUserID, bUserID int64
	select {
	case aUserID = <-aID:
	case <-time.After(30 * time.Second):
		t.Fatal("client A login timeout")
	}
	select {
	case bUserID = <-bID:
	case <-time.After(30 * time.Second):
		t.Fatal("client B login timeout")
	}

	exec := func(cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
		d := make(chan error, 1)
		select {
		case cmds <- command{fn: fn, done: d}:
		case <-time.After(10 * time.Second):
			t.Fatal("command enqueue timeout")
		}
		return <-d
	}
	peerB := peerUser(aUserID, bUserID)
	sendToB := func(text string, randomID int64) {
		if err := exec(aCmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
				Peer: peerB, Message: text, RandomID: randomID,
			})
			return err
		}); err != nil {
			t.Fatalf("A send %q: %v", text, err)
		}
	}

	// Baseline: push works before the connection is cut.
	sendToB("before termination", 1)
	if got := recvOr(t, collB.newMsg, "B updateNewMessage before termination"); got.Message != "before termination" {
		t.Fatalf("B received %q, want %q", got.Message, "before termination")
	}

	terminateListenBackends(t, dsn)

	// A notification emitted while the listener is reconnecting is dropped, so
	// resend until one lands rather than asserting on a single send. Whichever
	// message arrives, its arrival is the proof: push resumed with no restart.
	deadline := time.Now().Add(60 * time.Second)
	var delivered *tg.Message
	for i := 2; delivered == nil; i++ {
		if time.Now().After(deadline) {
			t.Fatal("no live delivery after the listener backend was terminated")
		}
		sendToB("after termination "+strconv.Itoa(i), int64(i))
		select {
		case delivered = <-collB.newMsg:
		case <-time.After(2 * time.Second):
		}
	}
	if !strings.HasPrefix(delivered.Message, "after termination") {
		t.Fatalf("B received %q after termination", delivered.Message)
	}

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}

// terminateListenBackends kills every Postgres backend holding a LISTEN on this
// test's database: the server-side equivalent of the listener's connection
// dropping under a live process.
func terminateListenBackends(t *testing.T, dsn string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	rows, err := conn.Query(ctx, `SELECT pid FROM pg_stat_activity
		WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE 'LISTEN %'`)
	if err != nil {
		t.Fatalf("query listen backends: %v", err)
	}
	pids, err := pgx.CollectRows(rows, pgx.RowTo[int32])
	if err != nil {
		t.Fatalf("collect listen backends: %v", err)
	}
	if len(pids) == 0 {
		t.Fatal("no LISTEN backend found to terminate")
	}
	for _, pid := range pids {
		if _, err := conn.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
			t.Fatalf("terminate backend %d: %v", pid, err)
		}
	}
}
