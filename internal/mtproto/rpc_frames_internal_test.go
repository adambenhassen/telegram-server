package mtproto

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tgerr"
)

type rpcFrameTestRead struct {
	frame []byte
	err   error
}

type rpcFrameTestConn struct {
	reads               chan rpcFrameTestRead
	returned            chan struct{}
	calls               atomic.Int32
	sends               atomic.Int32
	deadline            atomic.Int64
	firstTimeoutRelease chan struct{}
	readCancelled       chan struct{}
}

func newRPCFrameTestConn() *rpcFrameTestConn {
	return &rpcFrameTestConn{
		reads:    make(chan rpcFrameTestRead, 8),
		returned: make(chan struct{}, 8),
	}
}

func (c *rpcFrameTestConn) Recv(ctx context.Context, b *bin.Buffer) error {
	if deadline, ok := ctx.Deadline(); ok {
		c.deadline.Store(deadline.UnixNano())
	}
	call := c.calls.Add(1)
	select {
	case c.returned <- struct{}{}:
	default:
	}
	if call == 1 && c.firstTimeoutRelease != nil {
		<-ctx.Done()
		c.readCancelled <- struct{}{}
		<-c.firstTimeoutRelease
		return rpcFrameTestTimeout{}
	}
	select {
	case read := <-c.reads:
		if read.err != nil {
			return read.err
		}
		b.ResetTo(read.frame)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *rpcFrameTestConn) Send(context.Context, *bin.Buffer) error {
	c.sends.Add(1)
	return nil
}

func (*rpcFrameTestConn) Close() error { return nil }

type rpcFrameTestTimeout struct{}

func (rpcFrameTestTimeout) Error() string   { return "read timeout" }
func (rpcFrameTestTimeout) Timeout() bool   { return true }
func (rpcFrameTestTimeout) Temporary() bool { return true }

func TestRPCFrameReaderCancelsActiveRPCOnPeerEOF(t *testing.T) {
	t.Parallel()

	conn := newRPCFrameTestConn()
	conn.reads <- rpcFrameTestRead{frame: []byte{1, 0, 0, 0, 0, 0, 0, 0}}
	state := newPeerRPCState(newRPCCancelBudget(time.Now, 8))
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, context.Background(), peerReadTransport{Conn: conn}, state, nil)
	defer func() {
		reader.stop()
		reader.join()
	}()
	if _, err, _ := reader.next(); err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	waitForRPCFrameReads(t, conn, 2)
	activeCtx, finish, started := state.begin(19, context.Background())
	if !started {
		t.Fatal("active RPC did not start")
	}
	defer finish()
	conn.reads <- rpcFrameTestRead{err: io.EOF}
	if _, err, _ := reader.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("peer EOF = %v, want EOF", err)
	}
	if got := context.Cause(activeCtx); got != errPeerRPCDisconnected {
		t.Fatalf("active RPC cause = %v, want peer disconnect", got)
	}
}

func TestRPCFrameReaderTimeoutDoesNotCancelActiveRPC(t *testing.T) {
	t.Parallel()

	conn := newRPCFrameTestConn()
	conn.reads <- rpcFrameTestRead{frame: []byte{1, 0, 0, 0, 0, 0, 0, 0}}
	state := newPeerRPCState(newRPCCancelBudget(time.Now, 8))
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, context.Background(), peerReadTransport{Conn: conn}, state, nil)
	defer func() {
		reader.stop()
		reader.join()
	}()
	if _, err, _ := reader.next(); err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	waitForRPCFrameReads(t, conn, 2)
	activeCtx, finish, started := state.begin(20, context.Background())
	if !started {
		t.Fatal("active RPC did not start")
	}
	finished := false
	defer func() {
		if !finished {
			finish()
		}
	}()
	conn.reads <- rpcFrameTestRead{err: rpcFrameTestTimeout{}}
	waitForRPCFrameReads(t, conn, 3)
	if got := context.Cause(activeCtx); got != nil {
		t.Fatalf("read timeout canceled active RPC with %v", got)
	}
	if got := len(state.budget.users); got != 0 {
		t.Fatalf("read timeout consumed %d user allowances", got)
	}
	finish()
	finished = true
	conn.reads <- rpcFrameTestRead{err: io.EOF}
	if _, err, _ := reader.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("peer EOF after read timeout = %v, want EOF", err)
	}
}

func TestRPCFrameReaderRestartsIdleTimeoutAfterRPCCompletion(t *testing.T) {
	t.Parallel()

	const readTimeout = time.Second
	conn := newRPCFrameTestConn()
	conn.reads <- rpcFrameTestRead{frame: []byte{1, 0, 0, 0, 0, 0, 0, 0}}
	state := newPeerRPCState(newRPCCancelBudget(time.Now, 8))
	reader := newRPCFrameReader(&Server{readTimeout: readTimeout}, context.Background(), peerReadTransport{Conn: conn}, state, nil)
	defer func() {
		reader.stop()
		reader.join()
	}()
	if _, err, _ := reader.next(); err != nil {
		t.Fatalf("read first frame: %v", err)
	}
	waitForRPCFrameReads(t, conn, 2)
	_, finish, started := state.begin(21, context.Background())
	if !started {
		t.Fatal("active RPC did not start")
	}
	waitForRPCFrameReads(t, conn, 3)
	finishedAt := time.Now()
	finish()
	waitForRPCFrameReads(t, conn, 4)
	deadline := time.Unix(0, conn.deadline.Load())
	if deadline.Before(finishedAt.Add(readTimeout - 10*time.Millisecond)) {
		t.Fatalf("idle read deadline = %s, want a fresh %s after RPC completion %s", deadline, readTimeout, finishedAt)
	}
}

func TestRPCFrameReaderRestartsWhenTimeoutRacesRPCCompletion(t *testing.T) {
	t.Parallel()

	conn := newRPCFrameTestConn()
	conn.firstTimeoutRelease = make(chan struct{})
	conn.readCancelled = make(chan struct{}, 1)
	state := newPeerRPCState(newRPCCancelBudget(time.Now, 8))
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, context.Background(), peerReadTransport{Conn: conn}, state, nil)
	defer func() {
		reader.stop()
		reader.join()
	}()
	waitForRPCFrameReads(t, conn, 1)
	_, finish, started := state.begin(22, context.Background())
	if !started {
		t.Fatal("active RPC did not start")
	}
	select {
	case <-conn.readCancelled:
	case <-time.After(time.Second):
		t.Fatal("RPC start did not cancel the in-progress idle read")
	}
	finish()
	close(conn.firstTimeoutRelease)
	waitForRPCFrameReads(t, conn, 3)
	conn.reads <- rpcFrameTestRead{err: io.EOF}
	if _, err, _ := reader.next(); !errors.Is(err, io.EOF) {
		t.Fatalf("peer EOF after raced timeout = %v, want EOF", err)
	}
}

func TestRPCFrameReaderInterruptsTransportReadOnRPCStateChange(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	conn := &rpcFrameNetTestConn{rpcFrameTestConn: newRPCFrameTestConn(), Conn: serverConn}
	state := newPeerRPCState(newRPCCancelBudget(time.Now, 8))
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, context.Background(), peerReadTransport{
		Conn:     conn,
		readConn: serverConn,
	}, state, nil)
	defer func() {
		reader.stop()
		_ = clientConn.Close()
		_ = serverConn.Close()
		reader.join()
	}()
	waitForRPCFrameReads(t, conn.rpcFrameTestConn, 1)
	_, finish, started := state.begin(23, context.Background())
	if !started {
		t.Fatal("active RPC did not start")
	}
	waitForRPCFrameReads(t, conn.rpcFrameTestConn, 2)
	if _, err := clientConn.Write([]byte{42}); err != nil {
		t.Fatalf("write test frame: %v", err)
	}
	frame, err, _ := reader.next()
	if err != nil {
		t.Fatalf("read frame after interrupting idle read: %v", err)
	}
	if len(frame.Buf) != 1 || frame.Buf[0] != 42 {
		t.Fatalf("frame after interrupting idle read = %v, want [42]", frame.Buf)
	}
	finish()
}

type rpcFrameNetTestConn struct {
	*rpcFrameTestConn
	net.Conn
}

func (c *rpcFrameNetTestConn) Recv(ctx context.Context, b *bin.Buffer) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.Conn.SetReadDeadline(deadline); err != nil {
			return err
		}
	}
	c.calls.Add(1)
	select {
	case c.returned <- struct{}{}:
	default:
	}
	var frame [1]byte
	if _, err := c.Conn.Read(frame[:]); err != nil {
		return err
	}
	b.ResetTo(frame[:])
	return nil
}

func (c *rpcFrameNetTestConn) Close() error { return c.Conn.Close() }

func TestRPCFrameReaderBoundsReadAheadAndPreservesOrder(t *testing.T) {
	t.Parallel()

	conn := newRPCFrameTestConn()
	for _, frame := range [][]byte{{1}, {2}, {3}} {
		conn.reads <- rpcFrameTestRead{frame: frame}
	}
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, context.Background(), peerReadTransport{Conn: conn}, newPeerRPCState(nil), nil)
	defer func() {
		reader.stop()
		reader.join()
	}()
	for i := int32(1); i <= 3; i++ {
		frame, err, _ := reader.next()
		if err != nil {
			t.Fatalf("read frame %d: %v", i, err)
		}
		if len(frame.Buf) != 1 || frame.Buf[0] != byte(i) {
			t.Fatalf("frame %d = %v, want [%d]", i, frame.Buf, i)
		}
		if i == 1 {
			waitForRPCFrameReads(t, conn, 2)
			if got := conn.calls.Load(); got != 2 {
				t.Fatalf("read-ahead calls while second frame is buffered = %d, want 2", got)
			}
		}
	}
}

func TestRPCFrameReaderPausesTransportReadsDuringExchange(t *testing.T) {
	t.Parallel()

	conn := newRPCFrameTestConn()
	conn.reads <- rpcFrameTestRead{frame: make([]byte, 8)}
	conn.reads <- rpcFrameTestRead{frame: []byte{1, 0, 0, 0, 0, 0, 0, 0}}
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, context.Background(), peerReadTransport{Conn: conn}, newPeerRPCState(nil), nil)
	defer func() {
		reader.stop()
		reader.join()
	}()
	if _, err, exchange := reader.next(); err != nil || !exchange {
		t.Fatalf("exchange frame result = err %v, exchange %t", err, exchange)
	}
	select {
	case <-conn.returned:
	case <-time.After(time.Second):
		t.Fatal("reader did not perform the handshake frame read")
	}
	time.Sleep(20 * time.Millisecond)
	if got := conn.calls.Load(); got != 1 {
		t.Fatalf("transport reads during key exchange = %d, want one", got)
	}
	reader.resumeExchange()
	if _, err, _ := reader.next(); err != nil {
		t.Fatalf("read frame after exchange: %v", err)
	}
}

func TestRPCReplySkipsWriteAfterPeerClose(t *testing.T) {
	t.Parallel()

	transport := newRPCFrameTestConn()
	state := newPeerRPCState(nil)
	state.peerDisconnected()
	conn := &Conn{transport: transport}
	conn.peerRPC.Store(state)
	req := &Request{Ctx: context.Background(), peerRPC: state}
	if err := conn.SendResult(req, rpcFrameTestEncoder{}); !errors.Is(err, errPeerRPCReplySkipped) {
		t.Fatalf("reply after peer close = %v, want skipped sentinel", err)
	}
	if got := transport.sends.Load(); got != 0 {
		t.Fatalf("transport writes after peer close = %d, want zero", got)
	}
}

func TestRPCErrorReplySkipsConnectionFailureAfterPeerClose(t *testing.T) {
	t.Parallel()

	transport := newRPCFrameTestConn()
	state := newPeerRPCState(nil)
	state.peerDisconnected()
	conn := &Conn{transport: transport}
	conn.peerRPC.Store(state)
	server := &Server{handler: HandlerFunc(func(*Conn, *Request) error {
		return tgerr.New(400, "BAD_REQUEST")
	})}
	req := &Request{Ctx: context.Background(), peerRPC: state}
	if err := server.dispatchRPC(conn, req); err != nil {
		t.Fatalf("Telegram error after peer close = %v, want suppressed reply", err)
	}
	if got := transport.sends.Load(); got != 0 {
		t.Fatalf("transport writes after peer close = %d, want zero", got)
	}
}

type rpcFrameTestEncoder struct{}

func (rpcFrameTestEncoder) Encode(*bin.Buffer) error { return nil }

func waitForRPCFrameReads(t *testing.T, conn *rpcFrameTestConn, want int32) {
	t.Helper()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for conn.calls.Load() < want {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("transport Recv calls = %d, want at least %d", conn.calls.Load(), want)
		}
	}
}

var _ net.Error = rpcFrameTestTimeout{}
