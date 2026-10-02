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
	reads    chan rpcFrameTestRead
	returned chan struct{}
	calls    atomic.Int32
	sends    atomic.Int32
}

func newRPCFrameTestConn() *rpcFrameTestConn {
	return &rpcFrameTestConn{
		reads:    make(chan rpcFrameTestRead, 8),
		returned: make(chan struct{}, 8),
	}
}

func (c *rpcFrameTestConn) Recv(ctx context.Context, b *bin.Buffer) error {
	c.calls.Add(1)
	select {
	case c.returned <- struct{}{}:
	default:
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

type rpcFrameTestTimeoutError struct{}

func (rpcFrameTestTimeoutError) Error() string   { return "read timeout" }
func (rpcFrameTestTimeoutError) Timeout() bool   { return true }
func (rpcFrameTestTimeoutError) Temporary() bool { return true }

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
	if got := context.Cause(activeCtx); !errors.Is(got, errPeerRPCDisconnected) {
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
	conn.reads <- rpcFrameTestRead{err: rpcFrameTestTimeoutError{}}
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

func TestRPCFrameReaderKeepsReadAcrossRPCCompletion(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	conn := &rpcFrameNetTestConn{rpcFrameTestConn: newRPCFrameTestConn(), Conn: serverConn}
	state := newPeerRPCState(newRPCCancelBudget(time.Now, 8))
	_, finish, started := state.begin(21, t.Context())
	if !started {
		t.Fatal("active RPC did not start")
	}
	reader := newRPCFrameReader(&Server{readTimeout: 100 * time.Millisecond}, t.Context(), peerReadTransport{Conn: conn}, state, nil)
	defer func() {
		reader.stop()
		if err := clientConn.Close(); err != nil {
			t.Errorf("close client connection: %v", err)
		}
		reader.join()
	}()
	waitForRPCFrameReads(t, conn.rpcFrameTestConn, 1)
	// An active request may outlive the ordinary transport idle timeout.
	time.Sleep(200 * time.Millisecond)
	if _, err := clientConn.Write([]byte{42}); err != nil {
		t.Fatalf("write during long RPC: %v", err)
	}
	frame, err, _ := reader.next()
	if err != nil || len(frame.Buf) != 1 || frame.Buf[0] != 42 {
		t.Fatalf("frame during long RPC = %v, %v; want [42]", frame, err)
	}
	waitForRPCFrameReads(t, conn.rpcFrameTestConn, 2)
	finishedAt := time.Now()
	finish()
	_, err, _ = reader.next()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("idle read = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(finishedAt); elapsed < 90*time.Millisecond || elapsed > time.Second {
		t.Fatalf("idle timeout after RPC completion = %s", elapsed)
	}
}

func TestRPCFrameReaderPreservesPartialFrameOnRPCStateChange(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	conn := &rpcFrameNetTestConn{rpcFrameTestConn: newRPCFrameTestConn(), Conn: serverConn, frameSize: 2}
	state := newPeerRPCState(newRPCCancelBudget(time.Now, 8))
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, t.Context(), peerReadTransport{Conn: conn}, state, nil)
	defer func() {
		reader.stop()
		if err := clientConn.Close(); err != nil {
			t.Errorf("close client connection: %v", err)
		}
		reader.join()
	}()
	waitForRPCFrameReads(t, conn.rpcFrameTestConn, 1)
	if _, err := clientConn.Write([]byte{42}); err != nil {
		t.Fatalf("write partial frame: %v", err)
	}
	_, finish, started := state.begin(23, t.Context())
	if !started {
		t.Fatal("active RPC did not start")
	}
	finish()
	// Give the state change a chance to interrupt the unfinished frame.
	time.Sleep(20 * time.Millisecond)
	if err := clientConn.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set write deadline: %v", err)
	}
	if _, err := clientConn.Write([]byte{43}); err != nil {
		t.Fatalf("finish partial frame: %v", err)
	}
	result := make(chan rpcFrameResult, 1)
	go func() {
		frame, err, _ := reader.next()
		result <- rpcFrameResult{buf: frame, err: err}
	}()
	select {
	case got := <-result:
		if got.err != nil || len(got.buf.Buf) != 2 || got.buf.Buf[0] != 42 || got.buf.Buf[1] != 43 {
			t.Fatalf("partial frame after RPC state change = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("RPC state change discarded the partial frame")
	}
}

func TestRPCFrameReaderPendingCeilingClosesActiveRPC(t *testing.T) {
	t.Parallel()

	serverConn, clientConn := net.Pipe()
	conn := &rpcFrameNetTestConn{rpcFrameTestConn: newRPCFrameTestConn(), Conn: serverConn}
	state := newPeerRPCState(nil)
	activeCtx, finish, started := state.begin(24, t.Context())
	if !started {
		t.Fatal("active RPC did not start")
	}
	defer finish()
	ceiling := time.Now().Add(100 * time.Millisecond)
	reader := newRPCFrameReader(&Server{readTimeout: time.Hour}, t.Context(), peerReadTransport{Conn: conn}, state, func() time.Time { return ceiling })
	defer func() {
		reader.stop()
		if err := clientConn.Close(); err != nil {
			t.Errorf("close client connection: %v", err)
		}
		reader.join()
	}()
	if _, err, _ := reader.next(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pending ceiling = %v, want deadline exceeded", err)
	}
	if !errors.Is(context.Cause(activeCtx), errServerRPCClosed) {
		t.Fatalf("pending ceiling cancellation = %v, want server close", context.Cause(activeCtx))
	}
}

type rpcFrameNetTestConn struct {
	*rpcFrameTestConn
	net.Conn

	frameSize int
}

func (c *rpcFrameNetTestConn) Recv(ctx context.Context, b *bin.Buffer) error {
	if deadline, ok := ctx.Deadline(); ok {
		if err := c.SetReadDeadline(deadline); err != nil {
			return err
		}
	}
	c.calls.Add(1)
	select {
	case c.returned <- struct{}{}:
	default:
	}
	frame := make([]byte, max(1, c.frameSize))
	if _, err := io.ReadFull(c, frame); err != nil {
		return err
	}
	b.ResetTo(frame)
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

var _ net.Error = rpcFrameTestTimeoutError{}

func TestPeerRPCIdleExpiryDoesNotCloseNewlyCompletedRPC(t *testing.T) {
	t.Parallel()

	state := newPeerRPCState(nil)
	_, generation := state.readState()
	_, finish, started := state.begin(25, t.Context())
	if !started {
		t.Fatal("active RPC did not start")
	}
	finish()
	if state.closeIfIdle(generation) {
		t.Fatal("stale idle expiry closed a newly completed RPC's connection")
	}
	_, generation = state.readState()
	if !state.closeIfIdle(generation) {
		t.Fatal("current idle expiry did not close the idle connection")
	}
	if _, _, started := state.begin(25, t.Context()); started {
		t.Fatal("RPC started after server idle close")
	}
}

func TestPeerRPCIdleExpiryRacesAdmission(t *testing.T) {
	t.Parallel()

	for range 100 {
		state := newPeerRPCState(nil)
		_, generation := state.readState()
		start := make(chan struct{})
		closed := make(chan bool, 1)
		go func() {
			<-start
			closed <- state.closeIfIdle(generation)
		}()
		close(start)
		ctx, finish, started := state.begin(26, t.Context())
		idleClosed := <-closed
		if started {
			if idleClosed || ctx.Err() != nil {
				t.Error("idle expiry closed an admitted RPC")
			}
			finish()
		} else if !idleClosed {
			t.Fatal("idle expiry and RPC admission both failed")
		}
	}
}

func TestDisconnectedRPCPreservesHandlerFailure(t *testing.T) {
	t.Parallel()

	state := newPeerRPCState(nil)
	state.peerDisconnected()
	failure := errors.New("synthetic handler failure")
	server := &Server{handler: HandlerFunc(func(*Conn, *Request) error { return failure })}
	req := &Request{Ctx: t.Context(), peerRPC: state}
	err := server.dispatchRPC(&Conn{}, req)
	if !errors.Is(err, failure) {
		t.Fatalf("disconnected handler failure = %v, want original failure", err)
	}
	if result := requestRPCResult(req, err); result != RPCResultInternal {
		t.Fatalf("disconnected handler result = %s, want internal", result)
	}
}
