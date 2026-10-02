package mtproto

import (
	"context"
	"errors"
	"io"
	"net"
	"time"

	"github.com/coder/websocket"
	"github.com/gotd/td/bin"
	"github.com/gotd/td/transport"
)

// peerReadTransport marks transports returned by codec detection. ServeConn's
// scripted test transports keep their synchronous read behavior; accepted TCP
// and WebSocket connections use one bounded read-ahead worker with one Recv at
// a time.
type peerReadTransport struct {
	transport.Conn
}

type rpcFrameResult struct {
	buf      *bin.Buffer
	err      error
	exchange bool
}

type rpcFrameReadResult struct {
	buf *bin.Buffer
	err error
}

type rpcFrameReader struct {
	server    *Server
	transport transport.Conn
	peer      *peerRPCState
	ctx       context.Context
	cancel    context.CancelFunc
	deadline  func() time.Time

	frames chan rpcFrameResult
	ack    chan struct{}
	resume chan struct{}
	done   chan struct{}
}

func newRPCFrameReader(
	server *Server,
	ctx context.Context,
	transportConn transport.Conn,
	peer *peerRPCState,
	deadline func() time.Time,
) *rpcFrameReader {
	readerCtx, cancel := context.WithCancel(ctx)
	r := &rpcFrameReader{
		server:    server,
		transport: transportConn,
		peer:      peer,
		ctx:       readerCtx,
		cancel:    cancel,
		deadline:  deadline,
		frames:    make(chan rpcFrameResult, 1),
		ack:       make(chan struct{}, 1),
		resume:    make(chan struct{}, 1),
		done:      make(chan struct{}),
	}
	go r.run()
	return r
}

func (r *rpcFrameReader) run() {
	defer close(r.done)
	for {
		b, deadline, err := r.readFrame()
		if isPeerReadTimeout(err) && r.peer.hasActive() &&
			(deadline.IsZero() || r.now().Before(deadline)) {
			// The ordinary read timeout only ends an idle connection. While an
			// RPC is running it must not cancel that RPC or stop watching for a
			// later EOF/reset. An absolute pending-login ceiling is different:
			// it is a server-owned close and bypasses the peer budget.
			continue
		}
		result := rpcFrameResult{buf: b, err: err}
		if err != nil {
			if r.ctx.Err() != nil || r.peer.isServerClosed() ||
				(!deadline.IsZero() && !r.now().Before(deadline)) {
				r.peer.serverClosed()
			} else if isPeerReadDisconnect(err) {
				r.peer.peerDisconnected()
			}
		} else {
			result.exchange = isExchangeFrame(b)
		}

		select {
		case r.frames <- result:
		case <-r.ctx.Done():
			return
		}
		if err != nil {
			return
		}
		select {
		case <-r.ack:
		case <-r.ctx.Done():
			return
		}
		if result.exchange {
			select {
			case <-r.resume:
			case <-r.ctx.Done():
				return
			}
		}
	}
}

// readFrame keeps one Recv alive until the whole frame arrives. Restarting a
// codec read loses partial framing; expiring a WebSocket read deadline closes
// its stream permanently. RPC state changes only update the terminal timer.
func (r *rpcFrameReader) readFrame() (*bin.Buffer, time.Time, error) {
	readCtx, cancelRead := context.WithCancel(context.WithoutCancel(r.ctx))
	defer cancelRead()
	readDone := make(chan rpcFrameReadResult, 1)
	go func() {
		b := new(bin.Buffer)
		err := r.transport.Recv(readCtx, b)
		readDone <- rpcFrameReadResult{buf: b, err: err}
	}()

	timer := time.NewTimer(time.Hour)
	defer timer.Stop()
	var deadline time.Time
	var generation uint64
	resetTimer := func() {
		timer.Stop()
		deadline = time.Time{}
		if r.deadline != nil {
			deadline = r.deadline()
		}
		active, observedGeneration := r.peer.readState()
		generation = observedGeneration
		if !deadline.IsZero() {
			timer.Reset(max(0, deadline.Sub(r.now())))
		} else if !active {
			timer.Reset(r.server.readTimeout)
		}
	}
	resetTimer()
	changed := r.peer.changeSignal()

	closeRead := func(cause error) (*bin.Buffer, time.Time, error) {
		// Publish server provenance before cancellation or transport close can
		// make the reader observe a peer-shaped error.
		r.peer.serverClosed()
		cancelRead()
		closeErr := r.transport.Close()
		result := <-readDone
		if closeErr != nil && !isDisconnect(closeErr) {
			cause = errors.Join(cause, closeErr)
		}
		return result.buf, deadline, cause
	}
	for {
		select {
		case result := <-readDone:
			return result.buf, deadline, result.err
		case <-changed:
			resetTimer()
		case <-timer.C:
			if deadline.IsZero() && !r.peer.closeIfIdle(generation) {
				// An RPC start or completion raced the timer. Observe the new
				// state and grant a fresh idle interval after completion.
				resetTimer()
				continue
			}
			return closeRead(context.DeadlineExceeded)
		case <-r.ctx.Done():
			return closeRead(r.ctx.Err())
		}
	}
}

func (r *rpcFrameReader) now() time.Time {
	if r.server.clock != nil {
		return r.server.clock.Now()
	}
	return time.Now()
}

func (r *rpcFrameReader) next() (*bin.Buffer, error, bool) {
	select {
	case result := <-r.frames:
		if result.err == nil {
			select {
			case r.ack <- struct{}{}:
			case <-r.ctx.Done():
			}
		}
		return result.buf, result.err, result.exchange
	case <-r.ctx.Done():
		return nil, r.ctx.Err(), false
	}
}

func (r *rpcFrameReader) resumeExchange() {
	select {
	case r.resume <- struct{}{}:
	default:
	}
}

func (r *rpcFrameReader) stop() {
	r.cancel()
}

func (r *rpcFrameReader) join() {
	<-r.done
}

func isExchangeFrame(b *bin.Buffer) bool {
	var authKeyID [8]byte
	if b.PeekN(authKeyID[:], len(authKeyID)) != nil {
		return false
	}
	return authKeyID == ([8]byte{})
}

func isPeerReadDisconnect(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return false
	}
	if websocket.CloseStatus(err) >= 0 {
		return true
	}
	return errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) || isDisconnect(err)
}

func isPeerReadTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	return errors.As(err, &netErr) && netErr.Timeout()
}
