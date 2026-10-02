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

	readConn net.Conn
}

func (p peerReadTransport) interruptRead() {
	if p.readConn != nil {
		// gotd's transport.Conn applies context deadlines to the socket, but a
		// context cancellation alone does not interrupt an active Recv.
		if err := p.readConn.SetReadDeadline(time.Now()); err != nil {
			// A concurrent peer close can reject the deadline update; Recv
			// will report the transport's final state.
			return
		}
	}
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

// readFrame resets an in-progress idle read when an RPC starts or finishes.
// That keeps the transport timeout from counting time spent dispatching a
// request, while still keeping exactly one Recv active at a time.
func (r *rpcFrameReader) readFrame() (*bin.Buffer, time.Time, error) {
	for {
		var deadline time.Time
		if r.deadline != nil {
			deadline = r.deadline()
		}
		readCtx, cancelRead := context.WithCancel(r.ctx)
		readDone := make(chan rpcFrameReadResult, 1)
		go func() {
			b := new(bin.Buffer)
			err := r.server.read(readCtx, r.transport, b, deadline)
			readDone <- rpcFrameReadResult{buf: b, err: err}
		}()
		changed := r.peer.changeSignal()

		select {
		case result := <-readDone:
			cancelRead()
			if isPeerReadTimeout(result.err) {
				select {
				case <-changed:
					if r.ctx.Err() == nil {
						continue
					}
				default:
				}
			}
			return result.buf, deadline, result.err
		case <-changed:
			cancelRead()
			if interrupt, ok := r.transport.(interface{ interruptRead() }); ok {
				interrupt.interruptRead()
			}
			result := <-readDone
			if r.ctx.Err() != nil || result.err == nil {
				return result.buf, deadline, result.err
			}
			if isPeerReadTimeout(result.err) {
				if !deadline.IsZero() && !r.now().Before(deadline) {
					return result.buf, deadline, result.err
				}
				continue
			}
			if !errors.Is(result.err, context.Canceled) {
				return result.buf, deadline, result.err
			}
			continue
		case <-r.ctx.Done():
			cancelRead()
			result := <-readDone
			return result.buf, deadline, result.err
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
