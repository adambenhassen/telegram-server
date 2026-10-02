package mtproto

import (
	"context"
	"errors"
	"fmt"
	"net/netip"
	"strings"
	"sync"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

// Request represents a decrypted MTProto RPC request handed to a Handler.
type Request struct {
	// AuthKeyID is the 8-byte auth key ID of the session that sent the request.
	AuthKeyID [8]byte
	// UserID is the user bound to the auth key, or 0 when unbound.
	// Auth-key/user binding is wired in a later task; it stays 0 here.
	UserID int64
	// Provisional is true when the bound user is username-mode and has not
	// yet completed sign-in (no verifier stored). It is derived from the
	// AuthKeyByID lookup that already runs per frame, never an extra query.
	Provisional bool
	// ClientAddr is the peer address of the connection this request arrived on,
	// read off the socket at accept. It is the only address the server has, and
	// it is never derived from anything the client sends: MTProto carries no
	// client-supplied address, and a header on this protocol would be forgeable
	// anyway. The zero value means the socket had no address the server could
	// parse — a transport-layer fault a handler must decide about explicitly,
	// never a bucket to group requests into.
	ClientAddr netip.Addr
	// SessionID is the MTProto session ID from the decrypted message.
	SessionID int64
	// MsgID is the message ID of the request, used as the RPC result target.
	MsgID int64
	// Buf holds the plaintext request body, positioned at the constructor ID.
	Buf *bin.Buffer
	// Ctx is the request context.
	Ctx context.Context
	// serverCtx is the connection's server-lifetime context. It lets a handler
	// detach committed completion work from peer loss without dropping shutdown.
	serverCtx context.Context
	// peerRPC connects one actual dispatch to its connection's peer-loss state.
	peerRPC *peerRPCState

	// rpcMethod and rpcResult are set by the dispatcher and reply path for the
	// optional RPC tracing boundary. They stay inside this package so request
	// data can never become trace data by accident.
	rpcMethod string
	rpcResult RPCResultClass
}

// ServerContext returns the serving connection's lifetime context, before any
// per-RPC deadline or peer-disconnect cancellation is applied.
func (r *Request) ServerContext() context.Context { return r.serverCtx }

// PeerDisconnected reports whether the serving transport is known to have
// closed, so reply hooks can finish committed delivery bookkeeping without a
// write that can no longer reach the caller.
func (r *Request) PeerDisconnected() bool {
	return r.peerRPC != nil && r.peerRPC.disconnected()
}

// Handler processes decrypted MTProto requests.
type Handler interface {
	OnMessage(c *Conn, req *Request) error
}

var _ Handler = HandlerFunc(nil)

// HandlerFunc adapts a function to the Handler interface.
type HandlerFunc func(c *Conn, req *Request) error

// OnMessage implements Handler.
func (h HandlerFunc) OnMessage(c *Conn, req *Request) error {
	return h(c, req)
}

// Dispatcher routes requests to handlers keyed by TL constructor ID.
type Dispatcher struct {
	mux      sync.Mutex
	reqs     map[uint32]Handler
	methods  map[uint32]string
	fallback Handler
}

var _ Handler = (*Dispatcher)(nil)

// NewDispatcher creates an empty Dispatcher.
func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		reqs:    map[uint32]Handler{},
		methods: map[uint32]string{},
	}
}

// Handle registers a handler for the given TL constructor ID.
func (d *Dispatcher) Handle(id uint32, h Handler) *Dispatcher {
	return d.HandleNamed(id, compiledMethodName(id), h)
}

// HandleNamed registers h with the fixed method name used by RPC tracing.
// The name is supplied by compiled server code, never by a request.
func (d *Dispatcher) HandleNamed(id uint32, name string, h Handler) *Dispatcher {
	d.mux.Lock()
	d.reqs[id] = h
	if canonical := sanitizeRPCMethod(name); canonical != UnknownRPCMethod {
		d.methods[id] = canonical
	} else {
		delete(d.methods, id)
	}
	d.mux.Unlock()
	return d
}

// HandleFunc registers a handler function for the given TL constructor ID.
func (d *Dispatcher) HandleFunc(id uint32, fn func(c *Conn, req *Request) error) *Dispatcher {
	return d.Handle(id, HandlerFunc(fn))
}

// HandleFuncNamed registers fn with the fixed method name used by RPC
// tracing. It is for handlers whose constructor is not present in the gotd
// generated registry.
func (d *Dispatcher) HandleFuncNamed(id uint32, name string, fn func(c *Conn, req *Request) error) *Dispatcher {
	return d.HandleNamed(id, name, HandlerFunc(fn))
}

// Fallback sets the handler invoked for unregistered constructor IDs.
func (d *Dispatcher) Fallback(h Handler) *Dispatcher {
	d.mux.Lock()
	d.fallback = h
	d.mux.Unlock()
	return d
}

// OnMessage implements Handler by peeking the request constructor ID and
// dispatching to the registered handler, else the fallback.
func (d *Dispatcher) OnMessage(c *Conn, req *Request) error {
	id, err := req.Buf.PeekID()
	if err != nil {
		return fmt.Errorf("peek id: %w", err)
	}

	d.mux.Lock()
	h, ok := d.reqs[id]
	method := d.methods[id]
	fallback := d.fallback
	d.mux.Unlock()
	if method == "" {
		method = unknownRPCMethod
	}
	req.rpcMethod = method

	if ok {
		return h.OnMessage(c, req)
	}
	if fallback != nil {
		return fallback.OnMessage(c, req)
	}
	return fmt.Errorf("unexpected type %#x", id)
}

// compiledMethodName resolves an id through gotd's generated type registry.
// The dispatcher records only ids explicitly registered by this process, so a
// valid method in the schema that this server does not handle still collapses
// to unknown at the tracing boundary.
func compiledMethodName(id uint32) string {
	name, ok := compiledMethodNames[id]
	if !ok {
		return ""
	}
	return name
}

var compiledMethodNames = func() map[uint32]string {
	methods := make(map[uint32]string)
	for id, name := range tg.TypesMap() {
		method, _, ok := strings.Cut(name, "#")
		if ok && strings.Contains(method, ".") {
			methods[id] = method
		}
	}
	return methods
}()

var compiledMethodVocabulary = func() map[string]struct{} {
	methods := make(map[string]struct{}, len(compiledMethodNames)+1)
	for _, name := range compiledMethodNames {
		methods[name] = struct{}{}
	}
	// gotd v0.161.0 does not generate this request, but the application
	// handler is compiled for it and supplies its fixed name at registration.
	methods["messages.revokeExportedChatInvite"] = struct{}{}
	return methods
}()

// UnpackInvoke peels invokeWithLayer, initConnection and invokeWithoutUpdates
// wrappers off the request buffer before delegating to next, leaving the buffer
// positioned at the inner query. Mirrors gotd tgtest/middleware.go.
func UnpackInvoke(next Handler) Handler {
	return HandlerFunc(func(c *Conn, req *Request) error {
		id, err := req.Buf.PeekID()
		if err != nil {
			return fmt.Errorf("peek id: %w", err)
		}

		obj := &peekIDObject{}
		var r bin.Decoder
		for {
			switch id {
			case tg.InvokeWithLayerRequestTypeID:
				r = &tg.InvokeWithLayerRequest{Query: obj}
			case tg.InitConnectionRequestTypeID:
				r = &tg.InitConnectionRequest{Query: obj}
			case tg.InvokeWithoutUpdatesRequestTypeID:
				r = &tg.InvokeWithoutUpdatesRequest{Query: obj}
			default:
				return next.OnMessage(c, req)
			}

			if err := r.Decode(req.Buf); err != nil {
				return err
			}
			id = obj.TypeID
		}
	})
}

// InvokeAfterMsgRefusal identifies whether an invokeAfterMsg dependency was
// unknown/invalid or completed with an RPC error.
type InvokeAfterMsgRefusal int

const (
	InvokeAfterMsgWaitTimeout InvokeAfterMsgRefusal = iota + 1
	InvokeAfterMsgWaitFailed
)

// UnpackInvokeWithAfterMsg unwraps the ordinary invocation envelopes and
// implements invokeAfterMsg only when its dependency completed successfully on
// this connection and session. onRefusal owns the refusal response and any
// application-specific accounting; unknown dependencies have already consumed
// the connection's unimplemented-method budget.
func UnpackInvokeWithAfterMsg(next Handler, onRefusal func(*Conn, *Request, uint32, InvokeAfterMsgRefusal, UnimplementedVerdict) error) Handler {
	return HandlerFunc(func(c *Conn, req *Request) error {
		id, err := req.Buf.PeekID()
		if err != nil {
			return fmt.Errorf("peek id: %w", err)
		}

		obj := &peekIDObject{}
		refuse := func(innerID uint32, reason InvokeAfterMsgRefusal, charge bool) error {
			verdict := UnimplementedAnswer
			if charge && c != nil {
				verdict = c.ChargeUnimplemented()
			}
			if onRefusal != nil {
				return onRefusal(c, req, innerID, reason, verdict)
			}
			if c == nil {
				return errors.New("invokeAfterMsg refused without connection")
			}
			if reason == InvokeAfterMsgWaitFailed {
				return c.SendErr(req, tgerr.New(400, "MSG_WAIT_FAILED"))
			}
			switch verdict {
			case UnimplementedClose:
				return errors.New("unimplemented-method ceiling")
			case UnimplementedFloodWait:
				return c.SendErr(req, tgerr.New(420, "FLOOD_WAIT_30"))
			default:
				return c.SendErr(req, tgerr.New(400, "MSG_WAIT_TIMEOUT"))
			}
		}
		refuseUnsupported := func(innerID uint32) error {
			if isInvokeWrapper(innerID) {
				var err error
				innerID, err = enclosedInvokeMethodID(req.Buf, innerID)
				if err != nil {
					return err
				}
			}
			return refuse(innerID, InvokeAfterMsgWaitTimeout, true)
		}
		for {
			switch id {
			case tg.InvokeWithLayerRequestTypeID:
				if err := (&tg.InvokeWithLayerRequest{Query: obj}).Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
			case tg.InitConnectionRequestTypeID:
				if err := (&tg.InitConnectionRequest{Query: obj}).Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
			case tg.InvokeWithoutUpdatesRequestTypeID:
				if err := (&tg.InvokeWithoutUpdatesRequest{Query: obj}).Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
			case tg.InvokeAfterMsgRequestTypeID:
				var wrapper tg.InvokeAfterMsgRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				if isInvokeWrapper(id) {
					return refuseUnsupported(id)
				}
				if wrapper.MsgID <= 0 || wrapper.MsgID >= req.MsgID || c == nil {
					return refuse(id, InvokeAfterMsgWaitTimeout, true)
				}
				found, success := c.RPCDependencyOutcome(req.SessionID, wrapper.MsgID, req.MsgID)
				if !found {
					return refuse(id, InvokeAfterMsgWaitTimeout, true)
				}
				if !success {
					return refuse(id, InvokeAfterMsgWaitFailed, false)
				}
				continue
			case tg.InvokeAfterMsgsRequestTypeID:
				var wrapper tg.InvokeAfterMsgsRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				return refuseUnsupported(id)
			case tg.InvokeWithTakeoutRequestTypeID:
				var wrapper tg.InvokeWithTakeoutRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				return refuseUnsupported(id)
			case tg.InvokeWithGooglePlayIntegrityRequestTypeID:
				var wrapper tg.InvokeWithGooglePlayIntegrityRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				return refuseUnsupported(id)
			case tg.InvokeWithBusinessConnectionRequestTypeID:
				var wrapper tg.InvokeWithBusinessConnectionRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				return refuseUnsupported(id)
			case tg.InvokeWithReCaptchaRequestTypeID:
				var wrapper tg.InvokeWithReCaptchaRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				return refuseUnsupported(id)
			case tg.InvokeWithApnsSecretRequestTypeID:
				var wrapper tg.InvokeWithApnsSecretRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				return refuseUnsupported(id)
			case tg.InvokeWithMessagesRangeRequestTypeID:
				var wrapper tg.InvokeWithMessagesRangeRequest
				wrapper.Query = obj
				if err := wrapper.Decode(req.Buf); err != nil {
					return err
				}
				id = obj.TypeID
				return refuseUnsupported(id)
			default:
				return next.OnMessage(c, req)
			}
		}
	})
}

// enclosedInvokeMethodID walks supported wrapper bodies without dispatching
// them, returning the leaf constructor for refusal accounting. In particular,
// a nested folder mutation must still spend its attempt budget and repair only
// its requester even though the wrapper form itself is unsupported.
func enclosedInvokeMethodID(b *bin.Buffer, id uint32) (uint32, error) {
	obj := &peekIDObject{}
	for isInvokeWrapper(id) {
		var r bin.Decoder
		switch id {
		case tg.InvokeAfterMsgRequestTypeID:
			r = &tg.InvokeAfterMsgRequest{Query: obj}
		case tg.InvokeAfterMsgsRequestTypeID:
			r = &tg.InvokeAfterMsgsRequest{Query: obj}
		case tg.InvokeWithLayerRequestTypeID:
			r = &tg.InvokeWithLayerRequest{Query: obj}
		case tg.InitConnectionRequestTypeID:
			r = &tg.InitConnectionRequest{Query: obj}
		case tg.InvokeWithoutUpdatesRequestTypeID:
			r = &tg.InvokeWithoutUpdatesRequest{Query: obj}
		case tg.InvokeWithTakeoutRequestTypeID:
			r = &tg.InvokeWithTakeoutRequest{Query: obj}
		case tg.InvokeWithGooglePlayIntegrityRequestTypeID:
			r = &tg.InvokeWithGooglePlayIntegrityRequest{Query: obj}
		case tg.InvokeWithBusinessConnectionRequestTypeID:
			r = &tg.InvokeWithBusinessConnectionRequest{Query: obj}
		case tg.InvokeWithReCaptchaRequestTypeID:
			r = &tg.InvokeWithReCaptchaRequest{Query: obj}
		case tg.InvokeWithApnsSecretRequestTypeID:
			r = &tg.InvokeWithApnsSecretRequest{Query: obj}
		case tg.InvokeWithMessagesRangeRequestTypeID:
			r = &tg.InvokeWithMessagesRangeRequest{Query: obj}
		default:
			return id, nil
		}
		if err := r.Decode(b); err != nil {
			return 0, err
		}
		id = obj.TypeID
	}
	return id, nil
}

func isInvokeWrapper(id uint32) bool {
	switch id {
	case tg.InvokeAfterMsgRequestTypeID,
		tg.InvokeAfterMsgsRequestTypeID,
		tg.InvokeWithLayerRequestTypeID,
		tg.InitConnectionRequestTypeID,
		tg.InvokeWithoutUpdatesRequestTypeID,
		tg.InvokeWithTakeoutRequestTypeID,
		tg.InvokeWithGooglePlayIntegrityRequestTypeID,
		tg.InvokeWithBusinessConnectionRequestTypeID,
		tg.InvokeWithReCaptchaRequestTypeID,
		tg.InvokeWithApnsSecretRequestTypeID,
		tg.InvokeWithMessagesRangeRequestTypeID:
		return true
	default:
		return false
	}
}

// peekIDObject is a bin.Object that records the constructor ID of the value it
// decodes without consuming it, so the wrapper's Query points at the inner body.
type peekIDObject struct {
	TypeID uint32
}

// Decode records the peeked constructor ID.
func (t *peekIDObject) Decode(b *bin.Buffer) error {
	id, err := b.PeekID()
	if err != nil {
		return fmt.Errorf("peek id: %w", err)
	}
	t.TypeID = id
	return nil
}

// Encode always errors: peekIDObject is decode-only.
func (t *peekIDObject) Encode(*bin.Buffer) error {
	return errors.New("peekIDObject must not be encoded")
}
