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

	// rpcMethod and rpcResult are set by the dispatcher and reply path for the
	// optional RPC tracing boundary. They stay inside this package so request
	// data can never become trace data by accident.
	rpcMethod string
	rpcResult RPCResultClass
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
