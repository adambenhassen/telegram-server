package mtproto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"slices"
)

// ListenProxyV2 accepts MTProto connections on ln, attributing each to the
// address a PROXY protocol v2 header names, and only when that header arrives
// from a source in allow.
//
// This is the mode for running behind an L4 load balancer. There every socket's
// peer address is the balancer's, so socket keying puts every client on earth in
// one bucket and a per-IP cap becomes a global one that locks out every login at
// the same moment. The balancer knows the address it accepted from and writes it
// ahead of the stream; this reads it back.
//
// The header is client-supplied bytes, and allow is the entire reason any of
// them can be believed. Both directions fail closed:
//
//   - a connection from an allowed source that carries no valid v2 header is
//     refused, never served on the balancer's own address;
//   - a header from any other source is never honoured — that connection is
//     refused before a byte of it is read, so the header cannot be credited and
//     cannot pass through as protocol either.
//
// There is no third path and no fallback. A spoofable client address is worse
// than none: it lets an attacker step out of their own bucket and push an
// innocent address into denial at the same time.
//
// log records refusals. It is the signal an operator has for a misconfigured
// allowlist, which otherwise looks like every client failing to connect at once;
// nil discards.
func ListenProxyV2(ln net.Listener, allow []netip.Prefix, log *slog.Logger) Listener {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	// Cloned: the allowlist is the trust decision, and it must not change under
	// the accept loop because a caller reused the slice it passed in.
	return newListener(ln, proxyV2Source{allow: slices.Clone(allow)}, log)
}

const (
	// proxyV2FixedLen is the fixed part of a v2 header: the 12-byte signature,
	// one byte of version and command, one of address family and protocol, and
	// two of address-block length.
	proxyV2FixedLen = 16
	// proxyV2Version is the only protocol version accepted. v1 is the text form
	// and is refused rather than parsed: one header format is the whole of what
	// this mode has to get right, and a second one is a second way to be wrong.
	proxyV2Version = 2

	// proxyV2CmdLocal marks a connection the sender opened itself — a health
	// check — which names no client. proxyV2CmdProxy marks a proxied one.
	proxyV2CmdLocal = 0x0
	proxyV2CmdProxy = 0x1

	// Address families, in the header's high nibble.
	proxyV2AFINet  = 0x1
	proxyV2AFINet6 = 0x2

	// Address block sizes: source and destination address, then both ports.
	proxyV2INetLen  = 4 + 4 + 2 + 2
	proxyV2INet6Len = 16 + 16 + 2 + 2
)

// proxyV2Signature is the fixed 12 bytes every v2 header opens with. It is
// deliberately un-typeable as MTProto: a stream that does not start with it is
// not a header this mode may read.
var proxyV2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

// proxyV2Source takes the client address from a PROXY v2 header written by an
// allowlisted balancer.
type proxyV2Source struct {
	allow []netip.Prefix
}

// clientAddr checks the sender against the allowlist, then reads exactly one
// PROXY v2 header off the connection.
//
// Exactly one: the header is self-delimiting — a fixed 16 bytes that carry the
// length of the address block after them — so the read stops on the byte the
// transport codec header starts at. Nothing is buffered past it, which is what
// keeps codec detection seeing the same first byte it would have seen without a
// balancer in front.
//
// The read runs under the deadline listener.setup armed. That deadline covers
// the codec sniff after this too, so it is not cleared here: a sender that
// writes a valid header and then goes quiet must be bounded by the same clock as
// one that writes nothing at all.
func (s proxyV2Source) clientAddr(conn net.Conn) (netip.Addr, error) {
	peer := peerAddr(conn.RemoteAddr())
	if !s.allowed(peer) {
		// Refused before any read: bytes from a sender outside the allowlist
		// are never interpreted, as a header or as anything else.
		return netip.Addr{}, fmt.Errorf("no PROXY header is accepted from %s: not an allowlisted balancer", peer)
	}
	return readProxyV2(conn)
}

// allowed reports whether a header from peer may be believed. An address the
// transport could not parse is in no prefix and is refused with the rest.
func (s proxyV2Source) allowed(peer netip.Addr) bool {
	if !peer.IsValid() {
		return false
	}
	return slices.ContainsFunc(s.allow, func(p netip.Prefix) bool { return p.Contains(peer) })
}

// readProxyV2 consumes one PROXY protocol v2 header from r and returns the
// client address it names.
//
// A header that names no client address — the LOCAL command a balancer's health
// check sends, or a family this server cannot key on — returns the zero Addr and
// no error. That connection is served and carries no address at all, which is
// not the same as being served on the balancer's: nothing keyed on an address
// can be charged to it, and the sendCode limiter refuses what it cannot
// attribute. Refusing outright instead would take a load balancer's health
// checks down with it.
//
// Every other malformed case is an error, and an error refuses the connection.
func readProxyV2(r io.Reader) (netip.Addr, error) {
	var fixed [proxyV2FixedLen]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return netip.Addr{}, fmt.Errorf("read PROXY v2 header: %w", err)
	}
	if !bytes.Equal(fixed[:len(proxyV2Signature)], proxyV2Signature) {
		return netip.Addr{}, errors.New("connection does not start with a PROXY v2 header")
	}
	if version := fixed[12] >> 4; version != proxyV2Version {
		return netip.Addr{}, fmt.Errorf("PROXY protocol version %d: only version 2 is accepted", version)
	}
	// The declared length is read whatever the command is, so the stream is
	// left at the codec header even for a command carrying nothing usable.
	block := make([]byte, binary.BigEndian.Uint16(fixed[14:16]))
	if _, err := io.ReadFull(r, block); err != nil {
		return netip.Addr{}, fmt.Errorf("read PROXY v2 address block: %w", err)
	}

	switch cmd := fixed[12] & 0x0f; cmd {
	case proxyV2CmdLocal:
		return netip.Addr{}, nil
	case proxyV2CmdProxy:
	default:
		return netip.Addr{}, fmt.Errorf("unknown PROXY v2 command %#x", cmd)
	}

	// Anything past the addresses is TLV extensions, which this server does not
	// read: the source address is the whole of what it needs.
	switch family := fixed[13] >> 4; family {
	case proxyV2AFINet:
		if len(block) < proxyV2INetLen {
			return netip.Addr{}, fmt.Errorf("PROXY v2 IPv4 address block is %d bytes, want at least %d", len(block), proxyV2INetLen)
		}
		return netip.AddrFrom4([4]byte(block[:4])), nil
	case proxyV2AFINet6:
		if len(block) < proxyV2INet6Len {
			return netip.Addr{}, fmt.Errorf("PROXY v2 IPv6 address block is %d bytes, want at least %d", len(block), proxyV2INet6Len)
		}
		// Unmapped for the same reason a socket address is: one host must not
		// get a second bucket by being written in the other family's form.
		return netip.AddrFrom16([16]byte(block[:16])).Unmap(), nil
	default:
		// AF_UNSPEC and AF_UNIX name no address this server can key on.
		return netip.Addr{}, nil
	}
}
