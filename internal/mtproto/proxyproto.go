package mtproto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net/netip"
	"slices"
	"sync/atomic"
	"time"
)

// negotiationLogInterval bounds how often a failed negotiation is logged. The
// failures are driven by whoever can reach the port, unauthenticated, so an
// unsampled line each is a log an attacker writes; each line that does come out
// carries how many were suppressed behind it.
const negotiationLogInterval = 10 * time.Second

// logSampler thins a log line that anyone who can reach the port can provoke.
// It holds no lock — the compare-and-swap is the whole of it, and a caller that
// loses the swap is one of the callers being dropped anyway.
type logSampler struct {
	// last is the unix-nanosecond time of the line that was emitted, 0 before
	// the first one. dropped counts the lines suppressed since then.
	last    atomic.Int64
	dropped atomic.Int64
}

// allow reports whether a line may be emitted now and, when it may, how many
// were dropped since the previous one.
func (s *logSampler) allow(now time.Time, interval time.Duration) (int64, bool) {
	n := now.UnixNano()
	last := s.last.Load()
	if n-last < int64(interval) || !s.last.CompareAndSwap(last, n) {
		s.dropped.Add(1)
		return 0, false
	}
	return s.dropped.Swap(0), true
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

	// Address families, in the family byte's high nibble.
	proxyV2AFUnspec = 0x0
	proxyV2AFINet   = 0x1
	proxyV2AFINet6  = 0x2

	// Transport protocols, in its low nibble. Only STREAM carries an address
	// this server may key on: it is the one a TCP balancer reports, and MTProto
	// arrives over nothing else.
	proxyV2ProtoUnspec = 0x0
	proxyV2ProtoStream = 0x1

	// Address block sizes: source and destination address, then both ports.
	proxyV2INetLen  = 4 + 4 + 2 + 2
	proxyV2INet6Len = 16 + 16 + 2 + 2
)

// proxyV2Signature is the fixed 12 bytes every v2 header opens with. It is
// deliberately un-typeable as MTProto: a stream that does not start with it is
// not a header this mode may read.
var proxyV2Signature = []byte{0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A}

// proxyV2Source is the balancer allowlist that makes a PROXY v2 header
// believable.
//
// The header is client-supplied bytes, and the allowlist is the entire reason
// any of them can be trusted. Both directions fail closed: a connection from an
// allowed source carrying no valid v2 header is refused rather than served on
// the balancer's own address, and a header from any other source is refused
// before a byte of it is read, so it can neither be credited nor pass through as
// protocol. There is no third path and no fallback — a spoofable client address
// is worse than none, because it lets an attacker step out of their own bucket
// and push an innocent address into denial at the same time.
type proxyV2Source struct {
	allow []netip.Prefix
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

	// The family and the transport are one decision, not two. Reading the
	// family alone would credit an address to a combination no TCP balancer can
	// produce — AF_INET over DGRAM names a UDP peer, AF_INET over UNSPEC names
	// nothing at all — and an address this server cannot vouch for is exactly
	// what it must not key a limit on. So the supported combinations are listed,
	// the no-address ones are listed, and anything else is refused rather than
	// falling through to either.
	//
	// Anything past the addresses is TLV extensions, which this server does not
	// read: the source address is the whole of what it needs.
	family, proto := fixed[13]>>4, fixed[13]&0x0f
	switch {
	case family == proxyV2AFINet && proto == proxyV2ProtoStream:
		if len(block) < proxyV2INetLen {
			return netip.Addr{}, fmt.Errorf("PROXY v2 IPv4 address block is %d bytes, want at least %d", len(block), proxyV2INetLen)
		}
		return netip.AddrFrom4([4]byte(block[:4])), nil
	case family == proxyV2AFINet6 && proto == proxyV2ProtoStream:
		if len(block) < proxyV2INet6Len {
			return netip.Addr{}, fmt.Errorf("PROXY v2 IPv6 address block is %d bytes, want at least %d", len(block), proxyV2INet6Len)
		}
		// Unmapped for the same reason a socket address is: one host must not
		// get a second bucket by being written in the other family's form.
		return netip.AddrFrom16([16]byte(block[:16])).Unmap(), nil
	case family == proxyV2AFUnspec && proto == proxyV2ProtoUnspec:
		// The sender says it cannot report the original endpoints. It names no
		// client, which is served with no address rather than refused, for the
		// same reason the LOCAL command is.
		return netip.Addr{}, nil
	default:
		return netip.Addr{}, fmt.Errorf("unsupported PROXY v2 address family %#x and transport %#x", family, proto)
	}
}
