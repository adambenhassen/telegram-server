package mtproto

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"time"

	"github.com/adambenhassen/telegram-server/internal/discovery"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// DiscoveryLimits bounds valid local-direct requests admitted to the discovery
// response path. The existing pre-auth connection bounds cover sockets while a
// request is slow or partial; these fixed windows additionally bound the CPU
// and response work a peer can trigger after sending a valid request.
type DiscoveryLimits struct {
	// MaxRequests is the process-wide number of valid requests admitted to the
	// response path in Window. Zero disables this bound.
	MaxRequests int
	// MaxRequestsPerNet is the number of valid requests from one client network
	// admitted to the response path in PerNetWindow. Zero disables this bound.
	MaxRequestsPerNet int
	// Window is the process-wide fixed window.
	Window time.Duration
	// PerNetWindow is the per-network fixed window.
	PerNetWindow time.Duration
}

// DefaultDiscoveryLimits returns the enabled-by-default process and
// per-network request bounds.
func DefaultDiscoveryLimits() DiscoveryLimits {
	return DiscoveryLimits{
		MaxRequests:       60,
		MaxRequestsPerNet: 10,
		Window:            time.Minute,
		PerNetWindow:      time.Minute,
	}
}

// SetDiscoveryLimits replaces the local-direct discovery rate limits. Call it
// before Serve. A zero limit disables its bound; a negative value or a
// non-positive window for an enabled bound is rejected so a typo cannot make a
// safety control silently disappear.
func (s *Server) SetDiscoveryLimits(l DiscoveryLimits) error {
	if l.MaxRequests < 0 {
		return fmt.Errorf("discovery MaxRequests is %d: must not be negative, and 0 disables the cap", l.MaxRequests)
	}
	if l.MaxRequestsPerNet < 0 {
		return fmt.Errorf("discovery MaxRequestsPerNet is %d: must not be negative, and 0 disables the cap", l.MaxRequestsPerNet)
	}
	if l.Window < 0 || l.PerNetWindow < 0 {
		return errors.New("discovery rate-limit windows must not be negative")
	}
	if l.MaxRequests > 0 && l.Window <= 0 {
		return errors.New("discovery Window must be positive when MaxRequests is enabled")
	}
	if l.MaxRequestsPerNet > 0 && l.PerNetWindow <= 0 {
		return errors.New("discovery PerNetWindow must be positive when MaxRequestsPerNet is enabled")
	}
	s.discovery = newDiscoveryLimiter(l)
	return nil
}

type discoveryLimiter struct {
	limits DiscoveryLimits

	mu     sync.Mutex
	global discoveryWindow
	perNet map[netip.Prefix]discoveryWindow
	// maxBuckets prevents an operator who disables the global limit from
	// making memory proportional to the number of distinct source networks.
	maxBuckets int
}

type discoveryWindow struct {
	start time.Time
	calls int
}

const maxDiscoveryBuckets = 4096

func newDiscoveryLimiter(limits DiscoveryLimits) *discoveryLimiter {
	return &discoveryLimiter{
		limits:     limits,
		perNet:     make(map[netip.Prefix]discoveryWindow),
		maxBuckets: maxDiscoveryBuckets,
	}
}

// allow reserves one response slot atomically across the global and network
// windows. The source network is the same /32-or-/64 bucket used by the
// pre-auth and RPC per-address controls. An unaddressable connection can only
// consume the process-wide bound.
func (l *discoveryLimiter) allow(addr netip.Addr, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	global := l.global
	if l.limits.MaxRequests > 0 && (global.start.IsZero() || !now.Before(global.start.Add(l.limits.Window))) {
		global = discoveryWindow{start: now}
	}
	if l.limits.MaxRequests > 0 && global.calls >= l.limits.MaxRequests {
		return false
	}

	var bucket netip.Prefix
	var network discoveryWindow
	hasNetwork := false
	if l.limits.MaxRequestsPerNet > 0 {
		if bucket, hasNetwork = store.IPBucketKey(addr); hasNetwork {
			network = l.perNet[bucket]
			if network.start.IsZero() || !now.Before(network.start.Add(l.limits.PerNetWindow)) {
				network = discoveryWindow{start: now}
			}
			if network.calls >= l.limits.MaxRequestsPerNet {
				return false
			}
		}
	}
	if hasNetwork && len(l.perNet) >= l.maxBuckets && l.perNet[bucket].start.IsZero() {
		l.prune(now)
		if len(l.perNet) >= l.maxBuckets {
			return false
		}
	}

	if l.limits.MaxRequests > 0 {
		global.calls++
		l.global = global
	}
	if hasNetwork {
		network.calls++
		l.perNet[bucket] = network
	}
	return true
}

func (l *discoveryLimiter) prune(now time.Time) {
	for bucket, window := range l.perNet {
		if window.start.IsZero() || !now.Before(window.start.Add(l.limits.PerNetWindow)) {
			delete(l.perNet, bucket)
		}
	}
}

type preflightProbe struct {
	matched   bool
	malformed bool
	nonce     [discovery.NonceSize]byte
	stream    net.Conn
}

// probePreflight reads the local-direct request and its EOF delimiter. A
// non-match carries every byte back through replayConn so transport sniffing
// sees exactly the stream it would have seen without discovery enabled.
func probePreflight(sock net.Conn, deadline time.Time) (preflightProbe, error) {
	request := make([]byte, discovery.RequestMagicSize+discovery.NonceSize)
	magic := []byte(discovery.RequestMagic)
	read := 0
	replay := func() preflightProbe {
		return preflightProbe{stream: &replayConn{
			Conn: sock,
			r:    io.MultiReader(bytes.NewReader(request[:read]), sock),
		}}
	}

	for read < discovery.RequestMagicSize {
		n, err := sock.Read(request[read : read+1])
		read += n
		if read > 0 && !bytes.Equal(request[:read], magic[:read]) {
			return replay(), nil
		}
		if err != nil {
			return replay(), fmt.Errorf("read discovery magic: %w", err)
		}
		if n == 0 {
			return preflightProbe{}, io.ErrNoProgress
		}
	}

	if _, err := io.ReadFull(sock, request[discovery.RequestMagicSize:]); err != nil {
		// The complete magic is a discovery prefix, but a short nonce is not a
		// transport discriminator. Close it as malformed rather than handing
		// the magic to MTProto with the nonce bytes silently discarded.
		return preflightProbe{malformed: true, stream: sock}, fmt.Errorf("read discovery nonce: %w", err)
	}
	var nonce [discovery.NonceSize]byte
	copy(nonce[:], request[discovery.RequestMagicSize:])
	// The request has no length field, so the only delimiter that can prove it
	// is complete is EOF from the client's TCP write half. Read through the
	// original handshake deadline: a delayed trailing byte is malformed, and a
	// peer that never reaches EOF is malformed rather than receiving a response.
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		return preflightProbe{malformed: true, stream: sock}, errors.New("preflight handshake deadline expired")
	}
	var trailing [1]byte
	n, err := sock.Read(trailing[:])
	if n > 0 {
		return preflightProbe{malformed: true, stream: sock}, nil
	}
	if errors.Is(err, io.EOF) {
		return preflightProbe{matched: true, nonce: nonce, stream: sock}, nil
	}
	if err == nil {
		return preflightProbe{malformed: true, stream: sock}, io.ErrNoProgress
	}
	return preflightProbe{malformed: true, stream: sock}, fmt.Errorf("wait for discovery request EOF: %w", err)
}

// servePreflight writes one bounded response and closes the anonymous socket.
// It deliberately runs before codec detection and never touches auth-key,
// session, handler or store state. deadline is the original absolute
// pre-auth deadline, which the response may not extend.
func (s *Server) servePreflight(ctx context.Context, sock net.Conn, nonce [discovery.NonceSize]byte, deadline time.Time) error {
	stop := context.AfterFunc(ctx, func() {
		if closeErr := sock.Close(); closeErr != nil && !isDisconnect(closeErr) {
			s.log.Info("close discovery connection at shutdown", "err", closeErr)
		}
	})
	defer stop()
	defer func() {
		if closeErr := sock.Close(); closeErr != nil && !isDisconnect(closeErr) {
			s.log.Info("close discovery connection", "err", closeErr)
		}
	}() // the response socket has no next protocol

	writeDeadline := time.Now().Add(s.writeTimeout)
	if !deadline.IsZero() && deadline.Before(writeDeadline) {
		writeDeadline = deadline
	}
	if err := sock.SetWriteDeadline(writeDeadline); err != nil {
		return fmt.Errorf("set discovery write deadline: %w", err)
	}
	if s.key.RSA == nil {
		return errors.New("discovery server RSA key is nil")
	}
	response, err := discovery.BuildPreflightResponse(s.dcID, &s.key.RSA.PublicKey, nonce[:])
	if err != nil {
		return err
	}
	for len(response) > 0 {
		n, err := sock.Write(response)
		if n > 0 {
			response = response[n:]
		}
		if err != nil {
			return fmt.Errorf("write discovery response: %w", err)
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
