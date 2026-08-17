package mtproto

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/gotd/td/proto/codec"
)

// A stock Telegram client always obfuscates its stream. It opens with 64 bytes
// whose last eight are encrypted, and the codec tag it would otherwise have
// written in the clear travels inside them. The clients this server already
// serves write that tag in the clear. Both arrive on the same port, neither
// declares which it is, so the framing is decided from the opening bytes and
// from nothing else.
//
// Wrapping every connection in deobfuscation — what gotd's obfuscated listener
// does on its own — is not a way to serve both: it would read a plaintext tag
// as the first bytes of a header and turn a working client into a stream of
// garbage. Sniffing is what makes one listener serve both.
//
// The sniff is possible because the obfuscation header is generated to be
// unmistakable. Every client rejects and regenerates a header that begins with
// a codec tag, and one whose second little-endian word is zero. That second
// word is where full framing puts the sequence number of its first frame, which
// is always zero. So four cases exhaust the opening bytes:
//
//   - 0xef                     abridged, in the clear
//   - 0xeeeeeeee / 0xdddddddd  intermediate or padded intermediate, in the clear
//   - second word zero         full framing, in the clear
//   - anything else            obfuscated
//
// The last case is a fallback rather than a positive identification, exactly as
// full framing was the fallback before it. A peer whose bytes are neither ends
// up deobfuscated into nonsense and fails on its first frame, which is what a
// peer speaking no known transport did already.

// framingPrefixLen is the most that has to be read to decide, and is read only
// when the shorter answers do not apply. It is the two little-endian words the
// decision is made of, and no client sends fewer before its first frame: the
// shortest first frame of the shortest framing is longer, and an obfuscation
// header is 64 bytes.
const framingPrefixLen = 8

// sniffFraming decides whether sock is obfuscated from its opening bytes, and
// returns a connection that reads as though none of them had been consumed.
//
// It reads in the same three steps the plaintext detection does, so a framing
// that can be named from one byte costs one byte: a client that writes its tag
// and pauses before its first frame is not made to wait for bytes it has not
// sent yet. Only the ambiguous case reads all eight.
//
// The read is bounded by the handshake deadline the caller has already set, and
// the buffer is fixed, so a peer that sends a short prefix and stops costs this
// server one expired read and nothing else.
func sniffFraming(sock net.Conn) (bool, net.Conn, error) {
	var (
		head [framingPrefixLen]byte
		read int
	)
	// The connection is handed back on every path, error included: the prefix
	// has left the socket, and whoever closes it should be closing the same
	// stream this server was reading.
	prefixed := func(obfuscated bool, err error) (bool, net.Conn, error) {
		return obfuscated, &replayConn{Conn: sock, r: io.MultiReader(bytes.NewReader(head[:read]), sock)}, err
	}
	fill := func(n int) error {
		got, err := io.ReadFull(sock, head[read:n])
		read += got
		if err != nil {
			return fmt.Errorf("read %d framing bytes: %w", n, err)
		}
		return nil
	}

	if err := fill(1); err != nil {
		return prefixed(false, err)
	}
	if head[0] == codec.AbridgedClientStart[0] {
		return prefixed(false, nil)
	}
	if err := fill(4); err != nil {
		return prefixed(false, err)
	}
	switch [4]byte(head[:4]) {
	case codec.IntermediateClientStart, codec.PaddedIntermediateClientStart:
		return prefixed(false, nil)
	}
	if err := fill(framingPrefixLen); err != nil {
		return prefixed(false, err)
	}
	// The sequence number of a full-framed first frame, which an obfuscation
	// header is generated never to look like.
	if binary.LittleEndian.Uint32(head[4:framingPrefixLen]) == 0 {
		return prefixed(false, nil)
	}
	return prefixed(true, nil)
}

// replayConn puts bytes already read off a connection back in front of it, so
// the next reader sees the stream from its first byte. Everything but reading
// is the connection's own: the deadline the caller set still applies, and
// closing it closes the socket.
type replayConn struct {
	net.Conn

	r io.Reader
}

func (c *replayConn) Read(b []byte) (int, error) { return c.r.Read(b) }
