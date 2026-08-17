package mtproto_test

import (
	"context"
	"crypto/rand"
	"fmt"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/mtproxy"
	"github.com/gotd/td/mtproxy/obfuscator"
	"github.com/gotd/td/proto/codec"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/transport"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

// Every official Telegram client obfuscates its stream: it opens with 64 bytes
// that carry the codec tag encrypted inside them instead of writing the tag in
// the clear. The clients this server already serves write the tag in the clear.
// Both arrive on the same port, neither announces which it is, so the framing
// has to be told apart from the bytes themselves.
//
// What tells them apart is what the obfuscated header is generated to avoid: it
// never starts with an abridged, intermediate or padded-intermediate tag, and
// its second little-endian word is never zero — which is exactly the sequence
// number of a full-framed connection's first frame. Those two facts are the
// whole of the detection, and the test below is what holds them.

// obfuscatedDC is the datacentre id a client writes into its obfuscation
// header. The server reads the codec tag out of that header and nothing else,
// so the value only has to be one a client would plausibly send.
const obfuscatedDC = 2

// TestServeNegotiatesBothFramings is the one test both framings have to pass.
// Removing obfuscation support fails its obfuscated half; detecting it by
// wrapping every connection — the shape gotd's own listener offers — fails the
// plaintext half. Neither may be traded for the other: the same listener serves
// stock clients and the clients this server already had.
func TestServeNegotiatesBothFramings(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	seen, key := serveClients(t, ctx, nl, nil, nil)

	// Full is plaintext-only by construction: it has no protocol tag, so there
	// is nothing for an obfuscation header to carry. It is here because it is
	// the framing the detection has to keep distinguishing from an obfuscated
	// header — the two are alike in every byte but the sequence number.
	for _, tt := range []struct {
		name       string
		proto      transport.Protocol
		obfuscated bool
	}{
		{name: "plaintext abridged", proto: transport.Abridged},
		{name: "plaintext intermediate", proto: transport.Intermediate},
		{name: "plaintext padded intermediate", proto: transport.PaddedIntermediate},
		{name: "plaintext full", proto: transport.Full},
		{name: "obfuscated abridged", proto: transport.Abridged, obfuscated: true},
		{name: "obfuscated intermediate", proto: transport.Intermediate, obfuscated: true},
		{name: "obfuscated padded intermediate", proto: transport.PaddedIntermediate, obfuscated: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.obfuscated {
				speakObfuscated(t, ctx, nl.Addr().String(), tt.proto, key)
			} else {
				speak(t, ctx, nl.Addr().String(), nil, tt.proto, key)
			}
			// Asserted at the handler rather than at the socket: a codec picked
			// on the wrong byte still negotiates, and only fails once a frame
			// has to decode under it.
			if got := wantClientAddr(t, ctx, seen); !got.IsValid() {
				t.Errorf("request reached the handler with no client address")
			}
		})
	}
}

// TestServeClosesUnnegotiableFraming covers the input the detection actually
// gets from an unauthenticated peer: a prefix that stops partway through, or
// one that is neither framing. Each has to end in a closed socket rather than a
// goroutine parked on a read forever, and none of them may cost anybody else
// their connection.
func TestServeClosesUnnegotiableFraming(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// Short enough that a stalled negotiation is observed being dropped inside
	// the test's budget; production bounds the same read, further out.
	const handshake = 2 * time.Second
	nl := mustListenTCP(t, ctx, "127.0.0.1:0")
	seen, key := serveClients(t, ctx, nl, nil, nil, func(s *mtproto.Server) {
		s.SetHandshakeTimeout(handshake)
	})

	header := obfuscatedHeader(t)
	for _, tt := range []struct {
		name   string
		prefix []byte
	}{
		{name: "nothing at all"},
		{name: "one byte of an obfuscation header", prefix: header[:1]},
		{name: "less than the discriminator", prefix: header[:7]},
		{name: "the discriminator and no more", prefix: header[:8]},
		{name: "an obfuscation header one byte short", prefix: header[:63]},
		{name: "a truncated intermediate tag", prefix: []byte{0xee, 0xee}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := dialClient(t, ctx, nl.Addr().String())
			mustWrite(t, conn, tt.prefix)
			// The server closes it once the bound elapses: the read ends
			// instead of holding the connection for as long as the peer cares
			// to keep it open.
			assertClosed(t, conn)
			wantNoClient(t, seen)
		})
	}

	// And the port still serves, in both framings, after all of that.
	speak(t, ctx, nl.Addr().String(), nil, transport.Abridged, key)
	if got := wantClientAddr(t, ctx, seen); !got.IsValid() {
		t.Errorf("plaintext client served with no address after the stalled peers")
	}
	speakObfuscated(t, ctx, nl.Addr().String(), transport.Abridged, key)
	if got := wantClientAddr(t, ctx, seen); !got.IsValid() {
		t.Errorf("obfuscated client served with no address after the stalled peers")
	}
}

// speakObfuscated completes a connection the way a stock Telegram client does:
// the obfuscation header, carrying the codec tag inside it, then one encrypted
// frame over the deobfuscated stream. It is speak with the framing swapped, and
// the frame is what makes the connection observable at the handler.
func speakObfuscated(t *testing.T, ctx context.Context, addr string, proto transport.Protocol, key crypto.AuthKey) net.Conn {
	t.Helper()
	raw := dialClient(t, ctx, addr)
	client, err := obfuscatedHandshake(raw, proto)
	if err != nil {
		t.Fatalf("obfuscated handshake: %v", err)
	}
	frame := clientFrame(t, key, 42, int64(1)<<32, &tg.HelpGetConfigRequest{})
	if err := client.Send(ctx, &bin.Buffer{Buf: slices.Clone(frame)}); err != nil {
		t.Fatalf("send frame: %v", err)
	}
	return raw
}

// obfuscatedHandshake wraps raw in obfuscated2 and negotiates proto over it.
// The codec writes no header of its own: its tag travelled inside the
// obfuscation header, which is what the server reads it back out of.
func obfuscatedHandshake(raw net.Conn, proto transport.Protocol) (transport.Conn, error) {
	cdc := proto.Codec()
	tagged, ok := cdc.(codec.TaggedCodec)
	if !ok {
		return nil, fmt.Errorf("%T carries no obfuscation tag", cdc)
	}
	obfs := obfuscator.Obfuscated2(rand.Reader, raw)
	if err := obfs.Handshake(tagged.ObfuscatedTag(), obfuscatedDC, mtproxy.Secret{}); err != nil {
		return nil, fmt.Errorf("write obfuscation header: %w", err)
	}
	conn, err := transport.NewProtocol(func() transport.Codec {
		return codec.NoHeader{Codec: cdc}
	}).Handshake(obfs)
	if err != nil {
		return nil, fmt.Errorf("negotiate %T: %w", cdc, err)
	}
	return conn, nil
}

// obfuscatedHeader returns the 64 bytes a client opens an obfuscated
// connection with, so a test can send a prefix of a real one rather than
// bytes that only look like one.
func obfuscatedHeader(t *testing.T) []byte {
	t.Helper()
	var buf headerRecorder
	obfs := obfuscator.Obfuscated2(rand.Reader, &buf)
	if err := obfs.Handshake(codec.Abridged{}.ObfuscatedTag(), obfuscatedDC, mtproxy.Secret{}); err != nil {
		t.Fatalf("generate obfuscation header: %v", err)
	}
	if len(buf.written) != 64 {
		t.Fatalf("obfuscation header is %d bytes, want 64", len(buf.written))
	}
	return buf.written
}

// headerRecorder is the net.Conn an obfuscator writes its header to when the
// header itself, and not a connection, is what is wanted.
type headerRecorder struct {
	net.Conn

	written []byte
}

func (w *headerRecorder) Write(p []byte) (int, error) {
	w.written = append(w.written, p...)
	return len(p), nil
}
