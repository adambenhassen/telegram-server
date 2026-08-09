package mtproto

import (
	"bytes"
	"encoding/binary"
	"net/netip"
	"testing"
	"time"
)

// TestReadProxyV2FamilyAndTransport pins the one decision the parser makes about
// whether a header names a client at all.
//
// In package, because the matrix is the point: family and transport are read
// together, and the combinations that are neither supported nor an explicit
// no-address case have to be refused rather than fall through either way.
// Driving twenty nibble pairs through sockets would prove the same thing far
// more slowly; the listener tests cover that the refusal reaches the connection.
func TestReadProxyV2FamilyAndTransport(t *testing.T) {
	t.Parallel()

	v4 := netip.MustParseAddr("203.0.113.9")
	v6 := netip.MustParseAddr("2001:db8::5")
	for _, tt := range []struct {
		name string
		// famProto is the header's family-and-transport byte.
		famProto byte
		addr     netip.Addr
		want     netip.Addr
		wantErr  bool
	}{
		{name: "inet stream", famProto: 0x11, addr: v4, want: v4},
		{name: "inet6 stream", famProto: 0x21, addr: v6, want: v6},
		{name: "unspec", famProto: 0x00, addr: netip.Addr{}},

		// The address is present and well formed in every case below. Crediting
		// it would attribute a limit to a peer the sender never claimed was a
		// TCP client.
		{name: "inet datagram", famProto: 0x12, addr: v4, wantErr: true},
		{name: "inet unspec transport", famProto: 0x10, addr: v4, wantErr: true},
		{name: "inet6 datagram", famProto: 0x22, addr: v6, wantErr: true},
		{name: "inet6 unspec transport", famProto: 0x20, addr: v6, wantErr: true},
		// AF_UNIX names a socket path, not a network peer, and used to land in
		// the no-address carve-out rather than being refused.
		{name: "unix stream", famProto: 0x31, addr: v4, wantErr: true},
		{name: "unspec stream", famProto: 0x01, addr: netip.Addr{}, wantErr: true},
		{name: "unknown family", famProto: 0x41, addr: v4, wantErr: true},
		{name: "unknown transport", famProto: 0x1f, addr: v4, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := readProxyV2(bytes.NewReader(testProxyV2Header(proxyV2CmdProxy, tt.famProto, tt.addr)))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("family/transport %#x was accepted, keyed on %s", tt.famProto, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("readProxyV2: %v", err)
			}
			if got != tt.want {
				t.Errorf("address %s, want %s", got, tt.want)
			}
		})
	}
}

// TestReadProxyV2LocalIgnoresTheAddressBlock covers the other accepted
// no-address case. LOCAL is what a balancer's own health check sends: the
// address block is present but means nothing, and reading it would credit a
// client to a connection that has none.
func TestReadProxyV2LocalIgnoresTheAddressBlock(t *testing.T) {
	t.Parallel()

	header := testProxyV2Header(proxyV2CmdLocal, 0x11, netip.MustParseAddr("203.0.113.9"))
	// Trailing byte the parser must not consume: LOCAL still declares its block
	// length, so the stream has to be left at the codec header like any other.
	r := bytes.NewReader(append(header, 0xef))
	got, err := readProxyV2(r)
	if err != nil {
		t.Fatalf("readProxyV2: %v", err)
	}
	if got.IsValid() {
		t.Errorf("LOCAL header credited address %s", got)
	}
	rest, err := r.ReadByte()
	if err != nil || rest != 0xef {
		t.Errorf("stream left at %#x (err %v), want the codec tag 0xef", rest, err)
	}
}

// testProxyV2Header builds a v2 header with an arbitrary family-and-transport
// byte, which is what lets the matrix above cover combinations no correct
// balancer emits.
func testProxyV2Header(cmd, famProto byte, addr netip.Addr) []byte {
	var h [proxyV2FixedLen]byte
	copy(h[:], proxyV2Signature)
	h[12] = 0x20 | cmd
	h[13] = famProto

	var body []byte
	switch {
	case !addr.IsValid():
	case addr.Is4():
		src, dst := addr.As4(), [4]byte{10, 0, 0, 1}
		body = append(append(body, src[:]...), dst[:]...)
	default:
		src, dst := addr.As16(), netip.MustParseAddr("2001:db8::1").As16()
		body = append(append(body, src[:]...), dst[:]...)
	}
	if len(body) > 0 {
		body = binary.BigEndian.AppendUint16(body, 51000)
		body = binary.BigEndian.AppendUint16(body, 2443)
		binary.BigEndian.PutUint16(h[14:16], uint16(len(body))) // #nosec G115 -- fixed 12 or 36.
	}
	return append(h[:], body...)
}

// TestLogSamplerCountsSuppressed is what the sampled log line is actually for.
//
// The bound alone would turn a flood into silence: an operator who sees one
// refusal cannot tell it from ten thousand. The count is the difference, and the
// listener test cannot reach it — all its refusals land in one window, so the
// line it reads carries a zero and proves only that the field exists. Two
// windows are needed, which is why this is here.
func TestLogSamplerCountsSuppressed(t *testing.T) {
	t.Parallel()

	const interval = time.Minute
	var s logSampler
	start := time.Now()

	if dropped, ok := s.allow(start, interval); !ok || dropped != 0 {
		t.Fatalf("first line: dropped=%d ok=%v, want 0 and emitted", dropped, ok)
	}
	// Everything else inside the window is suppressed and counted.
	const suppressed = 24
	for i := range suppressed {
		if dropped, ok := s.allow(start.Add(time.Duration(i)*time.Second), interval); ok {
			t.Fatalf("line %d was emitted inside the window (dropped=%d)", i, dropped)
		}
	}

	// The next window reports what it stood for.
	if dropped, ok := s.allow(start.Add(interval), interval); !ok || dropped != suppressed {
		t.Fatalf("second line: dropped=%d ok=%v, want %d and emitted", dropped, ok, suppressed)
	}
	// And the counter starts again, so the next line is not cumulative.
	if dropped, ok := s.allow(start.Add(2*interval), interval); !ok || dropped != 0 {
		t.Fatalf("third line: dropped=%d ok=%v, want 0 and emitted", dropped, ok)
	}
}
