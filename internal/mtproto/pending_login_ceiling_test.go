package mtproto_test

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/exchange"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
)

var errPendingDeadlineTooLong = errors.New("pending-login deadline was not installed")

// pendingFrameConn records the read deadlines the server applies and can hold a
// read until its context expires. It is deliberately a transport.Conn rather
// than a net.Conn so the test observes the deadline at the same boundary the
// production read loop uses.
type pendingFrameConn struct {
	frames  [][]byte
	delays  []time.Duration
	blockAt int
	ready   chan<- struct{}
	block   <-chan struct{}
	// A non-zero value makes a blocked read fail immediately when the server
	// gives it a deadline farther away than this. It keeps a missing pending
	// deadline test fast instead of waiting for the production 30-second timeout.
	maxRemaining time.Duration

	mu        sync.Mutex
	i         int
	deadlines []time.Time
	readyOnce sync.Once
	closed    atomic.Bool
}

func (c *pendingFrameConn) Recv(ctx context.Context, b *bin.Buffer) error {
	c.mu.Lock()
	i := c.i
	c.i++
	deadline, _ := ctx.Deadline()
	c.deadlines = append(c.deadlines, deadline)
	c.mu.Unlock()

	if i == c.blockAt {
		c.readyOnce.Do(func() {
			if c.ready != nil {
				c.ready <- struct{}{}
			}
		})
		if c.maxRemaining > 0 && !deadline.IsZero() && time.Until(deadline) > c.maxRemaining {
			return errPendingDeadlineTooLong
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.block:
			return io.EOF
		}
	}
	if i >= len(c.frames) {
		return io.EOF
	}
	if i < len(c.delays) && c.delays[i] > 0 {
		timer := time.NewTimer(c.delays[i])
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
	}
	b.ResetTo(slices.Clone(c.frames[i]))
	return nil
}

func (c *pendingFrameConn) Send(context.Context, *bin.Buffer) error { return nil }

func (c *pendingFrameConn) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *pendingFrameConn) readDeadlines() []time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.deadlines)
}

func TestPendingLoginUsesAnAbsoluteReadCeiling(t *testing.T) {
	t.Parallel()

	key := rebindTestKey()
	keys := mtproto.NewMemoryAuthKeyStore()
	if err := keys.Save(context.Background(), key); err != nil {
		t.Fatalf("save key: %v", err)
	}

	const lease = 300 * time.Millisecond
	var calls atomic.Int32
	var markedAt time.Time
	h := mtproto.HandlerFunc(func(c *mtproto.Conn, _ *mtproto.Request) error {
		if calls.Add(1) == 1 {
			markedAt = time.Now()
			c.MarkPendingLogin()
		}
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, h, nil)
	srv.SetPendingLoginLifetime(lease)
	conn := &pendingFrameConn{
		frames: [][]byte{
			clientFrame(t, key, 42, 1<<32, &tg.AccountRegisterDeviceRequest{}),
			clientFrame(t, key, 42, 2<<32, &tg.AccountRegisterDeviceRequest{}),
			clientFrame(t, key, 42, 3<<32, &tg.AccountRegisterDeviceRequest{}),
		},
		blockAt:      -1,
		maxRemaining: lease * 2,
	}
	if err := srv.ServeConn(context.Background(), conn); !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn = %v, want EOF after scripted frames", err)
	}

	deadlines := conn.readDeadlines()
	if len(deadlines) != 4 {
		t.Fatalf("recorded %d read deadlines, want 4", len(deadlines))
	}
	if calls.Load() != 3 {
		t.Fatalf("handler saw %d frames, want 3", calls.Load())
	}
	if deadlines[1].IsZero() || deadlines[2].IsZero() {
		t.Fatal("pending reads did not receive an absolute deadline")
	}
	if remaining := deadlines[1].Sub(markedAt); remaining < lease-25*time.Millisecond || remaining > lease+25*time.Millisecond {
		t.Fatalf("pending deadline = %s after transition, want about %s", remaining, lease)
	}
	if delta := deadlines[2].Sub(deadlines[1]); delta > time.Millisecond || delta < -time.Millisecond {
		t.Fatalf("pending deadline moved by %s after another frame, want no refresh", delta)
	}
	if delta := deadlines[3].Sub(deadlines[1]); delta > time.Millisecond || delta < -time.Millisecond {
		t.Fatalf("pending deadline moved by %s after the scripted frames, want no refresh", delta)
	}
}

func TestNonPendingConnectionsKeepPerFrameReadTimeout(t *testing.T) {
	t.Parallel()

	for name, userID := range map[string]int64{
		"unbound":       0,
		"authenticated": 7,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			key := rebindTestKey()
			ks := &statusKeyStore{key: key, users: []int64{userID}}
			h := mtproto.HandlerFunc(func(_ *mtproto.Conn, _ *mtproto.Request) error {
				return nil
			})
			srv := mtproto.New(exchange.PrivateKey{}, 2, ks, h, nil)
			conn := &pendingFrameConn{
				frames:  [][]byte{clientFrame(t, key, 42, 1<<32, &tg.AccountRegisterDeviceRequest{})},
				blockAt: -1,
			}
			if err := srv.ServeConn(context.Background(), conn); !errors.Is(err, io.EOF) {
				t.Fatalf("ServeConn = %v, want EOF after scripted frames", err)
			}
			deadlines := conn.readDeadlines()
			if len(deadlines) != 2 {
				t.Fatalf("recorded %d read deadlines, want 2", len(deadlines))
			}
			if remaining := time.Until(deadlines[1]); remaining < 29*time.Second || remaining > 30*time.Second {
				t.Fatalf("next non-pending read deadline has %s remaining, want the 30-second timeout", remaining)
			}
			if delta := deadlines[1].Sub(deadlines[0]); delta > 100*time.Millisecond || delta < -100*time.Millisecond {
				t.Fatalf("per-frame deadline moved by %s between reads, want a fresh 30-second deadline", delta)
			}
		})
	}
}

func TestPendingLoginConnectionSurvivesInterframeReadTimeout(t *testing.T) {
	t.Parallel()

	key := rebindTestKey()
	keys := mtproto.NewMemoryAuthKeyStore()
	if err := keys.Save(context.Background(), key); err != nil {
		t.Fatalf("save key: %v", err)
	}

	const lease = 400 * time.Millisecond
	var calls atomic.Int32
	var markedAt time.Time
	h := mtproto.HandlerFunc(func(c *mtproto.Conn, _ *mtproto.Request) error {
		if calls.Add(1) == 1 {
			markedAt = time.Now()
			c.MarkPendingLogin()
		}
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, h, nil)
	srv.SetPendingLoginLifetime(lease)
	conn := &pendingFrameConn{
		frames: [][]byte{
			clientFrame(t, key, 42, 1<<32, &tg.AccountRegisterDeviceRequest{}),
			clientFrame(t, key, 42, 2<<32, &tg.AccountRegisterDeviceRequest{}),
		},
		delays:       []time.Duration{0, 100 * time.Millisecond},
		blockAt:      -1,
		maxRemaining: lease * 2,
	}
	if err := srv.ServeConn(context.Background(), conn); !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn = %v, want EOF after the interframe gap", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("handler saw %d frames, want the frame delivered after the gap", calls.Load())
	}
	deadlines := conn.readDeadlines()
	if len(deadlines) != 3 {
		t.Fatalf("recorded %d read deadlines, want 3", len(deadlines))
	}
	if remaining := deadlines[1].Sub(markedAt); remaining > lease+25*time.Millisecond || remaining < lease-25*time.Millisecond {
		t.Fatalf("pending read deadline is %s after transition, want about %s", remaining, lease)
	}
	if delta := deadlines[2].Sub(deadlines[1]); delta > time.Millisecond || delta < -time.Millisecond {
		t.Fatalf("pending deadline moved by %s after the interframe gap, want no refresh", delta)
	}
}

func TestPendingLoginCeilingClosesHeldConnection(t *testing.T) {
	t.Parallel()

	key := rebindTestKey()
	keys := mtproto.NewMemoryAuthKeyStore()
	if err := keys.Save(context.Background(), key); err != nil {
		t.Fatalf("save key: %v", err)
	}

	const lease = 150 * time.Millisecond
	h := mtproto.HandlerFunc(func(c *mtproto.Conn, _ *mtproto.Request) error {
		c.MarkPendingLogin()
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, h, nil)
	srv.SetPendingLoginLifetime(lease)
	conn := &pendingFrameConn{
		frames:       [][]byte{clientFrame(t, key, 42, 1<<32, &tg.AccountRegisterDeviceRequest{})},
		blockAt:      1,
		maxRemaining: lease * 2,
	}
	if err := srv.ServeConn(context.Background(), conn); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ServeConn = %v, want pending ceiling timeout", err)
	}
	if !conn.closed.Load() {
		t.Fatal("pending ceiling did not close the transport")
	}
}

func TestPendingLoginCapClosesNewAttemptAndReleasesOnExit(t *testing.T) {
	t.Parallel()

	key := rebindTestKey()
	keys := mtproto.NewMemoryAuthKeyStore()
	if err := keys.Save(context.Background(), key); err != nil {
		t.Fatalf("save key: %v", err)
	}
	h := mtproto.HandlerFunc(func(c *mtproto.Conn, _ *mtproto.Request) error {
		c.MarkPendingLogin()
		return nil
	})
	srv := mtproto.New(exchange.PrivateKey{}, 2, keys, h, nil)
	if err := srv.SetMaxPendingLoginConns(1); err != nil {
		t.Fatalf("set pending-login cap: %v", err)
	}

	firstReady := make(chan struct{}, 1)
	first := &pendingFrameConn{
		frames:  [][]byte{clientFrame(t, key, 42, 1<<32, &tg.AccountRegisterDeviceRequest{})},
		blockAt: 1,
		ready:   firstReady,
	}
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- srv.ServeConn(firstCtx, first) }()
	select {
	case <-firstReady:
	case <-time.After(time.Second):
		cancelFirst()
		t.Fatal("first pending connection did not claim its slot")
	}

	second := &pendingFrameConn{
		frames:  [][]byte{clientFrame(t, key, 42, 2<<32, &tg.AccountRegisterDeviceRequest{})},
		blockAt: -1,
	}
	if err := srv.ServeConn(context.Background(), second); err != nil {
		t.Fatalf("second pending attempt = %v, want immediate close", err)
	}
	if !second.closed.Load() {
		t.Fatal("pending cap exhaustion did not close the new connection")
	}

	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first pending connection = %v, want cancellation after release", err)
	}

	third := &pendingFrameConn{
		frames:  [][]byte{clientFrame(t, key, 42, 3<<32, &tg.AccountRegisterDeviceRequest{})},
		blockAt: -1,
	}
	if err := srv.ServeConn(context.Background(), third); !errors.Is(err, io.EOF) {
		t.Fatalf("third pending attempt = %v, want admission after first exit", err)
	}
}
