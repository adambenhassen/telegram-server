package api

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/crypto"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// fakePushConn is a pushConn that records what it was handed and keeps its own
// watermark, standing in for a live socket without a transport or a database.
type fakePushConn struct {
	pts  int
	got  []*tg.Updates
	fail error
}

func (f *fakePushConn) LastPushedPts() int { return f.pts }

func (f *fakePushConn) PushTo(_ context.Context, _ int64, enc bin.Encoder, pts int) (bool, error) {
	if f.fail != nil {
		return false, f.fail
	}
	ups, ok := enc.(*tg.Updates)
	if !ok {
		return false, errors.New("unexpected encoder")
	}
	f.got = append(f.got, ups)
	f.pts = pts
	return true, nil
}

type rpcBatchPushConn struct {
	fakePushConn

	authKeyID int64
	marked    []int
}

type pendingRPCPushConn struct {
	fakePushConn

	pendingPts int
}

func (f *pendingRPCPushConn) PendingRPCUpdate(int64) (int, bool) {
	return f.pendingPts, f.pendingPts != 0
}

type pendingRPCBatchPushConn struct {
	fakePushConn

	authKeyID  int64
	pendingPts int
	marked     []int
}

type flakyPendingRPCBatchPushConn struct {
	pendingRPCBatchPushConn

	failures int
}

func (f *flakyPendingRPCBatchPushConn) PushTo(ctx context.Context, owner int64, enc bin.Encoder, pts int) (bool, error) {
	if f.failures > 0 {
		f.failures--
		return false, errors.New("prefix write")
	}
	return f.pendingRPCBatchPushConn.PushTo(ctx, owner, enc, pts)
}

type blockedResultTransport struct {
	mu      sync.Mutex
	sends   int
	entered chan struct{}
	release chan struct{}
}

type queuedRPCBatchPushConn struct {
	mu          sync.Mutex
	pts         int
	got         []*tg.Updates
	authKeyID   int64
	pending     []int
	marked      []int
	markEntered chan struct{}
	releaseMark chan struct{}
	markOnce    sync.Once
}

func (f *queuedRPCBatchPushConn) LastPushedPts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pts
}

func (f *queuedRPCBatchPushConn) PushTo(_ context.Context, _ int64, enc bin.Encoder, pts int) (bool, error) {
	ups, ok := enc.(*tg.Updates)
	if !ok {
		return false, errors.New("unexpected encoder")
	}
	f.mu.Lock()
	f.got = append(f.got, ups)
	f.pts = pts
	f.mu.Unlock()
	return true, nil
}

func (f *queuedRPCBatchPushConn) PendingRPCUpdate(int64) (int, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) == 0 {
		return 0, false
	}
	return f.pending[0], true
}

func (f *queuedRPCBatchPushConn) PendingRPCUpdateReady(int64) bool { return false }

func (f *queuedRPCBatchPushConn) AuthKeyID() int64 { return f.authKeyID }

func (f *queuedRPCBatchPushConn) MarkRPCUpdate(owner, authKeyID int64, pts int) bool {
	if owner != 7 || authKeyID != f.authKeyID {
		return false
	}
	if f.markEntered != nil {
		f.markOnce.Do(func() { close(f.markEntered) })
		<-f.releaseMark
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.pending) == 0 || f.pending[0] != pts {
		return false
	}
	f.marked = append(f.marked, pts)
	if pts > f.pts {
		f.pts = pts
	}
	f.pending = f.pending[1:]
	return true
}

func (t *blockedResultTransport) Send(context.Context, *bin.Buffer) error {
	t.mu.Lock()
	t.sends++
	first := t.sends == 1
	t.mu.Unlock()
	if first {
		close(t.entered)
		<-t.release
	}
	return nil
}

func (*blockedResultTransport) Recv(context.Context, *bin.Buffer) error { return errors.New("unused") }
func (*blockedResultTransport) Close() error                            { return nil }

func (t *blockedResultTransport) sendCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.sends
}

func (f *pendingRPCBatchPushConn) PendingRPCUpdate(int64) (int, bool) {
	return f.pendingPts, f.pendingPts != 0
}

func (f *pendingRPCBatchPushConn) AuthKeyID() int64 { return f.authKeyID }

func (f *pendingRPCBatchPushConn) MarkRPCUpdate(owner, authKeyID int64, pts int) bool {
	if owner != 7 || authKeyID != f.authKeyID {
		return false
	}
	f.marked = append(f.marked, pts)
	if pts > f.pts {
		f.pts = pts
	}
	if f.pendingPts == pts {
		f.pendingPts = 0
	}
	return true
}

func (f *rpcBatchPushConn) AuthKeyID() int64 { return f.authKeyID }

func (f *rpcBatchPushConn) MarkRPCUpdate(owner, authKeyID int64, pts int) bool {
	if owner != 7 || authKeyID != f.authKeyID {
		return false
	}
	f.marked = append(f.marked, pts)
	if pts > f.pts {
		f.pts = pts
	}
	return true
}

type replyOrderTransport struct {
	events *[]string
}

func (t *replyOrderTransport) Send(context.Context, *bin.Buffer) error {
	*t.events = append(*t.events, "send")
	return nil
}

func (*replyOrderTransport) Recv(context.Context, *bin.Buffer) error { return errors.New("unused") }
func (*replyOrderTransport) Close() error                            { return nil }

type replyFailureTransport struct{}

func (replyFailureTransport) Send(context.Context, *bin.Buffer) error { return errors.New("write") }
func (replyFailureTransport) Recv(context.Context, *bin.Buffer) error { return errors.New("unused") }
func (replyFailureTransport) Close() error                            { return nil }

func replyTestKey() crypto.AuthKey {
	var raw crypto.Key
	for i := range raw {
		raw[i] = byte(i)
	}
	return raw.WithID()
}

type outcomePushConn struct {
	pushed   bool
	err      error
	pts      int
	attempts int
	onPush   func()
}

func (f *outcomePushConn) LastPushedPts() int { return f.pts }

func (f *outcomePushConn) PushTo(_ context.Context, _ int64, _ bin.Encoder, pts int) (bool, error) {
	f.attempts++
	if f.onPush != nil {
		f.onPush()
	}
	if f.err != nil {
		return false, f.err
	}
	if !f.pushed {
		return false, nil
	}
	f.pts = pts
	return true, nil
}

// batch builds a batch of one update per pts in (from, to], with head the
// user's pts before truncation: a batch stopping short of it is a truncated one
// advertising only the last event it carries, as buildUpdates does.
func batch(from, to, head int) updateBatch {
	b := updateBatch{state: store.State{Pts: to}, head: head, more: to < head}
	for p := from + 1; p <= to; p++ {
		b.ups = append(b.ups, &tg.UpdateNewMessage{Message: &tg.Message{ID: p}, Pts: p, PtsCount: 1})
		b.pts = append(b.pts, p)
	}
	return b
}

// ptsOf lists the pts of each update in a pushed envelope.
func ptsOf(t *testing.T, up *tg.Updates) []int {
	t.Helper()
	out := make([]int, 0, len(up.Updates))
	for _, u := range up.Updates {
		nm, ok := u.(*tg.UpdateNewMessage)
		if !ok {
			t.Fatalf("update type = %T, want *tg.UpdateNewMessage", u)
		}
		out = append(out, nm.Pts)
	}
	return out
}

func TestDeliverSuppressionSplitsOriginBatch(t *testing.T) {
	t.Parallel()

	origin := &rpcBatchPushConn{authKeyID: 11}
	other := &rpcBatchPushConn{authKeyID: 22}
	u := testUpdater()
	u.deliverAtSuppressed(
		context.Background(),
		7,
		[]pushConn{origin, other},
		func(int) (updateBatch, error) { return batch(0, 5, 5), nil },
		time.Time{},
		store.SuppressedUpdate{AuthKeyID: 11, Pts: 3},
	)

	if got := len(origin.got); got != 2 {
		t.Fatalf("origin pushes = %d, want prefix and suffix", got)
	}
	if got := ptsOf(t, origin.got[0]); !slices.Equal(got, []int{1, 2}) {
		t.Fatalf("origin prefix pts = %v, want [1 2]", got)
	}
	if got := ptsOf(t, origin.got[1]); !slices.Equal(got, []int{4, 5}) {
		t.Fatalf("origin suffix pts = %v, want [4 5]", got)
	}
	if !slices.Equal(origin.marked, []int{3}) {
		t.Fatalf("origin RPC marks = %v, want [3]", origin.marked)
	}
	if got := origin.pts; got != 5 {
		t.Fatalf("origin watermark = %d, want 5", got)
	}
	if len(other.got) != 1 {
		t.Fatalf("other pushes = %d, want one full batch", len(other.got))
	}
	if got := ptsOf(t, other.got[0]); !slices.Equal(got, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("other pts = %v, want [1 2 3 4 5]", got)
	}
}

func TestDeliverPendingSenderSuppressionLeavesOriginAtBarrier(t *testing.T) {
	t.Parallel()

	origin := &pendingRPCPushConn{fakePushConn: fakePushConn{pts: 1}, pendingPts: 2}
	sibling := &fakePushConn{}
	testUpdater().deliver(context.Background(), 7, []pushConn{origin, sibling}, func(fromPts int) (updateBatch, error) {
		return batch(fromPts, 3, 3), nil
	})

	if len(origin.got) != 0 {
		t.Fatalf("origin pushes = %d, want no push across pending sender event", len(origin.got))
	}
	if origin.pts != 1 {
		t.Fatalf("origin watermark = %d, want 1 at pending barrier", origin.pts)
	}
	if len(sibling.got) != 1 || !slices.Equal(ptsOf(t, sibling.got[0]), []int{1, 2, 3}) {
		t.Fatalf("sibling pushes = %d/%v, want one full batch", len(sibling.got), ptsOf(t, sibling.got[0]))
	}
}

func TestDeliverOverflowSuppressionSurvivesQueuedBarriers(t *testing.T) {
	t.Parallel()

	originTransport := &retryNotifyTransport{done: make(chan struct{})}
	origin := mtproto.NewTestConn(originTransport, replyTestKey())
	origin.SetOwner(7)
	keyID := origin.AuthKeyID()
	for pts := 1; pts <= 64; pts++ {
		reservation, ok := origin.BeginRPCUpdateAttempt(7, keyID, pts)
		if !ok || !origin.SetRPCUpdatePtsAttempt(reservation, pts) {
			t.Fatalf("stage sender barrier %d", pts)
		}
	}
	overflow, ok := origin.BeginRPCUpdateAttempt(7, keyID, 65)
	if ok {
		t.Fatal("overflow sender attempt unexpectedly registered")
	}
	if !origin.SetRPCUpdatePtsAttempt(overflow, 65) {
		t.Fatal("overflow sender attempt did not activate origin suppression")
	}

	sibling := &fakePushConn{}
	u := testUpdater()
	u.deliverAtSuppressed(
		context.Background(),
		7,
		[]pushConn{origin, sibling},
		func(fromPts int) (updateBatch, error) { return batch(fromPts, 65, 65), nil },
		time.Time{},
		store.SuppressedUpdate{AuthKeyID: keyID, Pts: 65},
	)
	for pts := 1; pts <= 64; pts++ {
		if !origin.MarkRPCUpdate(7, keyID, pts) {
			t.Fatalf("account queued sender barrier %d", pts)
		}
	}
	u.deliver(
		context.Background(),
		7,
		[]pushConn{origin, sibling},
		func(fromPts int) (updateBatch, error) { return batch(fromPts, 65, 65), nil },
	)

	if got := originTransport.count(); got != 0 {
		t.Fatalf("origin pushes after queued barriers drained = %d, want 0", got)
	}
	expected := make([]int, 65)
	for i := range expected {
		expected[i] = i + 1
	}
	if len(sibling.got) != 1 || !slices.Equal(ptsOf(t, sibling.got[0]), expected) {
		t.Fatalf("sibling pushes = %d, want one full overflow batch", len(sibling.got))
	}
}

func TestDeliverOverflowSuppressionStartsBeforeQueuedBarriersDrain(t *testing.T) {
	t.Parallel()

	originTransport := &retryNotifyTransport{done: make(chan struct{})}
	origin := mtproto.NewTestConn(originTransport, replyTestKey())
	origin.SetOwner(7)
	keyID := origin.AuthKeyID()
	for pts := 1; pts <= 64; pts++ {
		reservation, ok := origin.BeginRPCUpdateAttempt(7, keyID, pts)
		if !ok || !origin.SetRPCUpdatePtsAttempt(reservation, pts) {
			t.Fatalf("stage sender barrier %d", pts)
		}
	}
	_, ok := origin.BeginRPCUpdateAttempt(7, keyID, 65)
	if ok {
		t.Fatal("overflow sender attempt unexpectedly registered")
	}
	for pts := 1; pts <= 64; pts++ {
		if !origin.MarkRPCUpdate(7, keyID, pts) {
			t.Fatalf("account queued sender barrier %d", pts)
		}
	}
	if pts, pending := origin.PendingRPCUpdate(7); !pending || pts != 0 {
		t.Fatalf("overflow barrier after queue drain = (%d, %t), want (0, true)", pts, pending)
	}

	testUpdater().deliver(
		context.Background(),
		7,
		[]pushConn{origin},
		func(fromPts int) (updateBatch, error) { return batch(fromPts, 65, 65), nil },
	)
	if got := originTransport.count(); got != 0 {
		t.Fatalf("origin pushes before overflow commit marker = %d, want 0", got)
	}
}

func TestDeliverOverflowSuppressionReleasesAfterResultWatermark(t *testing.T) {
	t.Parallel()

	originTransport := &retryNotifyTransport{done: make(chan struct{})}
	origin := mtproto.NewTestConn(originTransport, replyTestKey())
	origin.SetOwner(7)
	keyID := origin.AuthKeyID()
	for pts := 1; pts <= 64; pts++ {
		reservation, ok := origin.BeginRPCUpdateAttempt(7, keyID, pts)
		if !ok || !origin.SetRPCUpdatePtsAttempt(reservation, pts) {
			t.Fatalf("stage sender barrier %d", pts)
		}
	}
	overflow, ok := origin.BeginRPCUpdateAttempt(7, keyID, 65)
	if ok || !origin.SetRPCUpdatePtsAttempt(overflow, 65) {
		t.Fatal("stage overflow sender barrier")
	}
	for pts := 1; pts <= 64; pts++ {
		if !origin.MarkRPCUpdate(7, keyID, pts) {
			t.Fatalf("account queued sender barrier %d", pts)
		}
	}
	if err := origin.SendResultAndMarkRPCUpdate(
		&mtproto.Request{Ctx: context.Background(), MsgID: 8},
		&tg.BoolTrue{}, 7, keyID, 65,
	); err != nil {
		t.Fatalf("send overflow result: %v", err)
	}
	if _, pending := origin.PendingRPCUpdate(7); pending {
		t.Fatal("successful overflow result left origin suppression active")
	}
	before := originTransport.count()
	testUpdater().deliver(
		context.Background(),
		7,
		[]pushConn{origin},
		func(fromPts int) (updateBatch, error) {
			b := batch(fromPts, 66, 66)
			for i, pts := range b.pts {
				b.ups[i] = &tg.UpdateNewMessage{
					Message: &tg.Message{
						ID:      pts,
						PeerID:  &tg.PeerUser{UserID: 7},
						Date:    1,
						Message: "post-overflow",
					},
					Pts:      pts,
					PtsCount: 1,
				}
			}
			return b, nil
		},
	)
	if got := originTransport.count(); got != before+1 {
		t.Fatalf("origin pushes after overflow result = %d, want %d", got, before+1)
	}
	if got := origin.LastPushedPts(); got != 66 {
		t.Fatalf("origin watermark after post-overflow push = %d, want 66", got)
	}
}

func TestDeliverPendingSenderSuppressionAccountsKeyedEventAfterPrefix(t *testing.T) {
	t.Parallel()

	origin := &pendingRPCBatchPushConn{
		fakePushConn: fakePushConn{pts: 0},
		authKeyID:    11,
		pendingPts:   5,
	}
	sibling := &fakePushConn{}
	u := testUpdater()
	u.deliver(context.Background(), 7, []pushConn{origin, sibling}, func(fromPts int) (updateBatch, error) {
		return batch(fromPts, 4, 4), nil
	})

	if len(origin.got) != 1 || !slices.Equal(ptsOf(t, origin.got[0]), []int{1, 2, 3, 4}) {
		t.Fatalf("origin generic pushes = %d/%v, want one prefix [1 2 3 4]", len(origin.got), func() []int {
			if len(origin.got) == 0 {
				return nil
			}
			return ptsOf(t, origin.got[0])
		}())
	}
	if origin.pts != 4 {
		t.Fatalf("origin watermark after generic notification = %d, want 4", origin.pts)
	}

	u.deliverAtSuppressed(
		context.Background(),
		7,
		[]pushConn{origin, sibling},
		func(fromPts int) (updateBatch, error) { return batch(fromPts, 5, 5), nil },
		time.Time{},
		store.SuppressedUpdate{AuthKeyID: 11, Pts: 5},
	)

	if !slices.Equal(origin.marked, []int{5}) {
		t.Fatalf("origin RPC marks = %v, want [5]", origin.marked)
	}
	if origin.pendingPts != 0 {
		t.Fatalf("origin pending pts = %d, want cleared after keyed delivery", origin.pendingPts)
	}
	if origin.pts != 5 {
		t.Fatalf("origin watermark after keyed delivery = %d, want 5", origin.pts)
	}
	if len(origin.got) != 1 {
		t.Fatalf("origin pushes = %d, want prefix only without sender echo", len(origin.got))
	}
}

func TestDeliverPendingSenderSuppressionAccountsMissingKeyedEvent(t *testing.T) {
	t.Parallel()

	origin := &pendingRPCBatchPushConn{
		fakePushConn: fakePushConn{pts: 0},
		authKeyID:    11,
		pendingPts:   1201,
	}
	sibling := &fakePushConn{}
	testUpdater().deliver(context.Background(), 7, []pushConn{origin, sibling}, func(fromPts int) (updateBatch, error) {
		return batch(fromPts, 1206, 1206), nil
	})

	if len(origin.got) != 2 {
		t.Fatalf("origin pushes = %d, want prefix and suffix", len(origin.got))
	}
	if got := ptsOf(t, origin.got[0]); len(got) != 1200 || got[0] != 1 || got[len(got)-1] != 1200 {
		t.Fatalf("origin prefix = %v, want pts 1..1200", got)
	}
	if got := ptsOf(t, origin.got[1]); !slices.Equal(got, []int{1202, 1203, 1204, 1205, 1206}) {
		t.Fatalf("origin suffix = %v, want [1202 1203 1204 1205 1206]", got)
	}
	if !slices.Equal(origin.marked, []int{1201}) {
		t.Fatalf("origin RPC marks = %v, want [1201]", origin.marked)
	}
	if origin.pendingPts != 0 || origin.pts != 1206 {
		t.Fatalf("origin state = pending %d, pts %d; want pending 0, pts 1206", origin.pendingPts, origin.pts)
	}
	if len(sibling.got) != 1 {
		t.Fatalf("sibling pushes = %d, want one full batch", len(sibling.got))
	}
	if got := ptsOf(t, sibling.got[0]); len(got) != 1206 || got[0] != 1 || got[len(got)-1] != 1206 {
		t.Fatalf("sibling batch = pts %d..%d, want 1..1206", got[0], got[len(got)-1])
	}
}

func TestDeliverPendingSenderSuppressionRetriesPrefixWrite(t *testing.T) {
	t.Parallel()

	origin := &flakyPendingRPCBatchPushConn{
		pendingRPCBatchPushConn: pendingRPCBatchPushConn{
			fakePushConn: fakePushConn{pts: 0},
			authKeyID:    11,
			pendingPts:   5,
		},
		failures: 1,
	}
	u := testUpdater()
	u.deliverAtSuppressed(
		context.Background(),
		7,
		[]pushConn{origin},
		func(fromPts int) (updateBatch, error) { return batch(fromPts, 5, 5), nil },
		time.Time{},
		store.SuppressedUpdate{AuthKeyID: 11, Pts: 5},
	)

	if origin.failures != 0 {
		t.Fatalf("prefix write retries = %d, want exhausted", origin.failures)
	}
	if !slices.Equal(origin.marked, []int{5}) {
		t.Fatalf("origin RPC marks = %v, want [5] after retry", origin.marked)
	}
	if origin.pendingPts != 0 {
		t.Fatalf("origin pending pts = %d, want cleared after retry", origin.pendingPts)
	}
	if origin.pts != 5 {
		t.Fatalf("origin watermark = %d, want 5 after retry", origin.pts)
	}
}

func TestPendingSenderResultGapSerializesGenericAndKeyedDelivery(t *testing.T) {
	t.Parallel()

	key := replyTestKey()
	transport := &blockedResultTransport{entered: make(chan struct{}), release: make(chan struct{})}
	origin := mtproto.NewTestConn(transport, key)
	origin.SetOwner(7)
	keyID := mtproto.AuthKeyIDInt64(key.ID)
	if !origin.BeginRPCUpdate(7, keyID, 0) || !origin.SetRPCUpdatePts(7, keyID, 5) {
		t.Fatal("failed to stage sender result barrier")
	}
	sibling := &fakePushConn{}
	buildWireBatch := func(fromPts int) updateBatch {
		b := batch(fromPts, 5, 5)
		for _, up := range b.ups {
			nm, ok := up.(*tg.UpdateNewMessage)
			if !ok {
				t.Fatalf("update type = %T, want *tg.UpdateNewMessage", up)
			}
			msg, ok := nm.Message.(*tg.Message)
			if !ok {
				t.Fatalf("message type = %T, want *tg.Message", nm.Message)
			}
			msg.PeerID = &tg.PeerUser{UserID: 7}
			msg.Date = 1
			msg.Message = "sender gap"
		}
		return b
	}
	resultDone := make(chan error, 1)
	go func() {
		resultDone <- origin.SendResultAndMarkRPCUpdate(
			&mtproto.Request{Ctx: context.Background(), MsgID: 4},
			&tg.BoolTrue{}, 7, keyID, 5,
		)
	}()
	select {
	case <-transport.entered:
	case <-time.After(time.Second):
		t.Fatal("sender result did not reach the transport")
	}

	deliveryDone := make(chan struct{})
	go func() {
		testUpdater().deliver(context.Background(), 7, []pushConn{origin, sibling}, func(fromPts int) (updateBatch, error) {
			return buildWireBatch(fromPts), nil
		})
		close(deliveryDone)
	}()
	close(transport.release)
	if err := <-resultDone; err != nil {
		t.Fatalf("send result: %v", err)
	}
	select {
	case <-deliveryDone:
	case <-time.After(time.Second):
		t.Fatal("generic delivery did not finish")
	}
	if got := origin.LastPushedPts(); got != 5 {
		t.Fatalf("origin watermark after generic delivery = %d, want accounted sender pts 5", got)
	}
	if _, pending := origin.PendingRPCUpdate(7); pending {
		t.Fatal("origin barrier after generic delivery remained active")
	}

	testUpdater().deliverAtSuppressed(
		context.Background(),
		7,
		[]pushConn{origin, sibling},
		func(fromPts int) (updateBatch, error) { return buildWireBatch(fromPts), nil },
		time.Time{},
		store.SuppressedUpdate{AuthKeyID: keyID, Pts: 5},
	)
	if _, pending := origin.PendingRPCUpdate(7); pending {
		t.Fatal("keyed delivery left the sender barrier active")
	}
	if got := origin.LastPushedPts(); got != 5 {
		t.Fatalf("origin watermark after keyed delivery = %d, want 5", got)
	}
	if got := transport.sendCount(); got != 2 {
		t.Fatalf("origin transport sends = %d, want result plus prefix only", got)
	}
}

func TestDeliverBackToBackSenderSuppressionKeepsFirstBarrier(t *testing.T) {
	t.Parallel()

	origin := &queuedRPCBatchPushConn{
		authKeyID:   11,
		pending:     []int{5, 6},
		markEntered: make(chan struct{}),
		releaseMark: make(chan struct{}),
	}
	sibling := &fakePushConn{}
	u := testUpdater()
	u.deliver(context.Background(), 7, []pushConn{origin, sibling}, func(fromPts int) (updateBatch, error) {
		return batch(fromPts, 6, 6), nil
	})
	if got := origin.LastPushedPts(); got != 0 {
		t.Fatalf("origin watermark before keyed delivery = %d, want 0", got)
	}
	if len(origin.got) != 0 {
		t.Fatalf("origin pushes before keyed delivery = %d, want none", len(origin.got))
	}

	firstDone := make(chan struct{})
	go func() {
		u.deliverAtSuppressed(
			context.Background(),
			7,
			[]pushConn{origin, sibling},
			func(fromPts int) (updateBatch, error) { return batch(fromPts, 6, 6), nil },
			time.Time{},
			store.SuppressedUpdate{AuthKeyID: 11, Pts: 5},
		)
		close(firstDone)
	}()
	select {
	case <-origin.markEntered:
	case <-time.After(time.Second):
		t.Fatal("first keyed delivery did not reach its accounting barrier")
	}

	u.deliver(context.Background(), 7, []pushConn{origin, sibling}, func(fromPts int) (updateBatch, error) {
		return batch(fromPts, 6, 6), nil
	})
	if got := origin.LastPushedPts(); got != 4 {
		t.Fatalf("origin watermark while first keyed delivery paused = %d, want 4", got)
	}
	if len(origin.got) != 1 {
		t.Fatalf("origin pushes while first keyed delivery paused = %d, want 1", len(origin.got))
	}

	close(origin.releaseMark)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first keyed delivery did not finish")
	}
	if got := origin.LastPushedPts(); got != 5 {
		t.Fatalf("origin watermark after first keyed delivery = %d, want 5", got)
	}

	u.deliverAtSuppressed(
		context.Background(),
		7,
		[]pushConn{origin, sibling},
		func(fromPts int) (updateBatch, error) { return batch(fromPts, 6, 6), nil },
		time.Time{},
		store.SuppressedUpdate{AuthKeyID: 11, Pts: 6},
	)
	if got := origin.LastPushedPts(); got != 6 {
		t.Fatalf("origin watermark after second keyed delivery = %d, want 6", got)
	}
	if len(origin.pending) != 0 {
		t.Fatalf("pending sender barriers = %v, want none", origin.pending)
	}
	if !slices.Equal(origin.marked, []int{5, 6}) {
		t.Fatalf("origin keyed marks = %v, want [5 6]", origin.marked)
	}
	if len(origin.got) != 1 {
		t.Fatalf("origin pushes after both keyed deliveries = %d, want prefix only", len(origin.got))
	}
	if got := ptsOf(t, origin.got[0]); !slices.Equal(got, []int{1, 2, 3, 4}) {
		t.Fatalf("origin prefix after both keyed deliveries = %v, want [1 2 3 4]", got)
	}
}

func TestRegisterReplyAfterSuccessRunsHookAfterWire(t *testing.T) {
	t.Parallel()

	events := []string{}
	d := mtproto.NewDispatcher()
	key := replyTestKey()
	var conn *mtproto.Conn
	registerReplyAfterSuccess(d, tg.HelpGetConfigRequestTypeID, func(_ *mtproto.Conn, _ *mtproto.Request) (bin.Encoder, *replyUpdate, func(), error) {
		events = append(events, "handler")
		return &tg.BoolTrue{}, &replyUpdate{owner: 1, authKey: mtproto.AuthKeyIDInt64(key.ID), pts: 7}, func() {
			events = append(events, "after")
			if got := conn.LastPushedPts(); got != 7 {
				t.Errorf("watermark in post-reply hook = %d, want 7", got)
			}
		}, nil
	})

	var body bin.Buffer
	if err := (&tg.HelpGetConfigRequest{}).Encode(&body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	conn = mtproto.NewTestConn(&replyOrderTransport{events: &events}, key)
	conn.SetOwner(1)
	if !conn.MarkRPCUpdate(1, mtproto.AuthKeyIDInt64(key.ID), 6) {
		t.Fatal("seed sender watermark")
	}
	err := d.OnMessage(conn, &mtproto.Request{
		Ctx:    context.Background(),
		UserID: 1,
		MsgID:  1,
		Buf:    &body,
	})
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	want := []string{"handler", "send", "after"}
	if len(events) != len(want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("events = %v, want %v", events, want)
		}
	}
}

func TestRegisterReplyAfterSuccessRunsFailureHook(t *testing.T) {
	t.Parallel()

	d := mtproto.NewDispatcher()
	key := replyTestKey()
	failed := false
	registerReplyAfterSuccess(d, tg.HelpGetConfigRequestTypeID, func(_ *mtproto.Conn, _ *mtproto.Request) (bin.Encoder, *replyUpdate, func(), error) {
		return &tg.BoolTrue{}, &replyUpdate{
			onFailure: func() { failed = true },
		}, nil, nil
	})

	var body bin.Buffer
	if err := (&tg.HelpGetConfigRequest{}).Encode(&body); err != nil {
		t.Fatalf("encode request: %v", err)
	}
	conn := mtproto.NewTestConn(replyFailureTransport{}, key)
	conn.SetOwner(1)
	err := d.OnMessage(conn, &mtproto.Request{
		Ctx:    context.Background(),
		UserID: 1,
		MsgID:  1,
		Buf:    &body,
	})
	if err == nil {
		t.Fatal("dispatch succeeded despite result write failure")
	}
	if !failed {
		t.Fatal("committed-send failure hook did not run")
	}
}

func TestSenderNotifyContextOutlivesRPCContext(t *testing.T) {
	t.Parallel()

	parent, cancelParent := context.WithCancel(context.Background())
	ctx, cancel := senderNotifyContext(parent)
	defer cancel()
	cancelParent()

	if err := ctx.Err(); err != nil {
		t.Fatalf("sender notify context canceled with parent: %v", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("sender notify context has no deadline")
	}
	remaining := time.Until(deadline)
	if remaining <= 0 || remaining > senderNotifyTimeout {
		t.Fatalf("sender notify deadline in %s, want (0, %s]", remaining, senderNotifyTimeout)
	}
}

func testUpdater() *Updater {
	log := slog.New(slog.DiscardHandler)
	return &Updater{h: &handlers{log: log}, log: log}
}

// TestDeliverBuildsOnceForEveryConn is the amplification fix: one notification
// costs one build no matter how many sockets the user holds, and each socket
// still gets exactly the updates past its own watermark.
func TestDeliverBuildsOnceForEveryConn(t *testing.T) {
	t.Parallel()
	behind, mid, caught := &fakePushConn{}, &fakePushConn{pts: 3}, &fakePushConn{pts: 5}
	conns := []pushConn{behind, mid, caught}

	builds := 0
	testUpdater().deliver(context.Background(), 7, conns, func(fromPts int) (updateBatch, error) {
		builds++
		if fromPts != 0 {
			t.Errorf("built from pts %d, want the minimum watermark 0", fromPts)
		}
		return batch(0, 5, 5), nil
	})

	if builds != 1 {
		t.Fatalf("builds = %d for 3 conns, want 1", builds)
	}
	if len(behind.got) != 1 {
		t.Fatalf("conn at pts 0 got %d pushes, want 1", len(behind.got))
	}
	if got := ptsOf(t, behind.got[0]); !slices.Equal(got, []int{1, 2, 3, 4, 5}) {
		t.Fatalf("conn at pts 0 got pts %v, want 1..5", got)
	}
	if len(mid.got) != 1 {
		t.Fatalf("conn at pts 3 got %d pushes, want 1", len(mid.got))
	}
	if got := ptsOf(t, mid.got[0]); !slices.Equal(got, []int{4, 5}) {
		t.Fatalf("conn at pts 3 got pts %v, want the gap 4..5", got)
	}
	if len(caught.got) != 0 {
		t.Fatalf("conn already at the head got %d pushes, want none", len(caught.got))
	}
	if behind.pts != 5 || mid.pts != 5 {
		t.Fatalf("watermarks = %d/%d, want both 5", behind.pts, mid.pts)
	}
}

// TestDeliverAddingConnsKeepsOneBuild pins the round trips to the distinct
// watermarks, not the connection count.
func TestDeliverAddingConnsKeepsOneBuild(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, 2, 16} {
		conns := make([]pushConn, n)
		for i := range conns {
			conns[i] = &fakePushConn{pts: i % 3}
		}
		builds := 0
		testUpdater().deliver(context.Background(), 7, conns, func(int) (updateBatch, error) {
			builds++
			return batch(0, 9, 9), nil
		})
		if builds != 1 {
			t.Fatalf("%d conns cost %d builds, want 1", n, builds)
		}
	}
}

// TestDeliverStaggeredWatermarksBoundsBuilds is the same guarantee under
// truncation: watermarks spread a whole batch apart would otherwise open one
// window per band, so the round count is capped and the conns nearest the head
// — the ones a live push exists for — are the ones the second round serves.
func TestDeliverStaggeredWatermarksBoundsBuilds(t *testing.T) {
	t.Parallel()
	const head = 20000
	for _, n := range []int{2, 4, 16, 32} {
		conns := make([]pushConn, n)
		for i := range conns {
			// A batch apart plus one, so no window can cover two of them.
			conns[i] = &fakePushConn{pts: i * (maxDiffEvents + 1)}
		}
		builds := 0
		testUpdater().deliver(context.Background(), 7, conns, func(fromPts int) (updateBatch, error) {
			builds++
			to := min(fromPts+maxDiffEvents, head)
			return batch(fromPts, to, head), nil
		})
		if builds > maxDeliveryRounds {
			t.Fatalf("%d conns cost %d builds, want at most %d", n, builds, maxDeliveryRounds)
		}
		top, ok := conns[n-1].(*fakePushConn)
		if !ok {
			t.Fatal("conn type")
		}
		if len(top.got) != 1 {
			t.Fatalf("%d conns: the conn nearest the head got %d pushes, want 1", n, len(top.got))
		}
	}
}

// TestDeliverTruncatedBatchServesConnsAhead covers the interaction with the
// maxDiffEvents cap: a batch clamped below an already-advanced conn's watermark
// does not strand it, and the extra round is per distinct window, not per conn.
func TestDeliverTruncatedBatchServesConnsAhead(t *testing.T) {
	t.Parallel()
	behind, alsoBehind, ahead := &fakePushConn{}, &fakePushConn{}, &fakePushConn{pts: 600}
	conns := []pushConn{behind, alsoBehind, ahead}

	var from []int
	testUpdater().deliver(context.Background(), 7, conns, func(fromPts int) (updateBatch, error) {
		from = append(from, fromPts)
		if fromPts == 0 {
			// Truncated at the cap: advertises only through the last event.
			return batch(0, 500, 605), nil
		}
		return batch(fromPts, 605, 605), nil
	})

	if !slices.Equal(from, []int{0, 600}) {
		t.Fatalf("built from %v, want one window per distinct watermark [0 600]", from)
	}
	for _, c := range []*fakePushConn{behind, alsoBehind} {
		if len(c.got) != 1 || c.pts != 500 {
			t.Fatalf("conn at pts 0: %d pushes, watermark %d, want 1 push and 500", len(c.got), c.pts)
		}
	}
	if len(ahead.got) != 1 {
		t.Fatalf("conn at pts 600 got %d pushes, want 1", len(ahead.got))
	}
	if got := ptsOf(t, ahead.got[0]); !slices.Equal(got, []int{601, 602, 603, 604, 605}) {
		t.Fatalf("conn at pts 600 got pts %v, want 601..605", got)
	}
}

// TestDeliverSecondWindowCoversEveryConnNearTheHead pins the second window to
// the head rather than to one connection: every conn within a batch of the head
// shares it and reaches the head, so a middle conn gets the same updates the
// per-connection path used to give it.
func TestDeliverSecondWindowCoversEveryConnNearTheHead(t *testing.T) {
	t.Parallel()
	const head = 1001
	behind, mid, live := &fakePushConn{}, &fakePushConn{pts: 600}, &fakePushConn{pts: 1000}

	var from []int
	testUpdater().deliver(context.Background(), 7, []pushConn{behind, mid, live}, func(fromPts int) (updateBatch, error) {
		from = append(from, fromPts)
		return batch(fromPts, min(fromPts+maxDiffEvents, head), head), nil
	})

	if !slices.Equal(from, []int{0, 600}) {
		t.Fatalf("built from %v, want [0 600]: the tail window must start low enough to take the pts-600 conn", from)
	}
	if got := ptsOf(t, behind.got[0]); !slices.Equal(got[:1], []int{1}) || behind.pts != 500 {
		t.Fatalf("conn at pts 0 got %d updates from pts %v, watermark %d, want 1..500", len(got), got[:1], behind.pts)
	}
	if len(mid.got) != 1 {
		t.Fatalf("conn at pts 600 got %d pushes, want 1", len(mid.got))
	}
	got := ptsOf(t, mid.got[0])
	if got[0] != 601 || got[len(got)-1] != head || len(got) != head-600 {
		t.Fatalf("conn at pts 600 got %d updates %d..%d, want 601..%d", len(got), got[0], got[len(got)-1], head)
	}
	if len(live.got) != 1 || !slices.Equal(ptsOf(t, live.got[0]), []int{head}) {
		t.Fatalf("conn at pts 1000 got %d pushes, want the single update %d", len(live.got), head)
	}
}

// TestDeliverNothingPending pins the caught-up case: the most-behind conn has
// nothing, so no conn does, and no second window is opened.
func TestDeliverNothingPending(t *testing.T) {
	t.Parallel()
	c := &fakePushConn{pts: 9}
	builds := 0
	testUpdater().deliver(context.Background(), 7, []pushConn{c}, func(int) (updateBatch, error) {
		builds++
		return batch(9, 9, 9), nil
	})
	if builds != 1 || len(c.got) != 0 {
		t.Fatalf("builds = %d, pushes = %d, want 1 and 0", builds, len(c.got))
	}
}

// TestDeliverBuildErrorStops keeps a failed build best-effort: nothing is
// pushed and the loop ends rather than retrying the same window.
func TestDeliverBuildErrorStops(t *testing.T) {
	t.Parallel()
	c := &fakePushConn{}
	builds := 0
	testUpdater().deliver(context.Background(), 7, []pushConn{c}, func(int) (updateBatch, error) {
		builds++
		return updateBatch{}, errors.New("boom")
	})
	if builds != 1 || len(c.got) != 0 {
		t.Fatalf("builds = %d, pushes = %d, want 1 and 0", builds, len(c.got))
	}
}

// TestDeliverPushFailureDoesNotBlockOthers keeps one broken socket from
// stopping the fan-out; the client's next getDifference backfills it.
func TestDeliverPushFailureDoesNotBlockOthers(t *testing.T) {
	t.Parallel()
	broken, ok := &fakePushConn{fail: errors.New("write")}, &fakePushConn{}
	testUpdater().deliver(context.Background(), 7, []pushConn{broken, ok}, func(int) (updateBatch, error) {
		return batch(0, 2, 2), nil
	})
	if len(ok.got) != 1 {
		t.Fatalf("healthy conn got %d pushes, want 1", len(ok.got))
	}
	if broken.pts != 0 {
		t.Fatalf("failed push advanced the watermark to %d", broken.pts)
	}
}

type stalePushConn struct {
	attempts int
}

func (s *stalePushConn) LastPushedPts() int { return 0 }

func (s *stalePushConn) PushTo(context.Context, int64, bin.Encoder, int) (bool, error) {
	s.attempts++
	return false, nil
}

func (s *stalePushConn) PushToAtWatermark(context.Context, int64, int, bin.Encoder, int) (bool, bool, error) {
	s.attempts++
	return false, true, nil
}

func TestDeliverLogsExhaustedRetries(t *testing.T) {
	t.Parallel()

	var logs bytes.Buffer
	u := &Updater{
		log: slog.New(slog.NewTextHandler(&logs, nil)),
	}
	conn := &stalePushConn{}
	u.deliver(context.Background(), 7, []pushConn{conn}, func(int) (updateBatch, error) {
		return batch(0, 1, 1), nil
	})

	if want := maxDeliveryRetries + 1; conn.attempts != want {
		t.Fatalf("delivery attempts = %d, want %d", conn.attempts, want)
	}
	if !strings.Contains(logs.String(), "deliver retries exhausted") {
		t.Fatalf("logs = %q, want exhausted-retry record", logs.String())
	}
	if !strings.Contains(logs.String(), "user_id=7") || !strings.Contains(logs.String(), "retries=5") {
		t.Fatalf("logs = %q, want user_id and retry count", logs.String())
	}
}

func TestDeliverRecordsPushOutcomesPerConnection(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	now := start
	metrics := store.NewNotificationMetricsWithClock(func() time.Time { return now })
	acceptedAt := start

	successA := &outcomePushConn{
		pushed: true,
		onPush: func() { now = start.Add(24 * time.Millisecond) },
	}
	successB := &outcomePushConn{
		pushed: true,
		onPush: func() { now = start.Add(48 * time.Millisecond) },
	}
	ownerMismatch := &outcomePushConn{}
	encodeFailure := &outcomePushConn{err: mtproto.MarkPushEncodeError(errors.New("encode"))}
	writeFailure := &outcomePushConn{err: errors.New("write")}

	u := &Updater{log: slog.New(slog.DiscardHandler), pushMetrics: metrics}
	u.deliverAt(context.Background(), 7, []pushConn{
		successA,
		successB,
		ownerMismatch,
		encodeFailure,
		writeFailure,
	}, func(int) (updateBatch, error) {
		return batch(0, 1, 1), nil
	}, acceptedAt)

	got := metrics.Snapshot()
	if got.Push.SampleCount != 2 {
		t.Errorf("push samples = %d, want one per successful connection", got.Push.SampleCount)
	}
	if got.Push.P50Milliseconds == 0 || got.Push.P95Milliseconds == 0 {
		t.Errorf("push percentiles = p50=%v p95=%v, want non-zero", got.Push.P50Milliseconds, got.Push.P95Milliseconds)
	}
	if got.Push.Outcomes != (store.PushOutcomeCounts{
		Success:       2,
		OwnerMismatch: 1,
		EncodeFailure: 1,
		WriteFailure:  1,
	}) {
		t.Errorf("push outcomes = %+v, want one fixed outcome per attempted connection", got.Push.Outcomes)
	}
	if successA.attempts != 1 || successB.attempts != 1 {
		t.Fatalf("successful connection attempts = %d/%d, want one each", successA.attempts, successB.attempts)
	}
}

func TestDeliverRecorderPanicDoesNotStopFanout(t *testing.T) {
	t.Parallel()

	start := time.Unix(1_700_000_000, 0)
	var clockCalls atomic.Int32
	metrics := store.NewNotificationMetricsWithClock(func() time.Time {
		if clockCalls.Add(1) == 2 {
			panic("telemetry failure")
		}
		return start.Add(24 * time.Millisecond)
	})
	first := &outcomePushConn{pushed: true}
	later := &outcomePushConn{pushed: true}

	u := &Updater{log: slog.New(slog.DiscardHandler), pushMetrics: metrics}
	u.deliverAt(context.Background(), 7, []pushConn{first, later}, func(int) (updateBatch, error) {
		return batch(0, 1, 1), nil
	}, start)

	if first.attempts != 1 || later.attempts != 1 {
		t.Fatalf("push attempts after recorder panic = %d/%d, want one per connection", first.attempts, later.attempts)
	}
	if got := metrics.Snapshot().Push.SampleCount; got != 1 {
		t.Fatalf("recorded samples after recorder panic = %d, want the later successful attempt", got)
	}
}

func TestDeliverWithoutPushObserverPreservesDelivery(t *testing.T) {
	t.Parallel()

	conn := &outcomePushConn{pushed: true}
	u := &Updater{log: slog.New(slog.DiscardHandler)}
	u.deliverAt(context.Background(), 7, []pushConn{conn}, func(int) (updateBatch, error) {
		return batch(0, 1, 1), nil
	}, time.Unix(1_700_000_000, 0))

	if conn.attempts != 1 || conn.pts != 1 {
		t.Fatalf("delivery without observer = attempts %d pts %d, want one successful write at pts 1", conn.attempts, conn.pts)
	}
}

// chanBatch builds a channelBatch of one UpdateNewChannelMessage per pts in
// (fromPts, toPts], mirroring the batch helper for the per-account stream.
func chanBatch(fromPts, toPts int) channelBatch {
	b := channelBatch{currentPts: toPts}
	for p := fromPts + 1; p <= toPts; p++ {
		b.ups = append(b.ups, &tg.UpdateNewChannelMessage{
			Message:  &tg.Message{ID: p},
			Pts:      p,
			PtsCount: 1,
		})
		b.pts = append(b.pts, p)
	}
	return b
}

// chanPtsOf extracts the pts sequence from a pushed tg.Updates carrying channel updates.
func chanPtsOf(t *testing.T, up *tg.Updates) []int {
	t.Helper()
	out := make([]int, 0, len(up.Updates))
	for _, u := range up.Updates {
		nm, ok := u.(*tg.UpdateNewChannelMessage)
		if !ok {
			t.Fatalf("update type = %T, want *tg.UpdateNewChannelMessage", u)
		}
		out = append(out, nm.Pts)
	}
	return out
}

// TestDeliverChannelPostPushes verifies a post pushes UpdateNewChannelMessage
// to a member's live conn and does not advance the per-account watermark.
func TestDeliverChannelPostPushes(t *testing.T) {
	t.Parallel()
	conn := &fakePushConn{}
	member := store.ChannelMember{UserID: 1, JoinPts: 0}
	builds := 0
	testUpdater().deliverChannel(
		context.Background(),
		[]store.ChannelMember{member},
		time.Now(),
		func(userID int64) []pushConn {
			if userID == 1 {
				return []pushConn{conn}
			}
			return nil
		},
		func(_ int64, fromPts int) (channelBatch, error) {
			builds++
			return chanBatch(fromPts, 3), nil
		},
	)
	if builds != 1 {
		t.Fatalf("builds = %d, want 1", builds)
	}
	if len(conn.got) != 1 {
		t.Fatalf("member got %d pushes, want 1", len(conn.got))
	}
	if got := chanPtsOf(t, conn.got[0]); !slices.Equal(got, []int{1, 2, 3}) {
		t.Fatalf("got pts %v, want 1..3", got)
	}
	// Channel push must not advance the per-account watermark.
	if conn.pts != 0 {
		t.Fatalf("account watermark = %d after channel push, want 0", conn.pts)
	}
}

// TestDeliverChannelPostNoConnNoWork verifies a member with no live conn costs
// no build call.
func TestDeliverChannelPostNoConnNoWork(t *testing.T) {
	t.Parallel()
	member := store.ChannelMember{UserID: 1, JoinPts: 0}
	builds := 0
	testUpdater().deliverChannel(
		context.Background(),
		[]store.ChannelMember{member},
		time.Now(),
		func(int64) []pushConn { return nil },
		func(_ int64, _ int) (channelBatch, error) {
			builds++
			return chanBatch(0, 5), nil
		},
	)
	if builds != 0 {
		t.Fatalf("builds = %d for no-conn member, want 0", builds)
	}
}

// TestDeliverChannelPostBannedReceivesNothing verifies a banned member gets
// no push and no build call.
func TestDeliverChannelPostBannedReceivesNothing(t *testing.T) {
	t.Parallel()
	conn := &fakePushConn{}
	until := time.Now().Add(time.Hour)
	member := store.ChannelMember{UserID: 1, JoinPts: 0, BannedUntil: &until}
	builds := 0
	testUpdater().deliverChannel(
		context.Background(),
		[]store.ChannelMember{member},
		time.Now(),
		func(int64) []pushConn { return []pushConn{conn} },
		func(_ int64, _ int) (channelBatch, error) {
			builds++
			return chanBatch(0, 3), nil
		},
	)
	if builds != 0 {
		t.Fatalf("builds = %d for banned member, want 0", builds)
	}
	if len(conn.got) != 0 {
		t.Fatalf("banned member got %d pushes, want 0", len(conn.got))
	}
}

// TestDeliverChannelPostTwoMembers verifies two members each receive exactly
// one push from one notification.
func TestDeliverChannelPostTwoMembers(t *testing.T) {
	t.Parallel()
	connA, connB := &fakePushConn{}, &fakePushConn{}
	members := []store.ChannelMember{
		{UserID: 1, JoinPts: 0},
		{UserID: 2, JoinPts: 0},
	}
	builds := 0
	testUpdater().deliverChannel(
		context.Background(),
		members,
		time.Now(),
		func(userID int64) []pushConn {
			switch userID {
			case 1:
				return []pushConn{connA}
			case 2:
				return []pushConn{connB}
			}
			return nil
		},
		func(_ int64, fromPts int) (channelBatch, error) {
			builds++
			return chanBatch(fromPts, 5), nil
		},
	)
	if builds != 2 {
		t.Fatalf("builds = %d, want one per active member (2)", builds)
	}
	if len(connA.got) != 1 {
		t.Fatalf("member 1 got %d pushes, want 1", len(connA.got))
	}
	if len(connB.got) != 1 {
		t.Fatalf("member 2 got %d pushes, want 1", len(connB.got))
	}
}

// TestDeliverChannelPostPushesViaRealStore exercises the full fan-out path
// against a real store: PostChannelMessage writes a channel event, deliverChannel
// is driven with a buildChannelUpdates call that reads it back, and the push
// arrives on a fakePushConn. This covers DeliverChannelPost's build closure
// (including the max(fromPts, currentPts-1) floor) without needing a real network
// connection.
func TestDeliverChannelPostPushesViaRealStore(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	b, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(b))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Errorf("store close: %v", err)
		}
	})

	alice, err := s.CreateUser(ctx, "+15550000201")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	ch, err := s.CreateChannel(ctx, alice.ID, "live channel", "", false)
	if err != nil {
		t.Fatalf("create channel: %v", err)
	}
	if _, _, _, err = s.PostChannelMessage(ctx, ch.ID, alice.ID, "hello", 1, nil, 0); err != nil {
		t.Fatalf("post: %v", err)
	}
	currentPts, err := s.ChannelState(ctx, ch.ID)
	if err != nil {
		t.Fatalf("channel state: %v", err)
	}

	conn := &fakePushConn{}
	member := store.ChannelMember{UserID: alice.ID, JoinPts: 0}
	u := &Updater{h: &handlers{store: s, log: slog.New(slog.DiscardHandler), peers: pgtest.PeerDeriver()}, log: slog.New(slog.DiscardHandler)}
	u.deliverChannel(ctx, []store.ChannelMember{member}, time.Now(),
		func(userID int64) []pushConn { return []pushConn{conn} },
		func(memberID int64, fromPts int) (channelBatch, error) {
			return u.h.buildChannelUpdates(ctx, ch.ID, memberID, max(fromPts, currentPts-1), maxDiffEvents, currentPts)
		},
	)
	if len(conn.got) != 1 {
		t.Fatalf("alice got %d pushes, want 1", len(conn.got))
	}
	var gotChannel bool
	for _, up := range conn.got[0].Updates {
		if _, ok := up.(*tg.UpdateNewChannelMessage); ok {
			gotChannel = true
		}
	}
	if !gotChannel {
		t.Fatal("push contained no UpdateNewChannelMessage")
	}
}

// TestBatchAbove pins the slicing rule itself: the suffix strictly above the
// watermark, no duplicates, empty once caught up.
func TestBatchAbove(t *testing.T) {
	t.Parallel()
	b := batch(0, 4, 4)
	for _, tc := range []struct {
		from int
		want []int
	}{
		{from: 0, want: []int{1, 2, 3, 4}},
		{from: 1, want: []int{2, 3, 4}},
		{from: 3, want: []int{4}},
		{from: 4, want: nil},
		{from: 99, want: nil},
	} {
		got := make([]int, 0, len(tc.want))
		for _, u := range b.above(tc.from) {
			nm, isNew := u.(*tg.UpdateNewMessage)
			if !isNew {
				t.Fatalf("update type = %T", u)
			}
			got = append(got, nm.Pts)
		}
		if !slices.Equal(got, tc.want) {
			t.Fatalf("above(%d) = %v, want %v", tc.from, got, tc.want)
		}
	}
}
