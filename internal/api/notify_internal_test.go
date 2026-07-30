package api

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

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
