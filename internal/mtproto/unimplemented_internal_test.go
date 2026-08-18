package mtproto

import (
	"testing"
	"time"
)

// TestUnimplementedBudgetThresholds drives one connection's budget across all
// three bands in a single window. The bands are the whole of what the bound
// promises a client: a real client's cold-start burst is answered as it always
// was, a loop past that is told to back off, and one past the ceiling is cut.
func TestUnimplementedBudgetThresholds(t *testing.T) {
	t.Parallel()
	now := time.Now()
	b := &unimplementedBudget{}

	for i := 1; i <= 300; i++ {
		want := UnimplementedAnswer
		switch {
		case i >= unimplementedCloseAt:
			want = UnimplementedClose
		case i > unimplementedAnswerBudget:
			want = UnimplementedFloodWait
		}
		if got := b.charge(now); got != want {
			t.Fatalf("call %d: verdict = %v, want %v", i, got, want)
		}
	}
}

// TestUnimplementedBudgetWindowRollsOver covers the window itself: a
// connection that spent its budget an hour ago is a connection that has spent
// nothing, or the first bound would be a lifetime quota rather than a rate.
func TestUnimplementedBudgetWindowRollsOver(t *testing.T) {
	t.Parallel()
	start := time.Now()
	b := &unimplementedBudget{}

	for range unimplementedAnswerBudget {
		if got := b.charge(start); got != UnimplementedAnswer {
			t.Fatalf("inside budget: verdict = %v, want %v", got, UnimplementedAnswer)
		}
	}
	justInside := start.Add(unimplementedWindow - time.Nanosecond)
	if got := b.charge(justInside); got != UnimplementedFloodWait {
		t.Fatalf("last nanosecond of the window: verdict = %v, want %v", got, UnimplementedFloodWait)
	}
	rolled := start.Add(unimplementedWindow)
	if got := b.charge(rolled); got != UnimplementedAnswer {
		t.Fatalf("first call of the next window: verdict = %v, want %v", got, UnimplementedAnswer)
	}
	// The new window is a whole one, not the remainder of the old.
	for range unimplementedAnswerBudget - 1 {
		if got := b.charge(rolled); got != UnimplementedAnswer {
			t.Fatalf("inside the new window: verdict = %v, want %v", got, UnimplementedAnswer)
		}
	}
	if got := b.charge(rolled); got != UnimplementedFloodWait {
		t.Fatalf("past the new window's budget: verdict = %v, want %v", got, UnimplementedFloodWait)
	}
}

// TestUnimplementedBudgetSampledLogCountsSuppressed covers the line the burst
// produces. The count is the point of sampling it: without it the operator
// cannot tell one stray call from eighty thousand, which is the difference the
// line exists to report.
func TestUnimplementedBudgetSampledLogCountsSuppressed(t *testing.T) {
	t.Parallel()
	start := time.Now()
	b := &unimplementedBudget{}

	if suppressed, ok := b.logAllow(start); !ok || suppressed != 0 {
		t.Fatalf("first line: (%d, %t), want (0, true)", suppressed, ok)
	}
	const burst = 300
	for i := range burst {
		if _, ok := b.logAllow(start.Add(time.Duration(i))); ok {
			t.Fatalf("line %d inside the interval was emitted", i)
		}
	}
	suppressed, ok := b.logAllow(start.Add(unimplementedLogInterval))
	if !ok {
		t.Fatal("no line emitted once the interval elapsed")
	}
	if suppressed != burst {
		t.Errorf("suppressed = %d, want %d", suppressed, burst)
	}
}

// TestUnimplementedBudgetStartsAtZero pins the per-connection reset: the
// counters are the conn's own, so a fresh one owes nothing whatever the
// connection before it spent.
func TestUnimplementedBudgetStartsAtZero(t *testing.T) {
	t.Parallel()
	now := time.Now()
	spent := &unimplementedBudget{}
	for range unimplementedCloseAt {
		spent.charge(now)
	}
	if got := spent.charge(now); got != UnimplementedClose {
		t.Fatalf("spent budget: verdict = %v, want %v", got, UnimplementedClose)
	}
	if got := (&unimplementedBudget{}).charge(now); got != UnimplementedAnswer {
		t.Errorf("fresh budget: verdict = %v, want %v", got, UnimplementedAnswer)
	}
}
