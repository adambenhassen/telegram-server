package api_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// privateChannelCounts spans the planner flip point the M15 title index
// introduced: below it the title arm is a cheap bitmap scan, above it the same
// arm walks every matching row. A leak shows as a step between two of these.
var privateChannelCounts = []int{10, 1000, 100000}

// costFlatFactor is how much the slowest corpus may cost over the cheapest
// before the two are no longer "the same latency". The defect this guards
// against is a step of roughly 20x, and the measurement is a minimum over many
// runs — the statistic least disturbed by load on a shared machine — so a
// factor of 5 separates the two without reading scheduler noise as a leak.
const costFlatFactor = 5

// costSearchRuns is how many times each corpus is searched. Only the minimum is
// kept: it approximates the query's own cost with the machine's noise removed,
// and a hundred runs of a sub-millisecond query cost nothing.
const costSearchRuns = 100

// costBackgroundPublic is how many public channels sit behind the corpus, none
// of them matching the token. They are not decoration: with no public channel
// on the server at all, the public arm's join against usernames has nothing on
// either side and Postgres skips the candidate scan outright, which hides the
// very cost this test measures. A server with public channels is also the only
// server this RPC exists for.
const costBackgroundPublic = 200

// costBackgroundMemberships is how many channels the caller belongs to, none of
// them matching the token — "a member of nothing matching", not "a member of
// nothing". It defeats the same short circuit on the member arm.
const costBackgroundMemberships = 20

// seedBackground gives the corpus its public channels and the caller its own
// memberships. Neither matches token, so neither can appear in any response the
// measurement takes; both exist so the arms run the plans a real server runs.
func seedBackground(t *testing.T, ctx context.Context, s *store.Store, creatorID, callerID int64) {
	t.Helper()
	for i := range costBackgroundPublic {
		ch, err := s.CreateChannel(ctx, creatorID, fmt.Sprintf("Bulletin %d", i), "About", false)
		if err != nil {
			t.Fatalf("create public channel %d: %v", i, err)
		}
		if err := api.ClaimChannelUsernameForTest(s, ch.ID, fmt.Sprintf("bulletin%d", i)); err != nil {
			t.Fatalf("claim username %d: %v", i, err)
		}
	}
	for i := range costBackgroundMemberships {
		// The caller creates them, so it holds an unbanned role-2 row on each.
		if _, err := s.CreateChannel(ctx, callerID, fmt.Sprintf("Ledger %d", i), "About", false); err != nil {
			t.Fatalf("create member channel %d: %v", i, err)
		}
	}
}

// seedPrivateChannels adds n channels whose titles all match token and which
// hold no username, so none of them is publicly discoverable and none has a
// member. They are inserted in one statement rather than through CreateChannel:
// this seeds a corpus, not channels anyone acts on, and 100k round trips would
// dominate the measurement the test exists to take. Ids sit below 2^31 so they
// cannot collide with the random ids CreateChannel draws for the background.
func seedPrivateChannels(t *testing.T, ctx context.Context, dsn string, creatorID int64, token string, n, idOffset int) {
	t.Helper()
	channelExec(t, ctx, dsn, `
		INSERT INTO channels (id, title, about, creator_id)
		SELECT $4 + g, $1 || ' Circle ' || g, '', $2
		FROM generate_series(1, $3) g`, token, creatorID, n, idOffset)
	// Without fresh statistics the planner works from the table it was last
	// analysed on and picks the same plan at every size, which would hide
	// exactly the flip this test is looking for.
	channelExec(t, ctx, dsn, `ANALYZE`)
}

// searchMinLatency runs contacts.search costSearchRuns times and returns the
// fastest end-to-end call. Every response must be empty: a corpus of private
// channels the caller is not in is invisible to it, so anything returned means
// the test is measuring the wrong thing.
func searchMinLatency(t *testing.T, s *store.Store, callerID int64, query string) time.Duration {
	t.Helper()
	best := time.Duration(1<<63 - 1)
	for range costSearchRuns {
		start := time.Now()
		found := searchChannels(t, s, callerID, query, 10)
		elapsed := time.Since(start)
		if len(found.Results) != 0 || len(found.MyResults) != 0 {
			t.Fatalf("search returned rows: Results=%d MyResults=%d, want an empty response",
				len(found.Results), len(found.MyResults))
		}
		if elapsed < best {
			best = elapsed
		}
	}
	return best
}

// TestContactsSearchCostIndependentOfPrivateChannelCount is the M16 timing gate.
//
// Both channel arms of contacts.search once matched their candidate rows off
// the whole-table title index, so a token naming no public channel still cost
// one probe per private channel sharing it. The response was empty either way,
// but its latency counted private channels for a caller entitled to know about
// none of them — an aggregate existence oracle over private channels, paced by
// a guessed word.
//
// The gate is the latency itself, not the plan: a plan measured flat on a
// seeded corpus can flip back under production statistics, and "index-driven"
// does not separate the safe shape from the leaking one — a bitmap scan of the
// full title index followed by a membership filter is index-driven and is
// exactly the leak. What must hold is that the cost of an empty answer does not
// move when the private corpus behind it grows by four orders of magnitude.
func TestContactsSearchCostIndependentOfPrivateChannelCount(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)

	creator, err := s.CreateUser(ctx, "15550009001")
	if err != nil {
		t.Fatalf("creator: %v", err)
	}
	// The caller belongs to channels of its own and none of them matches the
	// token, which is the exact caller the oracle served: entitled to know
	// about none of the private channels the query would count.
	caller, err := s.CreateUser(ctx, "15550009002")
	if err != nil {
		t.Fatalf("caller: %v", err)
	}

	const token = "zeppelin"
	seedBackground(t, ctx, s, creator.ID, caller.ID)

	latencies := make([]time.Duration, 0, len(privateChannelCounts))
	seeded := 0
	for _, n := range privateChannelCounts {
		seedPrivateChannels(t, ctx, dsn, creator.ID, token, n-seeded, seeded)
		seeded = n
		latencies = append(latencies, searchMinLatency(t, s, caller.ID, token))
	}

	lo, hi := latencies[0], latencies[0]
	for _, d := range latencies {
		if d < lo {
			lo = d
		}
		if d > hi {
			hi = d
		}
	}
	for i, n := range privateChannelCounts {
		t.Logf("%d private channels matching %q: %v", n, token, latencies[i])
	}
	if hi > time.Duration(costFlatFactor)*lo {
		t.Fatalf("empty-response latency tracks the private-channel count: %s, spread %.1fx over the %dx allowed",
			describeLatencies(latencies), float64(hi)/float64(lo), costFlatFactor)
	}
}

// describeLatencies renders the measurement for a failure message, so the
// report names which corpus size stepped rather than only that one did.
func describeLatencies(latencies []time.Duration) string {
	parts := make([]string, len(latencies))
	for i, d := range latencies {
		parts[i] = fmt.Sprintf("N=%d %v", privateChannelCounts[i], d)
	}
	return strings.Join(parts, ", ")
}
