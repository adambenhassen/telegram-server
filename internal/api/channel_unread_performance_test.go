//go:build performance

package api

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/pgtest"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestChannelUnreadSummaryP99At500Memberships(t *testing.T) {
	t.Parallel()
	const (
		membershipCount = 500
		sessionCount    = 5
		requests        = 60
		channelIDBase   = int64(900_000_000_000)
	)
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	stores := make([]*store.Store, sessionCount)
	handlersBySession := make([]*handlers, sessionCount)
	for i := range stores {
		s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithoutBlobStore())
		if err != nil {
			t.Fatalf("open store session %d: %v", i, err)
		}
		stores[i] = s
		t.Cleanup(func() {
			if err := s.Close(); err != nil {
				t.Errorf("close store session %d: %v", i, err)
			}
		})
		handlersBySession[i] = &handlers{
			store:                        s,
			log:                          slog.New(slog.DiscardHandler),
			rateLimitChannelUnreadCounts: channelUnreadCountRateLimit,
		}
	}

	owner, err := stores[0].CreateUser(ctx, "+15551239981")
	if err != nil {
		t.Fatalf("create owner: %v", err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to disposable database: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close disposable database connection: %v", err)
		}
	})
	marker := int64(1) << 62
	if _, err = conn.Exec(ctx, `
INSERT INTO channels (id, title, creator_id, megagroup)
SELECT $1::bigint + item, 'unread-summary-perf', $2::bigint, true
FROM generate_series(1, $3::int) AS item`, channelIDBase, owner.ID, membershipCount); err != nil {
		t.Fatalf("seed channels: %v", err)
	}
	if _, err = conn.Exec(ctx, `
INSERT INTO channel_state (channel_id, next_local_id)
SELECT channel.id, $2::bigint + 1
FROM channels AS channel
WHERE channel.id > $1::bigint AND channel.id <= $1::bigint + $3::int`, channelIDBase, marker, membershipCount); err != nil {
		t.Fatalf("seed channel state: %v", err)
	}
	if _, err = conn.Exec(ctx, `
INSERT INTO channel_participants (channel_id, user_id, role, join_pts)
SELECT channel.id, $2::bigint, 2, 0
FROM channels AS channel
WHERE channel.id > $1::bigint AND channel.id <= $1::bigint + $3::int`, channelIDBase, owner.ID, membershipCount); err != nil {
		t.Fatalf("seed channel memberships: %v", err)
	}
	if _, err = conn.Exec(ctx, `
INSERT INTO channel_messages (channel_id, local_id, from_id, message)
SELECT channel.id, $2::bigint + 1, $3::bigint, 'owner post'
FROM channels AS channel
WHERE channel.id > $1::bigint AND channel.id <= $1::bigint + $4::int`, channelIDBase, marker, owner.ID, membershipCount); err != nil {
		t.Fatalf("seed summary contributions: %v", err)
	}
	if _, err = conn.Exec(ctx, `
UPDATE channel_state
SET next_local_id = $2::bigint + 2
WHERE channel_id > $1::bigint AND channel_id <= $1::bigint + $3::int`, channelIDBase, marker, membershipCount); err != nil {
		t.Fatalf("advance channel tops: %v", err)
	}

	planRows, err := conn.Query(ctx, `
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT live_count
FROM channel_post_summaries
WHERE channel_id = $1::bigint
  AND scope_kind = 0
  AND author_id = 0
  AND depth = 63
  AND prefix = $2::bigint`, channelIDBase+1, marker+1)
	if err != nil {
		t.Fatalf("explain disposable summary key lookup: %v", err)
	}
	var plan []string
	for planRows.Next() {
		var line string
		if err := planRows.Scan(&line); err != nil {
			planRows.Close()
			t.Fatalf("read summary plan: %v", err)
		}
		plan = append(plan, line)
	}
	if err := planRows.Err(); err != nil {
		planRows.Close()
		t.Fatalf("read summary plan rows: %v", err)
	}
	planRows.Close()
	planText := strings.Join(plan, "\n")
	t.Logf("disposable equality-key EXPLAIN (ANALYZE, BUFFERS):\n%s", planText)
	if !strings.Contains(planText, "channel_post_summaries_pkey") {
		t.Fatalf("summary plan did not use the primary key: %s", planText)
	}

	state, err := stores[0].State(ctx, owner.ID)
	if err != nil || state.UnreadCount != 0 {
		t.Fatalf("warm summary state = %+v, err %v; want unread 0", state, err)
	}
	latencies := make(chan time.Duration, requests)
	errors := make(chan error, requests)
	start := make(chan struct{})
	var wait sync.WaitGroup
	for session := range sessionCount {
		wait.Add(1)
		go func(session int) {
			defer wait.Done()
			for request := 0; request < requests/sessionCount; request++ {
				<-start
				begin := time.Now()
				req := &mtproto.Request{Ctx: ctx, UserID: owner.ID}
				if err := handlersBySession[session].checkChannelUnreadCountRateLimit(req); err != nil {
					errors <- fmt.Errorf("session %d request %d admission: %w", session, request, err)
					continue
				}
				state, err := stores[session].State(ctx, owner.ID)
				latencies <- time.Since(begin)
				if err != nil {
					errors <- fmt.Errorf("session %d request %d state: %w", session, request, err)
				} else if state.UnreadCount != 0 {
					errors <- fmt.Errorf("session %d request %d unread total = %d, want 0", session, request, state.UnreadCount)
				}
			}
		}(session)
	}
	close(start)
	wait.Wait()
	close(latencies)
	close(errors)
	for err := range errors {
		t.Error(err)
	}
	samples := make([]time.Duration, 0, requests)
	for latency := range latencies {
		samples = append(samples, latency)
	}
	if len(samples) != requests {
		t.Fatalf("completed %d of %d summary requests", len(samples), requests)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p99 := samples[int(math.Ceil(0.99*float64(len(samples))))-1]
	t.Logf("500 memberships, %d sessions, %d admitted requests at %d/min: p50=%s p99=%s", sessionCount, requests, channelUnreadCountRateLimit.Limit, samples[len(samples)/2], p99)
	if p99 > time.Second {
		t.Fatalf("p99 %s exceeds the one-second per-account sustained interval", p99)
	}
}
