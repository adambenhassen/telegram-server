package store_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/teagramhq/teagram-server/internal/store"
)

func TestChannelPostSummarySchemaIsInstalled(t *testing.T) {
	t.Parallel()
	s := open(t)

	installed, err := store.ChannelPostSummarySchemaInstalled(context.Background(), s)
	if err != nil {
		t.Fatalf("check channel post summary schema: %v", err)
	}
	if !installed {
		t.Fatal("channel post summary schema is absent")
	}
}

func TestChannelPostSummaryInitializationCountsExactSuffixes(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551269001")
	reader := mustUser(t, s, "+15551269002")
	channel := mustChannel(t, s, creator.ID, "summary-init")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}

	// Simulate a pre-migration channel: old membership and markers remain, but
	// there is no readiness row and source writers skip derived maintenance.
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`DELETE FROM channel_post_summary_state WHERE channel_id = $1`, channel.ID); err != nil {
		t.Fatalf("remove simulated pre-migration readiness: %v", err)
	}
	if err = store.SetChannelStateNextLocalID(ctx, s, channel.ID, 2009); err != nil {
		t.Fatalf("set committed top: %v", err)
	}
	if err = store.InsertChannelPostRunForTest(ctx, s, channel.ID, 2, 1003, reader.ID, false); err != nil {
		t.Fatalf("seed leading self-authored run: %v", err)
	}
	if err = store.InsertChannelPostRunForTest(ctx, s, channel.ID, 1004, 1005, creator.ID, false); err != nil {
		t.Fatalf("seed inbound posts: %v", err)
	}
	if err = store.InsertChannelPostRunForTest(ctx, s, channel.ID, 1006, 1006, creator.ID, true); err != nil {
		t.Fatalf("seed deleted post: %v", err)
	}
	if err = store.InsertChannelPostRunForTest(ctx, s, channel.ID, 1007, 2008, reader.ID, false); err != nil {
		t.Fatalf("seed trailing self-authored run: %v", err)
	}

	before, err := store.ChannelPostSourceFingerprints(ctx, s, channel.ID)
	if err != nil {
		t.Fatalf("fingerprint source state before initialization: %v", err)
	}
	if err = s.InitializeChannelPostSummaries(ctx, channel.ID); err != nil {
		t.Fatalf("initialize summaries: %v", err)
	}
	after, err := store.ChannelPostSourceFingerprints(ctx, s, channel.ID)
	if err != nil {
		t.Fatalf("fingerprint source state after initialization: %v", err)
	}
	if before != after {
		t.Fatalf("initialization changed source state fingerprints: before %v, after %v", before, after)
	}

	for _, tc := range []struct {
		name      string
		scopeKind int16
		authorID  int64
		want      int64
	}{
		{name: "total live posts", scopeKind: 0, want: 2007},
		{name: "reader contribution", scopeKind: 1, authorID: reader.ID, want: 2004},
		{name: "creator contribution", scopeKind: 1, authorID: creator.ID, want: 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.ChannelPostSummaryRootCount(ctx, s, channel.ID, tc.scopeKind, tc.authorID)
			if err != nil {
				t.Fatalf("read root contribution: %v", err)
			}
			if got != tc.want {
				t.Fatalf("root contribution = %d, want %d", got, tc.want)
			}
		})
	}

	for _, tc := range []struct {
		marker int64
		want   int64
	}{{marker: 0, want: 3}, {marker: 1004, want: 1}} {
		got, err := s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, tc.marker)
		if err != nil {
			t.Fatalf("count suffix after %d: %v", tc.marker, err)
		}
		if got != tc.want {
			t.Fatalf("unread suffix after %d = %d, want %d", tc.marker, got, tc.want)
		}
	}
	status, err := s.ChannelPostSummaryReadiness(ctx, channel.ID)
	if err != nil || status != store.ChannelPostSummaryReady {
		t.Fatalf("readiness = %v, err %v; want ready", status, err)
	}
	if err = s.InitializeChannelPostSummaries(ctx, channel.ID); err != nil {
		t.Fatalf("repeat initialization: %v", err)
	}
	got, err := s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, 0)
	if err != nil || got != 3 {
		t.Fatalf("unread after idempotent rebuild = %d, err %v; want 3", got, err)
	}
}

func TestChannelPostSummaryTriggersMaintainMutationsAndFailClosed(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551269011")
	reader := mustUser(t, s, "+15551269012")
	channel := mustChannel(t, s, creator.ID, "summary-mutations")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}

	message, _ := post(t, s, channel.ID, creator.ID, "one", 72101)
	if message.LocalID != 2 {
		t.Fatalf("post local ID = %d, want 2", message.LocalID)
	}
	if _, _, dup, err := s.PostChannelMessage(ctx, channel.ID, creator.ID, "one", 72101, nil, 0); err != nil || !dup {
		t.Fatalf("dedup post duplicate=%v err=%v", dup, err)
	}
	deliveryBefore, err := store.ChannelPostDeliveryFingerprints(ctx, s, channel.ID)
	if err != nil {
		t.Fatalf("fingerprint delivery state before trigger-only changes: %v", err)
	}
	listener, err := store.ListenChannelPostNotificationsForTest(ctx, s)
	if err != nil {
		t.Fatalf("listen for channel post notifications: %v", err)
	}
	defer func() {
		if err := listener.Close(ctx); err != nil {
			t.Errorf("close notification listener: %v", err)
		}
	}()
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET message = 'edited' WHERE channel_id = $1 AND local_id = $2`, channel.ID, message.LocalID); err != nil {
		t.Fatalf("edit post content: %v", err)
	}
	notifyCtx, cancelNotify := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelNotify()
	if notification, notifyErr := listener.WaitForNotification(notifyCtx); notifyErr == nil {
		t.Fatalf("source trigger emitted notification %q", notification.Payload)
	} else if !errors.Is(notifyErr, context.DeadlineExceeded) {
		t.Fatalf("wait for source-trigger notification: %v", notifyErr)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET from_id = $3 WHERE channel_id = $1 AND local_id = $2`, channel.ID, message.LocalID, reader.ID); err != nil {
		t.Fatalf("change live author: %v", err)
	}
	got, err := s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, 1)
	if err != nil || got != 0 {
		t.Fatalf("self-authored post count = %d, err %v; want 0", got, err)
	}
	otherChannel := mustChannel(t, s, creator.ID, "summary-mutation-target")
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET channel_id = $2 WHERE channel_id = $1 AND local_id = $3`, channel.ID, otherChannel.ID, message.LocalID); err != nil {
		t.Fatalf("move live post to another channel: %v", err)
	}
	if got, err = s.ChannelPostUnreadCount(ctx, channel.ID, creator.ID, 0); err != nil || got != 0 {
		t.Fatalf("old channel count after identity move = %d, err %v; want 0", got, err)
	}
	if got, err = s.ChannelPostUnreadCount(ctx, otherChannel.ID, creator.ID, 0); err != nil || got != 1 {
		t.Fatalf("new channel count after identity move = %d, err %v; want 1", got, err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET channel_id = $2 WHERE channel_id = $1 AND local_id = $3`, otherChannel.ID, channel.ID, message.LocalID); err != nil {
		t.Fatalf("move live post back to original channel: %v", err)
	}
	movedLocalID := message.LocalID + 1
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET local_id = $2 WHERE channel_id = $1 AND local_id = $3`, channel.ID, movedLocalID, message.LocalID); err != nil {
		t.Fatalf("change live local ID: %v", err)
	}
	got, err = s.ChannelPostUnreadCount(ctx, channel.ID, creator.ID, 1)
	if err != nil || got != 1 {
		t.Fatalf("moved post suffix = %d, err %v; want 1", got, err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`, channel.ID, movedLocalID); err != nil {
		t.Fatalf("delete live post: %v", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`, channel.ID, movedLocalID); err != nil {
		t.Fatalf("repeat deletion: %v", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET file_id = NULL WHERE channel_id = $1 AND deleted = true`, channel.ID); err != nil {
		t.Fatalf("clear deleted file reference: %v", err)
	}
	got, err = s.ChannelPostUnreadCount(ctx, channel.ID, creator.ID, 0)
	if err != nil || got != 0 {
		t.Fatalf("count after deletion and file cleanup = %d, err %v; want 0", got, err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_messages SET deleted = false WHERE channel_id = $1 AND local_id = $2`, channel.ID, movedLocalID); err != nil {
		t.Fatalf("restore post: %v", err)
	}
	got, err = s.ChannelPostUnreadCount(ctx, channel.ID, creator.ID, 0)
	if err != nil || got != 1 {
		t.Fatalf("count after restore = %d, err %v; want 1", got, err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`DELETE FROM channel_messages WHERE channel_id = $1 AND local_id = $2`, channel.ID, movedLocalID); err != nil {
		t.Fatalf("physically delete live post: %v", err)
	}
	notifyCtx, cancelNotify = context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelNotify()
	if notification, notifyErr := listener.WaitForNotification(notifyCtx); notifyErr == nil {
		t.Fatalf("source identity/deletion trigger emitted notification %q", notification.Payload)
	} else if !errors.Is(notifyErr, context.DeadlineExceeded) {
		t.Fatalf("wait for source-trigger notification: %v", notifyErr)
	}
	deliveryAfter, err := store.ChannelPostDeliveryFingerprints(ctx, s, channel.ID)
	if err != nil {
		t.Fatalf("fingerprint delivery state after trigger-only changes: %v", err)
	}
	if deliveryBefore != deliveryAfter {
		t.Fatalf("source triggers changed delivery state: before %v, after %v", deliveryBefore, deliveryAfter)
	}

	if err = store.ExecChannelPostSummarySQL(ctx, s, `TRUNCATE channel_messages`); err == nil {
		t.Fatal("TRUNCATE channel_messages succeeded, want refusal")
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s, `
		INSERT INTO channel_messages (channel_id, local_id, from_id, message)
		VALUES ($1, 0, $2, 'invalid')
	`, channel.ID, creator.ID); err == nil {
		t.Fatal("nonpositive local ID insert succeeded, want refusal")
	}

	message, _ = post(t, s, channel.ID, creator.ID, "underflow", 72102)
	if message.ChannelID == 0 {
		t.Fatal("underflow test post has no channel ID")
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s, `
		DELETE FROM channel_post_summaries
		 WHERE channel_id = $1 AND scope_kind = 0 AND author_id = 0
		   AND depth = 0 AND prefix = 0
	`, channel.ID); err != nil {
		t.Fatalf("remove total root for underflow test: %v", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`DELETE FROM channel_messages WHERE channel_id = $1 AND local_id = $2`, channel.ID, message.LocalID); err == nil {
		t.Fatal("summary underflow deletion succeeded, want source write refusal")
	}
	messages, err := s.ChannelMessages(ctx, channel.ID, []int64{message.LocalID})
	if err != nil || len(messages) != 1 || messages[message.LocalID].Deleted {
		t.Fatalf("underflow source row = %+v, err %v; want live row retained", messages, err)
	}
	if err = s.InitializeChannelPostSummaries(ctx, channel.ID); err != nil {
		t.Fatalf("repair derived rows after underflow fixture: %v", err)
	}
}

func TestChannelPostSummaryInitializationAndSourceMutationsSerialize(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	creator := mustUser(t, s, "+15551269051")
	reader := mustUser(t, s, "+15551269052")
	channel := mustChannel(t, s, creator.ID, "summary-init-race")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	first, _ := post(t, s, channel.ID, creator.ID, "first", 72401)
	second, _ := post(t, s, channel.ID, creator.ID, "second", 72402)
	if first.LocalID != 2 || second.LocalID != 3 {
		t.Fatalf("seed local IDs = %d and %d, want 2 and 3", first.LocalID, second.LocalID)
	}
	readCommittedLocalID := second.LocalID + 1
	repeatableReadLocalID := second.LocalID + 2

	conn, err := store.ChannelPostSummaryControlConnection(ctx, s)
	if err != nil {
		t.Fatalf("open summary race control connection: %v", err)
	}
	barrierHeld := false
	releaseBarrier := func() {
		if !barrierHeld {
			return
		}
		if _, unlockErr := conn.Exec(context.Background(), `SELECT pg_advisory_unlock(1149, 1150)`); unlockErr != nil {
			t.Errorf("release summary initialization barrier: %v", unlockErr)
		}
		barrierHeld = false
	}
	defer func() {
		releaseBarrier()
		if _, dropErr := conn.Exec(context.Background(), `DROP TRIGGER IF EXISTS test_channel_summary_init_barrier ON channel_post_summaries`); dropErr != nil {
			t.Errorf("drop summary initialization barrier trigger: %v", dropErr)
		}
		if _, dropErr := conn.Exec(context.Background(), `DROP FUNCTION IF EXISTS test_channel_summary_init_barrier()`); dropErr != nil {
			t.Errorf("drop summary initialization barrier function: %v", dropErr)
		}
		if closeErr := conn.Close(context.Background()); closeErr != nil {
			t.Errorf("close summary race control connection: %v", closeErr)
		}
	}()
	if _, err = conn.Exec(ctx, `
		CREATE FUNCTION test_channel_summary_init_barrier() RETURNS trigger AS $body$
		BEGIN
		    PERFORM pg_advisory_xact_lock(1149, 1150);
		    RETURN NEW;
		END;
		$body$ LANGUAGE plpgsql
	`); err != nil {
		t.Fatalf("create summary initialization barrier function: %v", err)
	}
	if _, err = conn.Exec(ctx, `
		CREATE TRIGGER test_channel_summary_init_barrier
		BEFORE INSERT ON channel_post_summaries
		FOR EACH ROW EXECUTE FUNCTION test_channel_summary_init_barrier()
	`); err != nil {
		t.Fatalf("create summary initialization barrier trigger: %v", err)
	}
	if _, err = conn.Exec(ctx, `SELECT pg_advisory_lock(1149, 1150)`); err != nil {
		t.Fatalf("hold summary initialization barrier: %v", err)
	}
	barrierHeld = true
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`DELETE FROM channel_post_summary_state WHERE channel_id = $1`, channel.ID); err != nil {
		t.Fatalf("remove readiness row for race: %v", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`DELETE FROM channel_post_summaries WHERE channel_id = $1`, channel.ID); err != nil {
		t.Fatalf("clear derived rows for race: %v", err)
	}

	staleConn, err := store.ChannelPostSummaryControlConnection(ctx, s)
	if err != nil {
		t.Fatalf("open repeatable-read writer connection: %v", err)
	}
	defer func() {
		if closeErr := staleConn.Close(context.Background()); closeErr != nil {
			t.Errorf("close repeatable-read writer connection: %v", closeErr)
		}
	}()
	staleTx, err := staleConn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatalf("begin repeatable-read writer: %v", err)
	}
	var readinessExists bool
	if err = staleTx.QueryRow(ctx, `
		SELECT EXISTS (SELECT 1 FROM channel_post_summary_state WHERE channel_id = $1)
	`, channel.ID).Scan(&readinessExists); err != nil {
		t.Fatalf("establish stale readiness snapshot: %v", err)
	}
	if readinessExists {
		t.Fatal("repeatable-read writer snapshot unexpectedly sees readiness")
	}

	initDone := make(chan error, 1)
	go func() { initDone <- s.InitializeChannelPostSummaries(ctx, channel.ID) }()
	if err = store.WaitForLockWaiters(ctx, s, 1); err != nil {
		releaseBarrier()
		initErr := <-initDone
		t.Fatalf("wait for initialization to reach the summary barrier: %v (initializer returned %v)", err, initErr)
	}

	postDone := make(chan error, 1)
	go func() {
		postDone <- store.ExecChannelPostSummarySQL(ctx, s, `
			INSERT INTO channel_messages (channel_id, local_id, from_id, message)
			VALUES ($1, $2, $3, 'old-binary post during initialization')
		`, channel.ID, readCommittedLocalID, creator.ID)
	}()
	repeatablePostDone := make(chan error, 1)
	go func() {
		_, writeErr := staleTx.Exec(ctx, `
			INSERT INTO channel_messages (channel_id, local_id, from_id, message)
			VALUES ($1, $2, $3, 'repeatable-read post after initialization')
		`, channel.ID, repeatableReadLocalID, creator.ID)
		if writeErr == nil {
			writeErr = staleTx.Commit(ctx)
		}
		repeatablePostDone <- writeErr
	}()
	if err = store.WaitForLockWaiters(ctx, s, 3); err != nil {
		releaseBarrier()
		initErr, postErr, repeatablePostErr := <-initDone, <-postDone, <-repeatablePostDone
		t.Fatalf("wait for source writers behind initialization: %v (initializer %v, read committed %v, repeatable read %v)", err, initErr, postErr, repeatablePostErr)
	}
	releaseBarrier()
	if err = <-initDone; err != nil {
		t.Fatalf("initialize channel in overlap: %v", err)
	}
	if err = <-postDone; err != nil {
		t.Fatalf("commit old-binary post after initialization: %v", err)
	}
	repeatablePostErr := <-repeatablePostDone
	if repeatablePostErr == nil {
		t.Fatal("repeatable-read source mutation committed with a stale readiness snapshot")
	}
	if !strings.Contains(repeatablePostErr.Error(), "require READ COMMITTED") {
		t.Fatalf("repeatable-read source mutation error = %v, want unsupported-isolation refusal", repeatablePostErr)
	}
	if err = staleTx.Rollback(ctx); err != nil {
		t.Fatalf("roll back rejected repeatable-read writer: %v", err)
	}
	if got, err := store.ChannelPostSummaryRootCount(ctx, s, channel.ID, 0, 0); err != nil || got != 4 {
		t.Fatalf("root count after init/post overlap = %d, err %v; want 4", got, err)
	}

	// Hold the state row while two independent source writes reach their AFTER
	// triggers. Both must serialize their derived deltas through this row.
	stateHold, err := store.BeginChannelPostSummaryStateHold(ctx, s)
	if err != nil {
		t.Fatalf("begin source mutation state hold: %v", err)
	}
	defer func() { _ = stateHold.Rollback(context.Background()) }() //nolint:errcheck // no-op after commit
	if _, err = stateHold.Exec(ctx, `SELECT channel_id FROM channel_state WHERE channel_id = $1 FOR UPDATE`, channel.ID); err != nil {
		t.Fatalf("hold channel state for source mutation overlap: %v", err)
	}
	deleteDone := make(chan error, 1)
	go func() {
		deleteDone <- store.ExecChannelPostSummarySQL(ctx, s,
			`UPDATE channel_messages SET deleted = true WHERE channel_id = $1 AND local_id = $2`, channel.ID, first.LocalID)
	}()
	moveDone := make(chan error, 1)
	go func() {
		moveDone <- store.ExecChannelPostSummarySQL(ctx, s,
			`UPDATE channel_messages SET local_id = $2 WHERE channel_id = $1 AND local_id = $3`, channel.ID, repeatableReadLocalID, second.LocalID)
	}()
	if err = store.WaitForLockWaiters(ctx, s, 2); err != nil {
		_ = stateHold.Rollback(context.Background()) //nolint:errcheck // release waiters before failing
		deleteErr, moveErr := <-deleteDone, <-moveDone
		t.Fatalf("wait for source mutations at channel_state: %v (delete %v, move %v)", err, deleteErr, moveErr)
	}
	if err = stateHold.Commit(ctx); err != nil {
		t.Fatalf("release source mutation state hold: %v", err)
	}
	if err = <-deleteDone; err != nil {
		t.Fatalf("commit concurrent source deletion: %v", err)
	}
	if err = <-moveDone; err != nil {
		t.Fatalf("commit concurrent live identity move: %v", err)
	}
	if got, err := store.ChannelPostSummaryRootCount(ctx, s, channel.ID, 0, 0); err != nil || got != 3 {
		t.Fatalf("root count after source mutation overlap = %d, err %v; want 3", got, err)
	}
	if got, err := s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, 1); err != nil || got != 2 {
		t.Fatalf("unread suffix after source mutation overlap = %d, err %v; want 2", got, err)
	}
}

func TestChannelReadMarkerAdmissionAndMonotonicAdvance(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551269021")
	oldMember := mustUser(t, s, "+15551269022")
	newMember := mustUser(t, s, "+15551269023")
	channel := mustChannel(t, s, creator.ID, "read-markers")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, oldMember.ID); err != nil {
		t.Fatalf("join old member: %v", err)
	}
	_, _ = post(t, s, channel.ID, creator.ID, "first", 72201)
	second, _ := post(t, s, channel.ID, creator.ID, "second", 72202)
	if _, _, err = s.JoinChannelByInvite(ctx, invite, newMember.ID); err != nil {
		t.Fatalf("join new member: %v", err)
	}
	marker, err := s.ChannelReadMarker(ctx, channel.ID, newMember.ID)
	if err != nil || marker != second.LocalID {
		t.Fatalf("new member marker = %d, err %v; want %d", marker, err, second.LocalID)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_read_state SET read_max_id = 1 WHERE channel_id = $1 AND user_id = $2`, channel.ID, newMember.ID); err != nil {
		t.Fatalf("set existing marker for rejoin preservation: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, newMember.ID); err != nil {
		t.Fatalf("repeat admission: %v", err)
	}
	marker, err = s.ChannelReadMarker(ctx, channel.ID, newMember.ID)
	if err != nil || marker != 1 {
		t.Fatalf("repeated admission marker = %d, err %v; want preserved 1", marker, err)
	}
	if left, err := s.LeaveChannel(ctx, channel.ID, newMember.ID); err != nil || !left {
		t.Fatalf("leave before re-admission: left=%v err=%v", left, err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, newMember.ID); err != nil {
		t.Fatalf("re-admit removed member: %v", err)
	}
	marker, err = s.ChannelReadMarker(ctx, channel.ID, newMember.ID)
	if err != nil || marker != second.LocalID {
		t.Fatalf("re-admitted member marker = %d, err %v; want new top %d", marker, err, second.LocalID)
	}
	marker, err = s.ChannelReadMarker(ctx, channel.ID, oldMember.ID)
	if err != nil || marker != 1 {
		t.Fatalf("existing member marker = %d, err %v; want 1", marker, err)
	}
	read, err := s.AdvanceChannelReadMarker(ctx, channel.ID, oldMember.ID, 1)
	if err != nil || read != 1 {
		t.Fatalf("advance partial marker = %d, err %v; want 1", read, err)
	}
	read, err = s.AdvanceChannelReadMarker(ctx, channel.ID, oldMember.ID, 0)
	if err != nil || read != 1 {
		t.Fatalf("lower marker = %d, err %v; want 1", read, err)
	}
	read, err = s.AdvanceChannelReadMarker(ctx, channel.ID, oldMember.ID, 999)
	if err != nil || read != second.LocalID {
		t.Fatalf("future marker = %d, err %v; want committed top %d", read, err, second.LocalID)
	}
	read, err = s.AdvanceChannelReadMarker(ctx, channel.ID, oldMember.ID, 0)
	if err != nil || read != second.LocalID {
		t.Fatalf("replayed marker = %d, err %v; want %d", read, err, second.LocalID)
	}
	got, err := s.ChannelPostUnreadCount(ctx, channel.ID, oldMember.ID, read)
	if err != nil || got != 0 {
		t.Fatalf("count after full marker = %d, err %v; want 0", got, err)
	}
	if got, err = s.ChannelPostUnreadCount(ctx, channel.ID, newMember.ID, second.LocalID); err != nil || got != 0 {
		t.Fatalf("new-member suffix = %d, err %v; want 0", got, err)
	}
}

func TestChannelPostSummaryReadinessDistinguishesEmptyMissingAndWrongVersion(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551269031")
	channel := mustChannel(t, s, creator.ID, "summary-readiness")

	status, err := s.ChannelPostSummaryReadiness(ctx, channel.ID)
	if err != nil || status != store.ChannelPostSummaryReady {
		t.Fatalf("new empty channel readiness = %v, err %v; want ready", status, err)
	}
	if err = s.ValidateChannelPostSummariesReady(ctx); err != nil {
		t.Fatalf("validate empty ready channel: %v", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_post_summary_state SET version = 2 WHERE channel_id = $1`, channel.ID); err != nil {
		t.Fatalf("set wrong summary version: %v", err)
	}
	status, err = s.ChannelPostSummaryReadiness(ctx, channel.ID)
	if err != nil || status != store.ChannelPostSummaryWrongVersion {
		t.Fatalf("wrong-version readiness = %v, err %v", status, err)
	}
	if _, err = s.ChannelPostUnreadCount(ctx, channel.ID, creator.ID, 0); !errors.Is(err, store.ErrChannelPostSummaryWrongVersion) {
		t.Fatalf("count with wrong version = %v, want wrong-version refusal", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`UPDATE channel_post_summary_state SET version = 1, ready = false WHERE channel_id = $1`, channel.ID); err != nil {
		t.Fatalf("mark summaries not ready: %v", err)
	}
	status, err = s.ChannelPostSummaryReadiness(ctx, channel.ID)
	if err != nil || status != store.ChannelPostSummaryNotReady {
		t.Fatalf("not-ready status = %v, err %v", status, err)
	}
	if _, err = s.ChannelPostUnreadCount(ctx, channel.ID, creator.ID, 0); !errors.Is(err, store.ErrChannelPostSummaryNotReady) {
		t.Fatalf("count while not ready = %v, want not-ready refusal", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`DELETE FROM channel_post_summary_state WHERE channel_id = $1`, channel.ID); err != nil {
		t.Fatalf("remove readiness row: %v", err)
	}
	status, err = s.ChannelPostSummaryReadiness(ctx, channel.ID)
	if err != nil || status != store.ChannelPostSummaryMissing {
		t.Fatalf("missing readiness status = %v, err %v", status, err)
	}
	if _, err = s.ChannelPostUnreadCount(ctx, channel.ID, creator.ID, 0); !errors.Is(err, store.ErrChannelPostSummaryMissing) {
		t.Fatalf("count without readiness row = %v, want missing-row refusal", err)
	}
}

func TestChannelPostSummaryFailedRebuildStaysUnavailableAndRetries(t *testing.T) {
	t.Parallel()
	s := open(t)
	ctx := context.Background()
	creator := mustUser(t, s, "+15551269041")
	reader := mustUser(t, s, "+15551269042")
	channel := mustChannel(t, s, creator.ID, "summary-retry")
	invite, err := s.CreateChannelInvite(ctx, channel.ID, creator.ID)
	if err != nil {
		t.Fatalf("create invite: %v", err)
	}
	if _, _, err = s.JoinChannelByInvite(ctx, invite, reader.ID); err != nil {
		t.Fatalf("join reader: %v", err)
	}
	readerMarker, err := s.ChannelReadMarker(ctx, channel.ID, reader.ID)
	if err != nil {
		t.Fatalf("read reader marker: %v", err)
	}
	_, _ = post(t, s, channel.ID, creator.ID, "live", 72301)
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`ALTER TABLE channel_post_summaries ADD CONSTRAINT channel_post_summary_test_fail CHECK (false) NOT VALID`); err != nil {
		t.Fatalf("install rebuild failure constraint: %v", err)
	}
	if err = s.InitializeChannelPostSummaries(ctx, channel.ID); err == nil {
		t.Fatal("initialization succeeded with a failing derived-row constraint")
	}
	status, err := s.ChannelPostSummaryReadiness(ctx, channel.ID)
	if err != nil || status != store.ChannelPostSummaryNotReady {
		t.Fatalf("failed rebuild readiness = %v, err %v; want not ready", status, err)
	}
	if _, err = s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, readerMarker); !errors.Is(err, store.ErrChannelPostSummaryNotReady) {
		t.Fatalf("count after failed rebuild = %v, want not-ready refusal", err)
	}
	if err = store.ExecChannelPostSummarySQL(ctx, s,
		`ALTER TABLE channel_post_summaries DROP CONSTRAINT channel_post_summary_test_fail`); err != nil {
		t.Fatalf("remove rebuild failure constraint: %v", err)
	}
	if err = s.InitializeChannelPostSummaries(ctx, channel.ID); err != nil {
		t.Fatalf("retry initialization: %v", err)
	}
	status, err = s.ChannelPostSummaryReadiness(ctx, channel.ID)
	if err != nil || status != store.ChannelPostSummaryReady {
		t.Fatalf("retry readiness = %v, err %v; want ready", status, err)
	}
	got, err := s.ChannelPostUnreadCount(ctx, channel.ID, reader.ID, readerMarker)
	if err != nil || got != 1 {
		t.Fatalf("unread after retry = %d, err %v; want 1", got, err)
	}
}
