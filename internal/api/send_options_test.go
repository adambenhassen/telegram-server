package api_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

type sendForm uint8

const (
	sendText sendForm = iota
	sendMedia
	sendForward
)

func (f sendForm) String() string {
	switch f {
	case sendText:
		return "text"
	case sendMedia:
		return "media"
	case sendForward:
		return "forward"
	default:
		return fmt.Sprintf("sendForm(%d)", f)
	}
}

type unsupportedSendOption string

const (
	scheduleTomorrow      unsupportedSendOption = "schedule_date_tomorrow"
	schedulePast          unsupportedSendOption = "schedule_date_past"
	scheduleZero          unsupportedSendOption = "schedule_date_zero"
	scheduleRepeatZero    unsupportedSendOption = "schedule_repeat_period_zero"
	quickReply            unsupportedSendOption = "quick_reply_shortcut"
	suggestedPost         unsupportedSendOption = "suggested_post"
	suggestedPostDateZero unsupportedSendOption = "suggested_post_schedule_date_zero"
)

type unsupportedSendOptionSetter interface {
	SetScheduleDate(int)
	SetScheduleRepeatPeriod(int)
	SetQuickReplyShortcut(tg.InputQuickReplyShortcutClass)
	SetSuggestedPost(tg.SuggestedPost)
}

func makeSendRequest(
	form sendForm,
	peer tg.InputPeerClass,
	userID, sourcePeerID int64,
	sourceMessageID int,
	randomID, fileID int64,
	option unsupportedSendOption,
	now time.Time,
) bin.Encoder {
	var req bin.Encoder
	switch form {
	case sendText:
		req = &tg.MessagesSendMessageRequest{
			Peer: peer, Message: "message payload", RandomID: randomID,
		}
	case sendMedia:
		req = &tg.MessagesSendMediaRequest{
			Peer: peer, Message: "media caption", RandomID: randomID,
			Media: &tg.InputMediaUploadedDocument{
				File: &tg.InputFile{ID: fileID, Parts: 1, Name: "send.txt"}, MimeType: "text/plain",
			},
		}
	case sendForward:
		req = &tg.MessagesForwardMessagesRequest{
			FromPeer: api.InputPeerUser(userID, sourcePeerID),
			ID:       []int{sourceMessageID},
			RandomID: []int64{randomID},
			ToPeer:   peer,
		}
	default:
		panic("unknown send form")
	}
	if option == "" {
		return req
	}

	setter, ok := req.(unsupportedSendOptionSetter)
	if !ok {
		panic(fmt.Sprintf("%T does not accept unsupported send options", req))
	}
	switch option {
	case scheduleTomorrow:
		setter.SetScheduleDate(int(now.Add(24 * time.Hour).Unix()))
	case schedulePast:
		setter.SetScheduleDate(int(now.Add(-24 * time.Hour).Unix()))
	case scheduleZero:
		setter.SetScheduleDate(0)
	case scheduleRepeatZero:
		setter.SetScheduleRepeatPeriod(0)
	case quickReply:
		setter.SetQuickReplyShortcut(&tg.InputQuickReplyShortcut{Shortcut: "saved"})
	case suggestedPost:
		setter.SetSuggestedPost(tg.SuggestedPost{})
	case suggestedPostDateZero:
		post := tg.SuggestedPost{}
		post.SetScheduleDate(0)
		setter.SetSuggestedPost(post)
	default:
		panic("unknown unsupported option")
	}
	return req
}

func assertRPC(t *testing.T, err error, code int, message string) {
	t.Helper()
	var rpc *tgerr.Error
	if !errors.As(err, &rpc) || rpc.Code != code || rpc.Message != message {
		t.Fatalf("RPC error = %v, want %d %s", err, code, message)
	}
}

type sendSideEffects struct {
	messageRows int64
	fileRows    int64
	rateRows    int
	states      map[int64]store.State
	eventCounts map[int64]int
}

func snapshotSendSideEffects(t *testing.T, ctx context.Context, s *store.Store, dsn string, rateOwner int64, owners ...int64) sendSideEffects {
	t.Helper()
	snapshot := sendSideEffects{
		messageRows: countMessageRows(t, ctx, dsn),
		fileRows:    countFiles(t, ctx, dsn),
		states:      make(map[int64]store.State, len(owners)),
		eventCounts: make(map[int64]int, len(owners)),
	}
	snapshot.rateRows = countRateLimitRows(t, ctx, dsn, rateOwner)
	for _, owner := range owners {
		state, err := s.State(ctx, owner)
		if err != nil {
			t.Fatalf("state for %d: %v", owner, err)
		}
		events, err := s.EventsSince(ctx, owner, 0)
		if err != nil {
			t.Fatalf("events for %d: %v", owner, err)
		}
		snapshot.states[owner] = state
		snapshot.eventCounts[owner] = len(events)
	}
	return snapshot
}

func assertSameSendSideEffects(t *testing.T, want, got sendSideEffects) {
	t.Helper()
	if got.messageRows != want.messageRows {
		t.Errorf("message rows = %d, want %d", got.messageRows, want.messageRows)
	}
	if got.fileRows != want.fileRows {
		t.Errorf("file rows = %d, want %d", got.fileRows, want.fileRows)
	}
	if got.rateRows != want.rateRows {
		t.Errorf("rate-limit rows = %d, want %d", got.rateRows, want.rateRows)
	}
	for owner, wantState := range want.states {
		if gotState := got.states[owner]; gotState != wantState {
			t.Errorf("state for %d = %+v, want %+v", owner, gotState, wantState)
		}
		if gotCount := got.eventCounts[owner]; gotCount != want.eventCounts[owner] {
			t.Errorf("event count for %d = %d, want %d", owner, gotCount, want.eventCounts[owner])
		}
	}
}

func countMessageRows(t *testing.T, ctx context.Context, dsn string) int64 {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to count messages: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close message count connection: %v", err)
		}
	}()
	var count int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM messages").Scan(&count); err != nil {
		t.Fatalf("count messages: %v", err)
	}
	return count
}

func countRateLimitRows(t *testing.T, ctx context.Context, dsn string, ownerID int64) int {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect to count rate limits: %v", err)
	}
	defer func() {
		if err := conn.Close(ctx); err != nil {
			t.Errorf("close rate-limit count connection: %v", err)
		}
	}()
	var count int
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM rate_limits WHERE subject_id = $1", ownerID).Scan(&count); err != nil {
		t.Fatalf("count rate limits: %v", err)
	}
	return count
}

func listenForUpdates(t *testing.T, ctx context.Context, dsn string) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect update listener: %v", err)
	}
	if _, err := conn.Exec(ctx, "LISTEN "+store.ChannelUpdates); err != nil {
		if closeErr := conn.Close(ctx); closeErr != nil {
			t.Errorf("close update listener after LISTEN failure: %v", closeErr)
		}
		t.Fatalf("listen for updates: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(context.Background()); err != nil {
			t.Errorf("close update listener: %v", err)
		}
	})
	return conn
}

func assertNoUpdateNotification(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	notification, err := conn.WaitForNotification(ctx)
	if err == nil {
		t.Fatalf("unexpected update notification: %+v", notification)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting for update notification: %v", err)
	}
}

func TestUnsupportedSendOptionsRejectBeforePeerResolution(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	sender, err := s.CreateUser(ctx, "+15551297001")
	if err != nil {
		t.Fatal(err)
	}
	target, err := s.CreateUser(ctx, "+15551297002")
	if err != nil {
		t.Fatal(err)
	}
	groupOwner, err := s.CreateUser(ctx, "+15551297003")
	if err != nil {
		t.Fatal(err)
	}
	groupMember, err := s.CreateUser(ctx, "+15551297004")
	if err != nil {
		t.Fatal(err)
	}
	chat, err := s.CreateChat(ctx, groupOwner.ID, "Crew", []int64{groupMember.ID})
	if err != nil {
		t.Fatalf("create chat: %v", err)
	}

	options := []unsupportedSendOption{
		scheduleTomorrow,
		schedulePast,
		scheduleZero,
		scheduleRepeatZero,
		quickReply,
		suggestedPost,
		suggestedPostDateZero,
	}
	forms := []sendForm{sendText, sendMedia, sendForward}
	owners := []int64{sender.ID, target.ID, groupOwner.ID, groupMember.ID}
	before := snapshotSendSideEffects(t, ctx, s, dsn, sender.ID, owners...)
	updates := listenForUpdates(t, ctx, dsn)

	rejectedIDs := make([]int64, 0, len(forms)*len(options)+len(forms))
	var randomID int64 = 97000
	for _, form := range forms {
		for _, option := range options {
			randomID++
			req := makeSendRequest(form, &tg.InputPeerEmpty{}, sender.ID, target.ID, 1, randomID, randomID+100000, option, time.Now())
			result, hasReplyUpdate, hasSuccessHook, commitCalls, err := api.SendAfterReplyForTest(
				s, sender.ID, req, nil, store.RateLimitConfig{Limit: 1, Window: time.Hour},
			)
			assertRPC(t, err, 400, "INPUT_REQUEST_INVALID")
			if result != nil || hasReplyUpdate || hasSuccessHook || commitCalls != 0 {
				t.Errorf("%s with %s returned result=%T reply=%v success=%v commits=%d", form, option, result, hasReplyUpdate, hasSuccessHook, commitCalls)
			}
			rejectedIDs = append(rejectedIDs, randomID)
		}
	}

	// A scheduled send to a real chat still fails before the non-member check.
	for _, form := range forms {
		randomID++
		req := makeSendRequest(form, &tg.InputPeerChat{ChatID: chat.ID}, sender.ID, target.ID, 1, randomID, randomID+100000, scheduleZero, time.Now())
		result, hasReplyUpdate, hasSuccessHook, commitCalls, err := api.SendAfterReplyForTest(
			s, sender.ID, req, nil, store.RateLimitConfig{Limit: 1, Window: time.Hour},
		)
		assertRPC(t, err, 400, "INPUT_REQUEST_INVALID")
		if result != nil || hasReplyUpdate || hasSuccessHook || commitCalls != 0 {
			t.Errorf("%s to non-member chat returned result=%T reply=%v success=%v commits=%d", form, result, hasReplyUpdate, hasSuccessHook, commitCalls)
		}
		rejectedIDs = append(rejectedIDs, randomID)
	}

	after := snapshotSendSideEffects(t, ctx, s, dsn, sender.ID, owners...)
	assertSameSendSideEffects(t, before, after)
	for _, id := range rejectedIDs {
		if _, ok, err := s.MessageByRandomID(ctx, sender.ID, id); err != nil || ok {
			t.Errorf("rejected random_id %d was reserved: found=%v err=%v", id, ok, err)
		}
	}
	assertNoUpdateNotification(t, updates)
}

func TestUnsupportedSendOptionsPreserveUnboundAuthError(t *testing.T) {
	for _, form := range []sendForm{sendText, sendMedia, sendForward} {
		t.Run(form.String(), func(t *testing.T) {
			req := makeSendRequest(form, &tg.InputPeerEmpty{}, 1, 2, 1, 98001, 98002, scheduleZero, time.Now())
			_, _, _, _, err := api.SendAfterReplyForTest(nil, 0, req, nil, store.RateLimitConfig{})
			assertRPC(t, err, 401, "AUTH_KEY_UNREGISTERED")
		})
	}
}

func TestUnsupportedSendDoesNotReserveIDOrSpendRateLimit(t *testing.T) {
	for _, form := range []sendForm{sendText, sendMedia, sendForward} {
		t.Run(form.String(), func(t *testing.T) {
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			sender, err := s.CreateUser(ctx, "+15551297101")
			if err != nil {
				t.Fatal(err)
			}
			target, err := s.CreateUser(ctx, "+15551297102")
			if err != nil {
				t.Fatal(err)
			}
			source, err := s.CreateUser(ctx, "+15551297103")
			if err != nil {
				t.Fatal(err)
			}
			var sourceMessageID int
			if form == sendForward {
				message, _, _, _, err := s.SendMessage(ctx, sender.ID, source.ID, "source", 98100, 0, 0)
				if err != nil {
					t.Fatalf("seed forward source: %v", err)
				}
				sourceMessageID = int(message.LocalID)
			}

			const randomID = 98101
			const fileID = 98102
			var blobs blob.Store
			if form == sendMedia {
				blobs = newBlobs(t)
				if _, err := api.SaveFilePartBlobsForTest(s, blobs, sender.ID, &tg.UploadSaveFilePartRequest{
					FileID: fileID, FilePart: 0, Bytes: []byte("payload"),
				}); err != nil {
					t.Fatalf("save upload part: %v", err)
				}
			}

			rateLimit := store.RateLimitConfig{Limit: 1, Window: time.Hour}
			peer := api.InputPeerUser(sender.ID, target.ID)
			rejected := makeSendRequest(form, peer, sender.ID, source.ID, sourceMessageID, randomID, fileID, scheduleZero, time.Now())
			before := snapshotSendSideEffects(t, ctx, s, dsn, sender.ID, sender.ID, target.ID, source.ID)
			_, hasReplyUpdate, hasSuccessHook, commitCalls, err := api.SendAfterReplyForTest(s, sender.ID, rejected, blobs, rateLimit)
			assertRPC(t, err, 400, "INPUT_REQUEST_INVALID")
			if hasReplyUpdate || hasSuccessHook || commitCalls != 0 {
				t.Errorf("rejected %s returned reply=%v success=%v commits=%d", form, hasReplyUpdate, hasSuccessHook, commitCalls)
			}
			afterReject := snapshotSendSideEffects(t, ctx, s, dsn, sender.ID, sender.ID, target.ID, source.ID)
			assertSameSendSideEffects(t, before, afterReject)
			if _, ok, err := s.MessageByRandomID(ctx, sender.ID, randomID); err != nil || ok {
				t.Fatalf("rejected random_id reserved: found=%v err=%v", ok, err)
			}
			if form == sendMedia {
				parts, _, _, err := s.UploadPartsSummary(ctx, sender.ID, fileID)
				if err != nil || parts != 1 {
					t.Fatalf("parts after rejection = %d, err=%v; want 1", parts, err)
				}
			}

			unscheduled := makeSendRequest(form, peer, sender.ID, source.ID, sourceMessageID, randomID, fileID, "", time.Now())
			result, err := sendWithLimits(t, s, sender.ID, form, blobs, rateLimit, unscheduled)
			if err != nil {
				t.Fatalf("unscheduled send after rejected request: %v", err)
			}
			if _, ok := result.(*tg.Updates); !ok {
				t.Fatalf("unscheduled result = %T, want *tg.Updates", result)
			}
			if _, ok, err := s.MessageByRandomID(ctx, sender.ID, randomID); err != nil || !ok {
				t.Fatalf("unscheduled send did not store random_id: found=%v err=%v", ok, err)
			}
			for _, owner := range []int64{sender.ID, target.ID} {
				state, err := s.State(ctx, owner)
				if err != nil {
					t.Fatalf("state for %d after delivery: %v", owner, err)
				}
				if state.Pts != before.states[owner].Pts+1 {
					t.Errorf("owner %d pts = %d, want %d after delivery", owner, state.Pts, before.states[owner].Pts+1)
				}
			}
			if got := countMessageRows(t, ctx, dsn); got != before.messageRows+2 {
				t.Errorf("message rows after unscheduled delivery = %d, want %d", got, before.messageRows+2)
			}
			if form == sendMedia {
				parts, _, _, err := s.UploadPartsSummary(ctx, sender.ID, fileID)
				if err != nil || parts != 0 {
					t.Errorf("parts after normal media delivery = %d, err=%v; want 0", parts, err)
				}
				if got := countFiles(t, ctx, dsn); got != before.fileRows+1 {
					t.Errorf("file rows after normal media delivery = %d, want %d", got, before.fileRows+1)
				}
			}

			second := makeSendRequest(form, peer, sender.ID, source.ID, sourceMessageID, randomID+1, fileID+1, "", time.Now())
			_, err = sendWithLimits(t, s, sender.ID, form, blobs, rateLimit, second)
			var rpc *tgerr.Error
			if !errors.As(err, &rpc) || rpc.Code != 420 || !strings.HasPrefix(rpc.Message, "FLOOD_WAIT_") {
				t.Errorf("second new %s send error = %v, want FLOOD_WAIT", form, err)
			}
		})
	}
}

func TestUnsupportedSendRetryIsRejectedBeforeDeduplication(t *testing.T) {
	for _, form := range []sendForm{sendText, sendMedia, sendForward} {
		t.Run(form.String(), func(t *testing.T) {
			ctx := context.Background()
			s, dsn := openStoreDSN(t)
			sender, err := s.CreateUser(ctx, "+15551297201")
			if err != nil {
				t.Fatal(err)
			}
			target, err := s.CreateUser(ctx, "+15551297202")
			if err != nil {
				t.Fatal(err)
			}
			source, err := s.CreateUser(ctx, "+15551297203")
			if err != nil {
				t.Fatal(err)
			}
			var sourceMessageID int
			if form == sendForward {
				message, _, _, _, err := s.SendMessage(ctx, sender.ID, source.ID, "source", 98200, 0, 0)
				if err != nil {
					t.Fatalf("seed forward source: %v", err)
				}
				sourceMessageID = int(message.LocalID)
			}

			const randomID = 98201
			const fileID = 98202
			var blobs blob.Store
			if form == sendMedia {
				blobs = newBlobs(t)
				if _, err := api.SaveFilePartBlobsForTest(s, blobs, sender.ID, &tg.UploadSaveFilePartRequest{
					FileID: fileID, FilePart: 0, Bytes: []byte("payload"),
				}); err != nil {
					t.Fatalf("save upload part: %v", err)
				}
			}
			peer := api.InputPeerUser(sender.ID, target.ID)
			delivered := makeSendRequest(form, peer, sender.ID, source.ID, sourceMessageID, randomID, fileID, "", time.Now())
			if _, err := sendWithLimits(t, s, sender.ID, form, blobs, store.RateLimitConfig{}, delivered); err != nil {
				t.Fatalf("deliver original: %v", err)
			}
			before := snapshotSendSideEffects(t, ctx, s, dsn, sender.ID, sender.ID, target.ID, source.ID)
			updates := listenForUpdates(t, ctx, dsn)

			retry := makeSendRequest(form, peer, sender.ID, source.ID, sourceMessageID, randomID, fileID, scheduleZero, time.Now())
			result, hasReplyUpdate, hasSuccessHook, commitCalls, err := api.SendAfterReplyForTest(
				s, sender.ID, retry, blobs, store.RateLimitConfig{Limit: 1, Window: time.Hour},
			)
			assertRPC(t, err, 400, "INPUT_REQUEST_INVALID")
			if result != nil || hasReplyUpdate || hasSuccessHook || commitCalls != 0 {
				t.Errorf("retry returned result=%T reply=%v success=%v commits=%d", result, hasReplyUpdate, hasSuccessHook, commitCalls)
			}
			after := snapshotSendSideEffects(t, ctx, s, dsn, sender.ID, sender.ID, target.ID, source.ID)
			assertSameSendSideEffects(t, before, after)
			if _, ok, err := s.MessageByRandomID(ctx, sender.ID, randomID); err != nil || !ok {
				t.Errorf("original random_id disappeared: found=%v err=%v", ok, err)
			}
			assertNoUpdateNotification(t, updates)
		})
	}
}

func sendWithLimits(
	t *testing.T,
	s *store.Store,
	userID int64,
	form sendForm,
	blobs blob.Store,
	rateLimit store.RateLimitConfig,
	req bin.Encoder,
) (bin.Encoder, error) {
	t.Helper()
	switch req := req.(type) {
	case *tg.MessagesSendMessageRequest:
		return api.SendMessageForTestWithLimits(s, userID, rateLimit, req)
	case *tg.MessagesSendMediaRequest:
		return api.SendMediaForTestWithLimits(s, userID, blobs, api.TestMaxUserStorageBytes, rateLimit, req)
	case *tg.MessagesForwardMessagesRequest:
		return api.ForwardMessagesForTestWithLimits(s, userID, rateLimit, req)
	default:
		return nil, fmt.Errorf("unsupported send request type %T for %s", req, form)
	}
}

var _ unsupportedSendOptionSetter = (*tg.MessagesSendMessageRequest)(nil)
var _ unsupportedSendOptionSetter = (*tg.MessagesSendMediaRequest)(nil)
var _ unsupportedSendOptionSetter = (*tg.MessagesForwardMessagesRequest)(nil)
