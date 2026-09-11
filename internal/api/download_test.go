package api_test

import (
	"context"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// downloadPayload is the body every fixture in this file uploads.
const downloadPayload = "hello world"

// downloadFixture has user a send downloadPayload as a document to user b,
// leaving a stored file plus a live message row on both sides.
func downloadFixture(t *testing.T, phoneA, phoneB string) (
	*store.Store, blob.Store, store.User, store.User, *tg.Document,
) {
	t.Helper()
	s := openStore(t)
	blobs := newBlobs(t)
	return downloadFixtureOn(t, s, blobs, phoneA, phoneB)
}

func downloadFixtureWithDSN(t *testing.T, phoneA, phoneB string) (
	*store.Store, string, blob.Store, store.User, store.User, *tg.Document,
) {
	t.Helper()
	s, dsn := openStoreDSN(t)
	blobs := newBlobs(t)
	_, _, a, b, doc := downloadFixtureOn(t, s, blobs, phoneA, phoneB)
	return s, dsn, blobs, a, b, doc
}

func downloadFixtureOn(t *testing.T, s *store.Store, blobs blob.Store, phoneA, phoneB string) (
	*store.Store, blob.Store, store.User, store.User, *tg.Document,
) {
	t.Helper()
	ctx := context.Background()
	a, err := s.CreateUser(ctx, phoneA)
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, phoneB)
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 900, []byte(downloadPayload))
	enc, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(900, 1, "hello.txt", "text/plain"),
		RandomID: 900,
	})
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	return s, blobs, a, b, documentOf(t, enc)
}

// getBytes runs one upload.getFile for userID and returns the bytes it served.
func getBytes(t *testing.T, s *store.Store, blobs blob.Store, userID int64, doc *tg.Document, offset int64, limit int) []byte {
	t.Helper()
	enc, err := api.GetFileForTest(s, userID, blobs, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
		Offset:   offset,
		Limit:    limit,
	})
	if err != nil {
		t.Fatalf("get file offset=%d limit=%d: %v", offset, limit, err)
	}
	assertEncodes(t, enc)
	f, ok := enc.(*tg.UploadFile)
	if !ok {
		t.Fatalf("result type = %T, want *tg.UploadFile", enc)
	}
	if _, ok = f.Type.(*tg.StorageFileUnknown); !ok {
		t.Errorf("Type = %T, want *tg.StorageFileUnknown", f.Type)
	}
	return f.Bytes
}

type countingDownloadBlobStore struct {
	blob.Store

	reads atomic.Int64
}

func (b *countingDownloadBlobStore) ReadAt(ctx context.Context, key string, offset, limit int64) ([]byte, error) {
	b.reads.Add(1)
	return b.Store.ReadAt(ctx, key, offset, limit)
}

func assertFixedRateLimitLog(t *testing.T, h *captureHandler, message, sentinel string) {
	t.Helper()
	if len(h.records) != 1 {
		t.Fatalf("captured %d records, want one fixed rate-limit record", len(h.records))
	}
	record := h.records[0]
	if record.Message != message {
		t.Errorf("message = %q, want %q", record.Message, message)
	}
	if record.NumAttrs() != 0 {
		t.Errorf("record has %d attrs, want no dynamic attrs", record.NumAttrs())
	}
	if strings.Contains(record.Message, sentinel) {
		t.Errorf("record message contains sentinel %q", sentinel)
	}
}

func TestGetFileRateLimitReserveErrorDoesNotLogDynamicDetails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn, blobs, a, _, doc := downloadFixtureWithDSN(t, "+15551297081", "+15551297082")
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	const sentinel = "get_file_reserve_error_sentinel"
	if _, err := conn.Exec(ctx, `
		ALTER TABLE rate_limits
		ADD CONSTRAINT get_file_reserve_error_sentinel CHECK (token_count < 0)
	`); err != nil {
		t.Fatalf("install reserve failure: %v", err)
	}

	logs := &captureHandler{}
	getFile := api.GetFileSeqForTestWithLimitsAndLogger(
		s, blobs, slog.New(logs),
		store.RateLimitConfig{Limit: 1, Window: time.Second},
		store.RateLimitConfig{},
	)
	_, err = getFile(a.ID, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
		Limit:    64,
	})
	if msg := rpcMessage(t, err); msg != "INTERNAL" {
		t.Fatalf("reserve failure = %s, want INTERNAL", msg)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("returned error contains sentinel %q: %v", sentinel, err)
	}
	assertFixedRateLimitLog(t, logs, "get file rate limit", sentinel)
}

func TestGetFileRateLimitRefundErrorDoesNotLogDynamicDetails(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn, blobs, a, _, doc := downloadFixtureWithDSN(t, "+15551297091", "+15551297092")
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }() //nolint:errcheck // best-effort close

	logs := &captureHandler{}
	getFile := api.GetFileSeqForTestWithLimitsAndLogger(
		s, blobs, slog.New(logs),
		store.RateLimitConfig{Limit: 2, Window: time.Second},
		store.RateLimitConfig{Limit: 1, Window: time.Second},
	)
	request := func() error {
		_, err := getFile(a.ID, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
			Limit:    64,
		})
		return err
	}
	if err := request(); err != nil {
		t.Fatalf("first getFile: %v", err)
	}

	if _, err := conn.Exec(ctx,
		`DELETE FROM rate_limits WHERE subject_id = $1 AND surface = 'upload_get_file'`,
		a.ID,
	); err != nil {
		t.Fatalf("clear account rate limit: %v", err)
	}

	const sentinel = "get_file_refund_error_sentinel"
	if _, err := conn.Exec(ctx, `
		ALTER TABLE rate_limits
		ADD CONSTRAINT get_file_refund_error_sentinel CHECK (token_count > 0) NOT VALID
	`); err != nil {
		t.Fatalf("install refund failure: %v", err)
	}
	err = request()
	if msg := rpcMessage(t, err); msg != "INTERNAL" {
		t.Fatalf("refund failure = %s, want INTERNAL", msg)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("returned error contains sentinel %q: %v", sentinel, err)
	}
	assertFixedRateLimitLog(t, logs, "get file rate limit refund", sentinel)
}

func TestGetFilePerAccountRateLimitAndReset(t *testing.T) {
	t.Parallel()
	s, dsn, blobs, a, _, doc := downloadFixtureWithDSN(t, "+15551297051", "+15551297052")
	getFile := api.GetFileSeqForTestWithLimits(
		s, blobs,
		store.RateLimitConfig{Limit: 2, Window: time.Second},
		store.RateLimitConfig{},
	)
	request := func() error {
		_, err := getFile(a.ID, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
			Limit:    64,
		})
		return err
	}

	for range 2 {
		if err := request(); err != nil {
			t.Fatalf("allowed getFile: %v", err)
		}
	}
	if msg := rpcMessage(t, request()); msg != "FLOOD_WAIT_1" {
		t.Fatalf("over-limit getFile = %s, want FLOOD_WAIT_1", msg)
	}
	if err := api.AgeRateLimitWindowForTest(dsn, a.ID, "upload_get_file", time.Second+time.Millisecond); err != nil {
		t.Fatalf("age getFile window: %v", err)
	}
	if err := request(); err != nil {
		t.Fatalf("getFile after reset: %v", err)
	}
}

func TestGetFileReplicaRateLimitIsSharedAcrossAccounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	blobs := &countingDownloadBlobStore{Store: newBlobs(t)}
	users := make([]store.User, 4)
	for i := range users {
		u, err := s.CreateUser(ctx, "+1555129706"+string(rune('0'+i)))
		if err != nil {
			t.Fatalf("user %d: %v", i, err)
		}
		users[i] = u
	}
	file, err := s.AllocateFile(ctx, users[0].ID, 7, "text/plain", "shared.txt", api.TestMaxUserStorageBytes)
	if err != nil {
		t.Fatalf("allocate file: %v", err)
	}
	if err := s.MarkFileStored(ctx, file.ID); err != nil {
		t.Fatalf("mark file stored: %v", err)
	}
	if _, err := blobs.Put(ctx, blob.Key(file.ID), strings.NewReader("payload")); err != nil {
		t.Fatalf("put blob: %v", err)
	}
	for i := 1; i < len(users); i++ {
		if _, _, _, _, err := s.SendMessage(ctx, users[0].ID, users[i].ID, "shared", int64(i), file.ID, 0); err != nil {
			t.Fatalf("entitle user %d: %v", i, err)
		}
	}
	getFile := api.GetFileSeqForTestWithLimits(
		s, blobs,
		store.RateLimitConfig{},
		store.RateLimitConfig{Limit: 3, Window: time.Second},
	)
	request := func(userID int64) error {
		_, err := getFile(userID, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: file.ID, AccessHash: file.AccessHash},
			Limit:    64,
		})
		return err
	}

	for i := range 3 {
		if err := request(users[i].ID); err != nil {
			t.Fatalf("account %d getFile: %v", i, err)
		}
	}
	if got := blobs.reads.Load(); got != 3 {
		t.Fatalf("blob reads before denial = %d, want 3", got)
	}
	if msg := rpcMessage(t, request(users[3].ID)); msg != "FLOOD_WAIT_1" {
		t.Fatalf("replica over-limit getFile = %s, want FLOOD_WAIT_1", msg)
	}
	if got := blobs.reads.Load(); got != 3 {
		t.Fatalf("blob reads after denial = %d, want 3", got)
	}

	stranger, err := s.CreateUser(ctx, "+15551297069")
	if err != nil {
		t.Fatalf("stranger: %v", err)
	}
	if msg := rpcMessage(t, request(stranger.ID)); msg != "LOCATION_INVALID" {
		t.Fatalf("unauthorized getFile while limited = %s, want LOCATION_INVALID", msg)
	}
	if got := blobs.reads.Load(); got != 3 {
		t.Fatalf("blob reads after unauthorized request = %d, want 3", got)
	}
}

func TestGetFileRateLimitsCanBeDisabledIndependently(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		perAccount store.RateLimitConfig
		perReplica store.RateLimitConfig
		phoneA     string
		phoneB     string
	}{
		"per-account disabled": {
			perAccount: store.RateLimitConfig{},
			perReplica: store.RateLimitConfig{Limit: 2, Window: time.Second},
			phoneA:     "+15551297061",
			phoneB:     "+15551297062",
		},
		"replica disabled": {
			perAccount: store.RateLimitConfig{Limit: 2, Window: time.Second},
			perReplica: store.RateLimitConfig{},
			phoneA:     "+15551297071",
			phoneB:     "+15551297072",
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, blobs, a, _, doc := downloadFixture(t, tc.phoneA, tc.phoneB)
			getFile := api.GetFileSeqForTestWithLimits(s, blobs, tc.perAccount, tc.perReplica)
			request := func() error {
				_, err := getFile(a.ID, &tg.UploadGetFileRequest{
					Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
					Limit:    64,
				})
				return err
			}
			for range 2 {
				if err := request(); err != nil {
					t.Fatalf("allowed getFile: %v", err)
				}
			}
			if msg := rpcMessage(t, request()); msg != "FLOOD_WAIT_1" {
				t.Fatalf("over-limit getFile = %s, want FLOOD_WAIT_1", msg)
			}
		})
	}
}

func TestGetFileRanges(t *testing.T) {
	t.Parallel()
	s, blobs, a, b, doc := downloadFixture(t, "+15551297001", "+15551297002")

	// Both the uploader and the recipient read the whole file: the gate is
	// ownership of a message row, not ownership of the upload.
	for name, uid := range map[string]int64{"uploader": a.ID, "recipient": b.ID} {
		if got := string(getBytes(t, s, blobs, uid, doc, 0, 64)); got != downloadPayload {
			t.Errorf("%s full read = %q, want %q", name, got, downloadPayload)
		}
	}

	if got := string(getBytes(t, s, blobs, a.ID, doc, 6, 5)); got != "world" {
		t.Errorf("ranged read = %q, want %q", got, "world")
	}
	// The last window of a file is short by definition, not an error.
	if got := string(getBytes(t, s, blobs, a.ID, doc, 8, api.MaxDownloadChunk)); got != "rld" {
		t.Errorf("short final window = %q, want %q", got, "rld")
	}
	// A client that has read to the end asks once more and gets an empty reply.
	if got := getBytes(t, s, blobs, a.ID, doc, int64(len(downloadPayload)), 1024); len(got) != 0 {
		t.Errorf("read at EOF = %q, want empty", got)
	}
}

func TestGetFileRejectsBadWindow(t *testing.T) {
	t.Parallel()
	s, blobs, a, _, doc := downloadFixture(t, "+15551297011", "+15551297012")

	windows := map[string]struct {
		offset int64
		limit  int
	}{
		"past end":      {int64(len(downloadPayload)) + 1, 1024},
		"zero limit":    {0, 0},
		"limit too big": {0, api.MaxDownloadChunk + 1},
		"negative":      {-1, 1024},
	}
	for name, w := range windows {
		_, err := api.GetFileForTest(s, a.ID, blobs, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
			Offset:   w.offset,
			Limit:    w.limit,
		})
		if msg := rpcMessage(t, err); msg != "LOCATION_INVALID" {
			t.Errorf("%s: got %s, want LOCATION_INVALID", name, msg)
		}
	}
}

// TestGetFileGate pins that every way of not being entitled to a file returns
// the identical error, so the download path is not an enumeration oracle.
func TestGetFileGate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, blobs, a, b, doc := downloadFixture(t, "+15551297021", "+15551297022")
	c, err := s.CreateUser(ctx, "+15551297023")
	if err != nil {
		t.Fatalf("user c: %v", err)
	}

	type rejection struct {
		userID int64
		loc    *tg.InputDocumentFileLocation
	}
	rejections := map[string]rejection{
		"stranger":    {c.ID, &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash}},
		"wrong hash":  {a.ID, &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash + 1}},
		"absent id":   {a.ID, &tg.InputDocumentFileLocation{ID: doc.ID + 1000, AccessHash: doc.AccessHash}},
		"thumb size":  {a.ID, &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash, ThumbSize: "m"}},
		"never given": {c.ID, &tg.InputDocumentFileLocation{ID: doc.ID + 1000, AccessHash: doc.AccessHash + 1}},
	}

	// A file row that exists but whose bytes were never stored: allocated and
	// never assembled, so it fails the gate's stored predicate.
	unstored, err := s.AllocateFile(ctx, a.ID, 11, "text/plain", "never.txt", api.TestMaxUserStorageBytes)
	if err != nil {
		t.Fatalf("allocate file: %v", err)
	}
	rejections["not stored"] = rejection{
		a.ID, &tg.InputDocumentFileLocation{ID: unstored.ID, AccessHash: unstored.AccessHash},
	}

	for name, r := range rejections {
		_, err = api.GetFileForTest(s, r.userID, blobs, &tg.UploadGetFileRequest{
			Location: r.loc, Offset: 0, Limit: 1024,
		})
		if msg := rpcMessage(t, err); msg != "LOCATION_INVALID" {
			t.Errorf("%s: got %s, want LOCATION_INVALID", name, msg)
		}
	}

	// A location type M5 does not serve is the same error again.
	_, err = api.GetFileForTest(s, a.ID, blobs, &tg.UploadGetFileRequest{
		Location: &tg.InputPhotoFileLocation{ID: doc.ID, AccessHash: doc.AccessHash, ThumbSize: "m"},
		Offset:   0,
		Limit:    1024,
	})
	if msg := rpcMessage(t, err); msg != "LOCATION_INVALID" {
		t.Errorf("photo location: got %s, want LOCATION_INVALID", msg)
	}

	// A garbage file reference still succeeds: it is ignored, not half-checked.
	enc, err := api.GetFileForTest(s, b.ID, blobs, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{
			ID: doc.ID, AccessHash: doc.AccessHash, FileReference: []byte("garbage"),
		},
		Offset: 0,
		Limit:  1024,
	})
	if err != nil {
		t.Fatalf("garbage file reference: %v", err)
	}
	assertEncodes(t, enc)
}

// TestGetFileDeletedMessageRevokes pins that a one-sided delete (revoke=false,
// the client default) revokes retrieval only for the deleter: the sender keeps
// the message and the file, while the deleter gets LOCATION_INVALID. The
// revoke=true companion case below asserts both sides are revoked.
func TestGetFileDeletedMessageRevokes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, blobs, a, b, doc := downloadFixture(t, "+15551297031", "+15551297032")

	// b's own local id for the message, which is what b deletes by.
	hist, err := api.GetHistoryForTest(s, b.ID, &tg.MessagesGetHistoryRequest{
		Peer: api.InputPeerUser(b.ID, a.ID),
	})
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	msgs, ok := hist.(*tg.MessagesMessages)
	if !ok || len(msgs.Messages) != 1 {
		t.Fatalf("history = %#v, want one message", hist)
	}
	inbox, ok := msgs.Messages[0].(*tg.Message)
	if !ok {
		t.Fatalf("history message type = %T", msgs.Messages[0])
	}

	// Both sides can read it before the delete.
	for _, uid := range []int64{a.ID, b.ID} {
		if got := string(getBytes(t, s, blobs, uid, doc, 0, 64)); got != downloadPayload {
			t.Fatalf("pre-delete read by %d = %q", uid, got)
		}
	}

	// b deletes her copy one-sidedly: only her retrieval is revoked.
	if _, err = s.DeleteMessages(ctx, b.ID, []int64{int64(inbox.ID)}, false); err != nil {
		t.Fatalf("delete: %v", err)
	}

	if got := string(getBytes(t, s, blobs, a.ID, doc, 0, 64)); got != downloadPayload {
		t.Fatalf("sender read after one-sided delete = %q, want %q", got, downloadPayload)
	}
	_, err = api.GetFileForTest(s, b.ID, blobs, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
		Offset:   0,
		Limit:    1024,
	})
	if msg := rpcMessage(t, err); msg != "LOCATION_INVALID" {
		t.Errorf("deleter read after one-sided delete: got %s, want LOCATION_INVALID", msg)
	}

	// revoke=true deletes both rows and revokes both accounts.
	hist2, err := api.GetHistoryForTest(s, a.ID, &tg.MessagesGetHistoryRequest{
		Peer: api.InputPeerUser(a.ID, b.ID),
	})
	if err != nil {
		t.Fatalf("get history a: %v", err)
	}
	outbox, ok := hist2.(*tg.MessagesMessages)
	if !ok || len(outbox.Messages) != 1 {
		t.Fatalf("history a = %#v, want one message", hist2)
	}
	out, ok := outbox.Messages[0].(*tg.Message)
	if !ok {
		t.Fatalf("history a message type = %T", outbox.Messages[0])
	}
	if _, err = s.DeleteMessages(ctx, a.ID, []int64{int64(out.ID)}, true); err != nil {
		t.Fatalf("revoke delete: %v", err)
	}
	for _, uid := range []int64{a.ID, b.ID} {
		_, err = api.GetFileForTest(s, uid, blobs, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
			Offset:   0,
			Limit:    1024,
		})
		if msg := rpcMessage(t, err); msg != "LOCATION_INVALID" {
			t.Errorf("post-revoke read by %d: got %s, want LOCATION_INVALID", uid, msg)
		}
	}
}

// TestGetFileReleasesSlotOnError drives the release through handleGetFile
// rather than through the slot methods, which is the only way to catch a
// dropped `defer h.endDownload`. That failure is permanent: a leaked slot locks
// the account out of downloading for the life of the process.
func TestGetFileReleasesSlotOnError(t *testing.T) {
	t.Parallel()
	s, blobs, a, _, doc := downloadFixture(t, "+15551297041", "+15551297042")
	// Both calls go through one handler, so they share the slot map.
	getFile := api.GetFileSeqForTest(s, blobs)

	// Fails after the claim: the window check that rejects an offset past the
	// end runs against the loaded file, so the slot has already been taken.
	_, err := getFile(a.ID, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
		Offset:   int64(len(downloadPayload)) + 1,
		Limit:    1024,
	})
	if msg := rpcMessage(t, err); msg != "LOCATION_INVALID" {
		t.Fatalf("got %s, want LOCATION_INVALID", msg)
	}

	enc, err := getFile(a.ID, &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
		Offset:   0,
		Limit:    64,
	})
	if err != nil {
		t.Fatalf("read after failed download: %v", err)
	}
	f, ok := enc.(*tg.UploadFile)
	if !ok {
		t.Fatalf("result type = %T, want *tg.UploadFile", enc)
	}
	if string(f.Bytes) != downloadPayload {
		t.Errorf("read after failed download = %q, want %q", f.Bytes, downloadPayload)
	}
}

// TestDownloadSlot pins one download in flight per account, and that releasing
// one account's slot leaves another's alone.
func TestDownloadSlot(t *testing.T) {
	t.Parallel()
	begin, end := api.DownloadSlotForTest()

	if !begin(1) {
		t.Fatal("first claim rejected")
	}
	if begin(1) {
		t.Error("second concurrent claim admitted")
	}
	if !begin(2) {
		t.Error("another account blocked by the first")
	}
	end(1)
	if !begin(1) {
		t.Error("claim after release rejected")
	}
}

func TestGetFileUnauthorized(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.GetFileForTest(s, 0, newBlobs(t), &tg.UploadGetFileRequest{
		Location: &tg.InputDocumentFileLocation{ID: 1, AccessHash: 1},
		Offset:   0,
		Limit:    1024,
	})
	if msg := rpcMessage(t, err); msg != "AUTH_KEY_UNREGISTERED" {
		t.Fatalf("got %s, want AUTH_KEY_UNREGISTERED", msg)
	}
}
