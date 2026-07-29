package api_test

import (
	"context"
	"testing"

	"github.com/gotd/td/tg"

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
	ctx := context.Background()
	s := openStore(t)
	blobs := newBlobs(t)
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
		Peer:     &tg.InputPeerUser{UserID: b.ID, AccessHash: b.ID},
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

// TestGetFileDeletedMessageRevokes pins that deleting a media message revokes
// retrieval rather than being cosmetic.
//
// DeleteMessages on a user peer soft-deletes the owner row AND the mirror row
// (internal/store/messages.go:522), so a delete by the recipient revokes the
// file for BOTH accounts, not only the deleter.
func TestGetFileDeletedMessageRevokes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, blobs, a, b, doc := downloadFixture(t, "+15551297031", "+15551297032")

	// b's own local id for the message, which is what b deletes by.
	hist, err := api.GetHistoryForTest(s, b.ID, &tg.MessagesGetHistoryRequest{
		Peer: &tg.InputPeerUser{UserID: a.ID, AccessHash: a.ID},
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

	if _, err = s.DeleteMessages(ctx, b.ID, []int64{int64(inbox.ID)}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	for _, uid := range []int64{a.ID, b.ID} {
		_, err = api.GetFileForTest(s, uid, blobs, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{ID: doc.ID, AccessHash: doc.AccessHash},
			Offset:   0,
			Limit:    1024,
		})
		if msg := rpcMessage(t, err); msg != "LOCATION_INVALID" {
			t.Errorf("post-delete read by %d: got %s, want LOCATION_INVALID", uid, msg)
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
