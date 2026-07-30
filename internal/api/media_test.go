package api_test

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSanitizeMIME(t *testing.T) {
	t.Parallel()
	const fallback = "application/octet-stream"
	tests := map[string]string{
		"image/png":                 "image/png",
		"text/plain; charset=utf-8": fallback,
		"image/png\r\nX: y":         fallback,
		"":                          fallback,
		"png":                       fallback,
		"a/b/c":                     fallback,
		"image/":                    fallback,
		"/png":                      fallback,
		strings.Repeat("a", 128) + "/" + strings.Repeat("b", 127): fallback,
	}
	for in, want := range tests {
		if got := api.SanitizeMIME(in); got != want {
			t.Errorf("SanitizeMIME(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSanitizeFileName(t *testing.T) {
	t.Parallel()
	tests := map[string]string{
		"hello.txt":              "hello.txt",
		"réunion notes.pdf":      "réunion notes.pdf",
		"nul\x00.txt":            "",
		"two\nlines.txt":         "",
		"bell\x07.txt":           "",
		"c1\u009f.txt":           "",
		"annexe\u202egnp.exe":    "",
		"isolate\u2066.txt":      "",
		strings.Repeat("a", 256): "",
	}
	for in, want := range tests {
		if got := api.SanitizeFileName(in); got != want {
			t.Errorf("SanitizeFileName(%q) = %q, want %q", in, got, want)
		}
	}
}

// saveParts uploads each payload as one part of fileID under userID.
func saveParts(t *testing.T, s *store.Store, userID, fileID int64, payloads ...[]byte) {
	t.Helper()
	for i, p := range payloads {
		if _, err := api.SaveFilePartForTest(s, userID, &tg.UploadSaveFilePartRequest{
			FileID: fileID, FilePart: i, Bytes: p,
		}); err != nil {
			t.Fatalf("save part %d: %v", i, err)
		}
	}
}

// uploadedDocument builds the one media type sendMedia accepts.
func uploadedDocument(fileID int64, parts int, name, mime string) *tg.InputMediaUploadedDocument {
	return &tg.InputMediaUploadedDocument{
		File:     &tg.InputFile{ID: fileID, Parts: parts, Name: name},
		MimeType: mime,
	}
}

// newBlobs opens a blob store rooted in the test's own temporary directory.
func newBlobs(t *testing.T) blob.Store {
	t.Helper()
	b, err := blob.NewLocal(t.TempDir())
	if err != nil {
		t.Fatalf("blob store: %v", err)
	}
	return b
}

// documentOf returns the document the first updateNewMessage in enc carries.
func documentOf(t *testing.T, enc any) *tg.Document {
	t.Helper()
	ups, ok := enc.(*tg.Updates)
	if !ok {
		t.Fatalf("result type = %T, want *tg.Updates", enc)
	}
	for _, u := range ups.Updates {
		nm, ok := u.(*tg.UpdateNewMessage)
		if !ok {
			continue
		}
		m, ok := nm.Message.(*tg.Message)
		if !ok {
			t.Fatalf("message type = %T, want *tg.Message", nm.Message)
		}
		media, ok := m.Media.(*tg.MessageMediaDocument)
		if !ok {
			t.Fatalf("media type = %T, want *tg.MessageMediaDocument", m.Media)
		}
		doc, ok := media.Document.(*tg.Document)
		if !ok {
			t.Fatalf("document type = %T, want *tg.Document", media.Document)
		}
		return doc
	}
	t.Fatal("no updateNewMessage in reply")
	return nil
}

func TestPartsReaderConcatenates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551296001")
	if err != nil {
		t.Fatalf("user: %v", err)
	}
	saveParts(t, s, u.ID, 700, []byte("hello "), []byte("wide "), []byte("world"))

	got, err := io.ReadAll(api.NewPartsReaderForTest(s, u.ID, 700, 3))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello wide world" {
		t.Errorf("read = %q", got)
	}

	// Zero parts is end of stream, not an error and not a spin.
	empty, err := io.ReadAll(api.NewPartsReaderForTest(s, u.ID, 701, 0))
	if err != nil || len(empty) != 0 {
		t.Errorf("empty read = %q err=%v", empty, err)
	}
}

func TestSendMediaToUser(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	blobs := newBlobs(t)
	a, err := s.CreateUser(ctx, "+15551296011")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296012")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 555, []byte("hello "), []byte("world"))

	enc, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(555, 2, "hello.txt", "text/plain"),
		Message:  "look",
		RandomID: 42,
	})
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	assertEncodes(t, enc)
	doc := documentOf(t, enc)
	if doc.Size != 11 {
		t.Errorf("Size = %d, want 11", doc.Size)
	}
	if doc.MimeType != "text/plain" {
		t.Errorf("MimeType = %q", doc.MimeType)
	}
	name, ok := doc.Attributes[0].(*tg.DocumentAttributeFilename)
	if !ok || name.FileName != "hello.txt" {
		t.Errorf("attributes = %#v", doc.Attributes)
	}

	// The recipient reads the same document off their own history.
	hist, err := api.GetHistoryForTest(s, b.ID, &tg.MessagesGetHistoryRequest{
		Peer: api.InputPeerUser(b.ID, a.ID),
	})
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	msgs, ok := hist.(*tg.MessagesMessages)
	if !ok || len(msgs.Messages) != 1 {
		t.Fatalf("history = %#v", hist)
	}
	inbox, ok := msgs.Messages[0].(*tg.Message)
	if !ok {
		t.Fatalf("history message type = %T", msgs.Messages[0])
	}
	media, ok := inbox.Media.(*tg.MessageMediaDocument)
	if !ok {
		t.Fatalf("history media type = %T", inbox.Media)
	}
	inboxDoc, ok := media.Document.(*tg.Document)
	if !ok || inboxDoc.ID != doc.ID {
		t.Fatalf("history document = %#v", media.Document)
	}

	// The bytes are the ones that were uploaded, at the file id's key.
	body, err := blobs.ReadAt(ctx, blob.Key(doc.ID), 0, 64)
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if !bytes.Equal(body, []byte("hello world")) {
		t.Errorf("blob = %q, want %q", body, "hello world")
	}

	// The parts are consumed: assembly is not repeatable off the same upload.
	n, _, _, err := s.UploadPartsSummary(ctx, a.ID, 555)
	if err != nil {
		t.Fatalf("parts summary: %v", err)
	}
	if n != 0 {
		t.Errorf("parts left = %d, want 0", n)
	}
}

func TestSendMediaSanitizesStoredMIME(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551296091")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296092")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 564, []byte("plain"))

	// The wiring, not the pure function: a mime type with a space is stored as
	// the generic type, and the reply echoes what was stored.
	enc, err := api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(564, 1, "two\nlines.txt", "text/plain; charset=utf-8"),
		RandomID: 50,
	})
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	doc := documentOf(t, enc)
	if doc.MimeType != "application/octet-stream" {
		t.Errorf("MimeType = %q, want application/octet-stream", doc.MimeType)
	}
	// The file name is unusable, so the document carries no name attribute.
	if len(doc.Attributes) != 0 {
		t.Errorf("attributes = %#v, want none", doc.Attributes)
	}
}

func TestSendMediaResendIsIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	a, err := s.CreateUser(ctx, "+15551296101")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296102")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 565, []byte("once"))

	req := &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(565, 1, "once.txt", "text/plain"),
		Message:  "look",
		RandomID: 51,
	}
	first, err := api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, req)
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	firstDoc := documentOf(t, first)

	// A client that lost the reply resends. Assembly already consumed the
	// parts, so a resend that reached it would report MEDIA_INVALID for a
	// message that was delivered.
	second, err := api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, req)
	if err != nil {
		t.Fatalf("resend: %v", err)
	}
	secondDoc := documentOf(t, second)
	if secondDoc.ID != firstDoc.ID {
		t.Errorf("resend document = %d, want %d", secondDoc.ID, firstDoc.ID)
	}
	// No second message and no second file: the resend cost nothing.
	if n := countFiles(t, ctx, dsn); n != 1 {
		t.Errorf("files rows = %d, want 1", n)
	}
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
}

func TestSendMediaToChat(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	blobs := newBlobs(t)
	users, chat := chatWith(t, s, "+15551296021", "+15551296022", "+15551296023")
	saveParts(t, s, users[0].ID, 556, []byte("chat bytes"))

	enc, err := api.SendMediaForTest(s, users[0].ID, blobs, api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     &tg.InputPeerChat{ChatID: chat.ID},
		Media:    uploadedDocument(556, 1, "notes.txt", "text/plain"),
		Message:  "for everyone",
		RandomID: 43,
	})
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	assertEncodes(t, enc)
	doc := documentOf(t, enc)

	// Every member's own copy carries the document.
	for _, u := range users[1:] {
		hist, herr := api.GetHistoryForTest(s, u.ID, &tg.MessagesGetHistoryRequest{
			Peer: &tg.InputPeerChat{ChatID: chat.ID},
		})
		if herr != nil {
			t.Fatalf("get history %d: %v", u.ID, herr)
		}
		msgs, ok := hist.(*tg.MessagesMessages)
		if !ok || len(msgs.Messages) == 0 {
			t.Fatalf("history %d = %#v", u.ID, hist)
		}
		m, ok := msgs.Messages[0].(*tg.Message)
		if !ok {
			t.Fatalf("history %d message type = %T", u.ID, msgs.Messages[0])
		}
		media, ok := m.Media.(*tg.MessageMediaDocument)
		if !ok {
			t.Fatalf("history %d media type = %T", u.ID, m.Media)
		}
		got, ok := media.Document.(*tg.Document)
		if !ok || got.ID != doc.ID {
			t.Fatalf("history %d document = %#v", u.ID, media.Document)
		}
	}
}

func TestSendMediaRejectsPhoto(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551296031")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296032")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 557, []byte("not really a png"))

	_, err = api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer: api.InputPeerUser(a.ID, b.ID),
		Media: &tg.InputMediaUploadedPhoto{
			File: &tg.InputFile{ID: 557, Parts: 1, Name: "photo.png"},
		},
		RandomID: 44,
	})
	rpcError(t, err, "MEDIA_INVALID")
}

func TestSendMediaRejectsThumb(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551296041")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296042")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 558, []byte("body"))

	media := uploadedDocument(558, 1, "clip.bin", "application/octet-stream")
	media.SetThumb(&tg.InputFile{ID: 559, Parts: 1, Name: "thumb.jpg"})
	_, err = api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    media,
		RandomID: 45,
	})
	rpcError(t, err, "MEDIA_INVALID")
}

func TestSendMediaRejectsPartCountMismatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	a, err := s.CreateUser(ctx, "+15551296051")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296052")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 560, []byte("one"), []byte("two"))

	_, err = api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(560, 3, "short.txt", "text/plain"),
		RandomID: 46,
	})
	rpcError(t, err, "MEDIA_INVALID")
	// The row is allocated after the check, so a rejected assembly must not
	// have consumed any of the account's quota.
	if n := countFiles(t, ctx, dsn); n != 0 {
		t.Errorf("files rows = %d, want 0", n)
	}
}

func TestSendMediaRejectsAnotherAccountsUpload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	a, err := s.CreateUser(ctx, "+15551296061")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296062")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	// b uploads; a names b's file id. The parts are looked up under the
	// caller's own user id, so a's assembly finds nothing.
	saveParts(t, s, b.ID, 561, []byte("b's bytes"))

	_, err = api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(561, 1, "stolen.txt", "text/plain"),
		RandomID: 47,
	})
	rpcError(t, err, "MEDIA_INVALID")
	if n := countFiles(t, ctx, dsn); n != 0 {
		t.Errorf("files rows = %d, want 0", n)
	}
}

func TestSendMediaToChatRequiresMembership(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, dsn := openStoreDSN(t)
	_, chat := chatWith(t, s, "+15551296071", "+15551296072")
	outsider, err := s.CreateUser(ctx, "+15551296073")
	if err != nil {
		t.Fatalf("outsider: %v", err)
	}
	saveParts(t, s, outsider.ID, 562, []byte("intrusion"))

	_, err = api.SendMediaForTest(s, outsider.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     &tg.InputPeerChat{ChatID: chat.ID},
		Media:    uploadedDocument(562, 1, "intrusion.txt", "text/plain"),
		RandomID: 48,
	})
	rpcError(t, err, "PEER_ID_INVALID")
	// Membership is checked before assembly, so no bytes and no row.
	if n := countFiles(t, ctx, dsn); n != 0 {
		t.Errorf("files rows = %d, want 0", n)
	}
}

func TestSendMediaEnforcesStorageQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551296081")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296082")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 563, []byte("eleven byte"))

	_, err = api.SendMediaForTest(s, a.ID, newBlobs(t), 4, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(563, 1, "big.txt", "text/plain"),
		RandomID: 49,
	})
	rpcError(t, err, "STORAGE_CHECK_FAILED")
}

// countFiles reports how many files rows exist, for the rejection paths that
// must not allocate one.
func countFiles(t *testing.T, ctx context.Context, dsn string) int64 {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() {
		if cerr := conn.Close(ctx); cerr != nil {
			t.Errorf("close conn: %v", cerr)
		}
	}()
	var n int64
	if err := conn.QueryRow(ctx, "SELECT count(*) FROM files").Scan(&n); err != nil {
		t.Fatalf("count files: %v", err)
	}
	return n
}
