package api_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/jackc/pgx/v5"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/pgtest"
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

// saveParts uploads each payload as one part of fileID under userID, through
// the handler that testHandlers builds for this store's blob backend, so the
// parts land where assembly will look for them.
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

// messageOf returns the message the first updateNewMessage in enc carries,
// for a test that needs the local id the send allocated.
func messageOf(t *testing.T, enc any) *tg.Message {
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
		return m
	}
	t.Fatal("no updateNewMessage in the result")
	return nil
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

	r, err := api.NewPartsReaderForTest(s, u.ID, 700)
	if err != nil {
		t.Fatalf("parts reader: %v", err)
	}
	sized, ok := r.(interface{ Size() int64 })
	if !ok {
		t.Fatal("parts reader does not expose its known size")
	}
	if sized.Size() != int64(len("hello wide world")) {
		t.Fatalf("parts reader size = %d, want %d", sized.Size(), len("hello wide world"))
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got) != "hello wide world" {
		t.Errorf("read = %q", got)
	}

	// Zero parts is end of stream, not an error and not a spin.
	r, err = api.NewPartsReaderForTest(s, u.ID, 701)
	if err != nil {
		t.Fatalf("empty parts reader: %v", err)
	}
	empty, err := io.ReadAll(r)
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
	// The part objects go with the rows: nothing of the upload is left in the
	// store.
	if got := partObjects(t, blobs); len(got) != 0 {
		t.Errorf("part objects left = %d, want 0", len(got))
	}
}

// partObjects lists the keys under the parts prefix that blobs still holds.
func partObjects(t *testing.T, blobs blob.Store) []string {
	t.Helper()
	dir, ok := blobLocalDir(t, blobs)
	if !ok {
		t.Skip("blob backend is not local; object listing is not available")
	}
	entries, err := os.ReadDir(dir + "/parts")
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read parts dir: %v", err)
	}
	keys := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			keys = append(keys, blob.PartsPrefix+e.Name())
		}
	}
	return keys
}

// blobLocalDir reports the root directory of a local blob backend.
func blobLocalDir(t *testing.T, b blob.Store) (string, bool) {
	t.Helper()
	l, ok := b.(*blob.Local)
	if !ok {
		return "", false
	}
	return l.RootDir(), true
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

// A forward whose file row is gone is refused, and reports the invalid message
// id a deleted source already reports. The source message here is still live
// and still names the file, so the handler's own liveness read passes and the
// interlock inside the transaction is the only thing that can refuse.
//
// The send paths reject the same state through the same interlock, asserted in
// the store's own tests: a handler cannot stage it, because handleSendMedia
// answers a resend from its own read before the send transaction opens, and a
// first send assembles the file it then references.
func TestForwardRejectsErasedFile(t *testing.T) {
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
	c, err := s.CreateUser(ctx, "+15551296103")
	if err != nil {
		t.Fatalf("user c: %v", err)
	}
	peerB := api.InputPeerUser(a.ID, b.ID)
	peerC := api.InputPeerUser(a.ID, c.ID)
	saveParts(t, s, a.ID, 565, []byte("eleven byte"))
	if _, err = api.SendMediaForTest(s, a.ID, newBlobs(t), api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer: peerB, Media: uploadedDocument(565, 1, "doc.txt", "text/plain"), RandomID: 51,
	}); err != nil {
		t.Fatalf("seed send: %v", err)
	}
	eraseFiles(t, ctx, dsn)

	_, err = api.ForwardMessagesForTest(s, a.ID, &tg.MessagesForwardMessagesRequest{
		ToPeer: peerC, FromPeer: peerB, ID: []int{1}, RandomID: []int64{52},
	})
	rpcError(t, err, "MESSAGE_ID_INVALID")

	msgs, err := s.History(ctx, c.ID, store.PeerTypeUser, a.ID, 0, 10)
	if err != nil {
		t.Fatalf("history: %v", err)
	}
	if len(msgs) != 0 {
		t.Errorf("destination holds %d messages, want 0", len(msgs))
	}
}

// eraseFiles removes every files row. Nothing in the shipped server deletes one
// — the eraser is a later stage of M17 — so this is the only way a handler test
// can reach the state the reference interlock refuses.
func eraseFiles(t *testing.T, ctx context.Context, dsn string) {
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
	tag, err := conn.Exec(ctx, "DELETE FROM files")
	if err != nil {
		t.Fatalf("erase files: %v", err)
	}
	if tag.RowsAffected() == 0 {
		t.Fatal("erase files removed nothing — the test is asserting against a file that was never stored")
	}
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

// Criterion 12 at the boundary a client actually reaches: a resend naming a
// file the eraser has removed is refused, not replayed.
//
// The store-level test for this drives SendMessage directly, which no client
// does. sendMedia answers a transport retry from the caller's stored message
// before it reaches the interlock, and MessageByRandomID has no deleted
// predicate — so the branch above the interlock is where criterion 12 is
// actually decided, and it has to be tested here.
//
// The leak this closes is a per-file erasure oracle, which is why it is not
// merely a consistency point. The same repeated request answers with the
// document while the file row exists and with a plain message once it is gone,
// so the uploader reads off exactly which of their files was erased — and
// therefore which recipient deleted which media — on demand, with none of the
// blunting the randomized sweep interval was accepted as providing.
func TestSendMediaResendAfterErasureIsRefused(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	blobs := newBlobs(t)
	// One blob store behind both the handler's assembly and the eraser's
	// unlink, the way the process wires them.
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(blobs))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	})

	a, err := s.CreateUser(ctx, "+15551296201")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551296202")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, 7700, []byte("erasable bytes"))

	req := &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(7700, 1, "doc.txt", "text/plain"),
		Message:  "look",
		RandomID: 7701,
	}
	first, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, req)
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	// The control on the oracle: before erasure this same request answers with
	// the document. Without it, an assertion that the resend is refused would
	// pass on a handler that refused every resend.
	doc := documentOf(t, first)
	sent := messageOf(t, first)

	// Both copies deleted, then erased. revoke = true takes the recipient's
	// copy too, through the shipped delete path rather than a direct update.
	if _, err = s.DeleteMessages(ctx, a.ID, []int64{int64(sent.ID)}, true); err != nil {
		t.Fatalf("delete: %v", err)
	}
	counts, err := s.SweepMediaErasure(ctx, time.Now().Add(time.Hour), store.ErasureScanBatch)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if counts.Erased != 1 {
		t.Fatalf("sweep counts = %+v, want one erased — the file under test was not erased", counts)
	}

	// The byte-identical resend a client would send after losing the reply.
	second, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, req)
	if err == nil {
		t.Fatalf("resend after erasure returned %#v, want MEDIA_INVALID: the erased file %d was replayed", second, doc.ID)
	}
	rpcError(t, err, "MEDIA_INVALID")

	// Nothing was written by the refusal: no new message for the recipient and
	// no new file row.
	hist, err := api.GetHistoryForTest(s, b.ID, &tg.MessagesGetHistoryRequest{
		Peer: api.InputPeerUser(b.ID, a.ID),
	})
	if err != nil {
		t.Fatalf("get history: %v", err)
	}
	if msgs, ok := hist.(*tg.MessagesMessages); !ok || len(msgs.Messages) != 0 {
		t.Errorf("recipient history = %#v, want empty after a refused resend", hist)
	}
	if n := countFiles(t, ctx, dsn); n != 0 {
		t.Errorf("files rows = %d, want 0 — the refused resend reassembled the upload", n)
	}
}

// deletedOriginalResend runs one arm of the erasure-oracle probe: send media,
// soft-delete both copies through the shipped delete path, optionally let the
// erasure sweep past, then resend the byte-identical request. It returns what
// that second call answered.
//
// The two arms differ in exactly one thing — whether the file behind the
// deleted original still exists — which is the bit an uploader must not be able
// to read off the reply.
func deletedOriginalResend(t *testing.T, phoneA, phoneB string, fileID, randomID int64, runSweep bool) (any, error) {
	t.Helper()
	ctx := context.Background()
	dsn := pgtest.DSN(t)
	blobs := newBlobs(t)
	s, err := store.Open(ctx, dsn, pgtest.EncKey(), store.WithBlobStore(blobs))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		if cerr := s.Close(); cerr != nil {
			t.Errorf("close: %v", cerr)
		}
	})
	a, err := s.CreateUser(ctx, phoneA)
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, phoneB)
	if err != nil {
		t.Fatalf("user b: %v", err)
	}
	saveParts(t, s, a.ID, fileID, []byte("erasable bytes"))

	req := &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(fileID, 1, "doc.txt", "text/plain"),
		Message:  "look",
		RandomID: randomID,
	}
	first, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, req)
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	// The control on the whole probe: before anything is deleted this request
	// answers with the document. Without it both arms could be refusals for a
	// reason that has nothing to do with the leak.
	documentOf(t, first)
	sent := messageOf(t, first)

	if _, err = s.DeleteMessages(ctx, a.ID, []int64{int64(sent.ID)}, true); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if runSweep {
		counts, serr := s.SweepMediaErasure(ctx, time.Now().Add(time.Hour), store.ErasureScanBatch)
		if serr != nil {
			t.Fatalf("sweep: %v", serr)
		}
		if counts.Erased != 1 {
			t.Fatalf("sweep counts = %+v, want one erased", counts)
		}
		if n := countFiles(t, ctx, dsn); n != 0 {
			t.Fatalf("files rows = %d after the sweep, want 0 — this arm is not the erased one", n)
		}
	} else if n := countFiles(t, ctx, dsn); n != 1 {
		// Without this the two arms could be identical and the comparison below
		// would be comparing one state with itself.
		t.Fatalf("files rows = %d without a sweep, want 1 — this arm is not the surviving one", n)
	}
	return api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, req)
}

// A resend whose original is soft-deleted answers the same way whether or not
// the eraser has taken the file behind it.
//
// Refusing is not the property — indistinguishability is. Two arms that both
// fail but fail differently leave the oracle exactly where it was: the uploader
// still reads off which of their files has been erased, and therefore which
// recipient deleted which media, from one repeated request. So this compares
// the two replies against each other rather than each against a constant.
//
// The surviving-file arm is the one that carried the leak after the first fix.
// The handler stopped short-circuiting on a deleted original, but the
// fall-through reached SendMessage, where the interlock refused only when the
// file row was gone — and when it survived, the store's own dedup replayed the
// deleted original and the handler rendered its document.
func TestSendMediaResendOfDeletedOriginalIsIndistinguishable(t *testing.T) {
	t.Parallel()

	kept, keptErr := deletedOriginalResend(t, "+15551296211", "+15551296212", 7710, 7711, false)
	erased, erasedErr := deletedOriginalResend(t, "+15551296221", "+15551296222", 7720, 7721, true)

	if keptErr == nil {
		t.Fatalf("resend with the file still present returned %#v, want a refusal — the deleted original was replayed", kept)
	}
	if erasedErr == nil {
		t.Fatalf("resend after erasure returned %#v, want a refusal", erased)
	}
	rpcError(t, keptErr, "MEDIA_INVALID")
	rpcError(t, erasedErr, "MEDIA_INVALID")

	// The load-bearing assertion. Anything that distinguishes the two replies —
	// a different code, a different message, one carrying a payload — is the
	// oracle still open.
	if keptErr.Error() != erasedErr.Error() {
		t.Errorf("the arms answer differently: file present -> %q, file erased -> %q; whether the sweep has run is readable from the reply",
			keptErr, erasedErr)
	}
	if kept != nil || erased != nil {
		t.Errorf("a refused resend returned a payload: present -> %#v, erased -> %#v", kept, erased)
	}
}
