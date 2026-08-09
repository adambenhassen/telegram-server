package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// mediaPartSize is the protocol part size, used both as the upload part size
// and as the download window.
const mediaPartSize = 512 * 1024

// mediaClient is one logged-in gotd client: its user id, its command channel
// and the updates it received.
type mediaClient struct {
	id    int64
	cmds  chan command
	coll  *updateCollector
	errCh chan error
}

// bootMediaEnv boots a server with delivery and logs in one client per phone.
// The blob directory comes from bootServerWithDelivery, which already gives
// each booted server its own t.TempDir(), so nothing here reads the
// environment and t.Parallel() stays usable.
func bootMediaEnv(t *testing.T, ctx context.Context, phones ...string) []*mediaClient {
	t.Helper()

	key, err := rsakey.LoadOrGenerate(t.TempDir() + "/key.pem")
	if err != nil {
		t.Fatal(err)
	}
	dsn := pgtest.DSN(t)
	st, err := store.Open(ctx, dsn, pgtest.EncKey())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cerr := st.Close(); cerr != nil {
			t.Errorf("store close: %v", cerr)
		}
	})

	const dcID = 2
	codes := newMultiCodeSink()
	ln := mustListen(t, ctx, "127.0.0.1:0")
	port := tcpPort(t, ln)
	t.Cleanup(bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln))

	clients := make([]*mediaClient, 0, len(phones))
	t.Cleanup(func() {
		for _, mc := range clients {
			close(mc.cmds)
			if rerr := <-mc.errCh; rerr != nil && !errors.Is(rerr, context.Canceled) && !errors.Is(rerr, context.DeadlineExceeded) {
				t.Errorf("client run: %v", rerr)
			}
		}
	})
	for _, phone := range phones {
		mc := &mediaClient{
			cmds:  make(chan command),
			coll:  newUpdateCollector(),
			errCh: make(chan error, 1),
		}
		client := createClient(port, key, dcID, mc.coll, nil)
		idCh := make(chan int64, 1)
		go func() { mc.errCh <- runInteractive(ctx, client, flowFor(phone, codes), idCh, mc.cmds) }()
		select {
		case mc.id = <-idCh:
		case <-ctx.Done():
			t.Fatalf("client %s login timeout: %v", phone, ctx.Err())
		}
		clients = append(clients, mc)
	}
	return clients
}

// execMedia runs one command on a client and returns its error instead of
// failing the test, for the cases where the error is the assertion.
func execMedia(t *testing.T, cmds chan command, fn func(ctx context.Context, c *tg.Client) error) error {
	t.Helper()
	d := make(chan error, 1)
	select {
	case cmds <- command{fn: fn, done: d}:
	case <-time.After(10 * time.Second):
		t.Fatal("command enqueue timeout")
	}
	return <-d
}

func assertRPCError(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected %s, got nil error", want)
	}
	var tgErr *tgerr.Error
	if !errors.As(err, &tgErr) {
		t.Fatalf("error type = %T, want *tgerr.Error", err)
	}
	if tgErr.Message != want {
		t.Fatalf("error = %s, want %s", tgErr.Message, want)
	}
}

// mediaPayload builds a payload whose every byte depends on its position, so a
// part assembled out of order fails a byte comparison rather than passing on a
// repeated filler byte.
func mediaPayload(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// uploadParts saves payload as 512 KiB parts under fileID and returns the part
// count, with the last part short whenever the payload does not divide evenly.
func uploadParts(t *testing.T, mc *mediaClient, fileID int64, payload []byte) int {
	t.Helper()
	parts := 0
	for off := 0; off < len(payload); off += mediaPartSize {
		part, chunk := parts, payload[off:min(off+mediaPartSize, len(payload))]
		execChat(t, mc.cmds, func(ctx context.Context, c *tg.Client) error {
			ok, err := c.UploadSaveFilePart(ctx, &tg.UploadSaveFilePartRequest{
				FileID: fileID, FilePart: part, Bytes: chunk,
			})
			if err != nil {
				return err
			}
			if !ok {
				return fmt.Errorf("saveFilePart %d returned false", part)
			}
			return nil
		})
		parts++
	}
	return parts
}

// sendUploadedDocument sends the just-uploaded parts as a document and returns
// the document off the sender's own updateNewMessage.
func sendUploadedDocument(
	t *testing.T, mc *mediaClient, peer tg.InputPeerClass,
	fileID int64, parts int, name, caption string, randomID int64,
) *tg.Document {
	t.Helper()
	var ups tg.UpdatesClass
	execChat(t, mc.cmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
			Peer: peer,
			Media: &tg.InputMediaUploadedDocument{
				File:     &tg.InputFile{ID: fileID, Parts: parts, Name: name},
				MimeType: "application/octet-stream",
			},
			Message:  caption,
			RandomID: randomID,
		})
		if err != nil {
			return err
		}
		ups = res
		return nil
	})

	full, ok := ups.(*tg.Updates)
	if !ok {
		t.Fatalf("sendMedia updates type = %T, want *tg.Updates", ups)
	}
	for _, u := range full.Updates {
		newMsg, ok := u.(*tg.UpdateNewMessage)
		if !ok {
			continue
		}
		msg, ok := newMsg.Message.(*tg.Message)
		if !ok {
			t.Fatalf("sendMedia message type = %T, want *tg.Message", newMsg.Message)
		}
		return documentOf(t, msg)
	}
	t.Fatal("sendMedia carried no updateNewMessage")
	return nil
}

// documentOf reads the document off a message, failing if the message carries
// no document media.
func documentOf(t *testing.T, msg *tg.Message) *tg.Document {
	t.Helper()
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok {
		t.Fatalf("message media = %T, want *tg.MessageMediaDocument", msg.Media)
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		t.Fatalf("document = %T, want *tg.Document", media.Document)
	}
	return doc
}

// downloadDocument walks a document in 512 KiB windows until a reply comes back
// shorter than the window, which is how the last window is recognised without
// assuming the file divides evenly.
func downloadDocument(t *testing.T, mc *mediaClient, doc *tg.Document) []byte {
	t.Helper()
	var out []byte
	for offset := int64(0); ; {
		var chunk []byte
		at := offset
		execChat(t, mc.cmds, func(ctx context.Context, c *tg.Client) error {
			res, err := c.UploadGetFile(ctx, &tg.UploadGetFileRequest{
				Location: &tg.InputDocumentFileLocation{
					ID:            doc.ID,
					AccessHash:    doc.AccessHash,
					FileReference: doc.FileReference,
				},
				Offset: at,
				Limit:  mediaPartSize,
			})
			if err != nil {
				return err
			}
			file, ok := res.(*tg.UploadFile)
			if !ok {
				return fmt.Errorf("getFile reply type = %T, want *tg.UploadFile", res)
			}
			chunk = file.Bytes
			return nil
		})
		out = append(out, chunk...)
		offset += int64(len(chunk))
		if len(chunk) < mediaPartSize {
			return out
		}
	}
}

// assertSameDocument compares the fields a recipient must see unchanged.
func assertSameDocument(t *testing.T, got, want *tg.Document, who string) {
	t.Helper()
	if got.ID != want.ID || got.AccessHash != want.AccessHash {
		t.Fatalf("%s document (%d,%d), want (%d,%d)", who, got.ID, got.AccessHash, want.ID, want.AccessHash)
	}
	if got.Size != want.Size {
		t.Fatalf("%s document size = %d, want %d", who, got.Size, want.Size)
	}
	if got.MimeType != want.MimeType {
		t.Fatalf("%s document mime = %q, want %q", who, got.MimeType, want.MimeType)
	}
	if fileNameOf(got) != fileNameOf(want) {
		t.Fatalf("%s document name = %q, want %q", who, fileNameOf(got), fileNameOf(want))
	}
}

func fileNameOf(doc *tg.Document) string {
	for _, a := range doc.Attributes {
		if name, ok := a.(*tg.DocumentAttributeFilename); ok {
			return name.FileName
		}
	}
	return ""
}

// TestMediaRoundTrip is the M5 gate: A uploads a multi-part payload, sends it to
// B, and B downloads bytes identical to what A uploaded.
func TestMediaRoundTrip(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	clients := bootMediaEnv(t, ctx, "+15551310001", "+15551310002")
	a, b := clients[0], clients[1]

	payload := mediaPayload(mediaPartSize + 7)
	const clientFileID = int64(0x5ED10001)
	parts := uploadParts(t, a, clientFileID, payload)
	if parts != 2 {
		t.Fatalf("upload parts = %d, want 2", parts)
	}

	peerB := peerUser(a.id, b.id)
	doc := sendUploadedDocument(t, a, peerB, clientFileID, parts, "gate.bin", "here", 910001)
	if doc.Size != int64(len(payload)) {
		t.Fatalf("document size = %d, want %d", doc.Size, len(payload))
	}
	if doc.MimeType != "application/octet-stream" {
		t.Fatalf("document mime = %q", doc.MimeType)
	}
	if fileNameOf(doc) != "gate.bin" {
		t.Fatalf("document name = %q, want %q", fileNameOf(doc), "gate.bin")
	}

	// B sees the same document on its own history.
	peerA := peerUser(b.id, a.id)
	var bDoc *tg.Document
	execChat(t, b.cmds, func(ctx context.Context, c *tg.Client) error {
		res, err := c.MessagesGetHistory(ctx, &tg.MessagesGetHistoryRequest{Peer: peerA, Limit: 10})
		if err != nil {
			return err
		}
		msgs, ok := res.(*tg.MessagesMessages)
		if !ok {
			return fmt.Errorf("history type = %T, want *tg.MessagesMessages", res)
		}
		if len(msgs.Messages) != 1 {
			return fmt.Errorf("history len = %d, want 1", len(msgs.Messages))
		}
		msg, ok := msgs.Messages[0].(*tg.Message)
		if !ok {
			return fmt.Errorf("history message type = %T, want *tg.Message", msgs.Messages[0])
		}
		if msg.Message != "here" {
			return fmt.Errorf("caption = %q, want %q", msg.Message, "here")
		}
		bDoc = documentOf(t, msg)
		return nil
	})
	assertSameDocument(t, bDoc, doc, "B")

	// The gate: the bytes B downloads are the bytes A uploaded.
	got := downloadDocument(t, b, bDoc)
	if !bytes.Equal(got, payload) {
		t.Fatalf("downloaded %d bytes, want %d identical bytes", len(got), len(payload))
	}

	// A wrong access hash resolves to nothing, not to the file.
	err := execMedia(t, b.cmds, func(ctx context.Context, c *tg.Client) error {
		_, gerr := c.UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{
				ID:            bDoc.ID,
				AccessHash:    bDoc.AccessHash + 1,
				FileReference: bDoc.FileReference,
			},
			Offset: 0,
			Limit:  mediaPartSize,
		})
		return gerr
	})
	assertRPCError(t, err, "LOCATION_INVALID")
}

// TestMediaDownloadRequiresOwnMessage proves the capability is not the pair: C,
// holding the exact (id, access_hash) B holds, is refused because no live
// message of C's names the file.
func TestMediaDownloadRequiresOwnMessage(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	clients := bootMediaEnv(t, ctx, "+15551311001", "+15551311002", "+15551311003")
	a, b, c := clients[0], clients[1], clients[2]

	payload := mediaPayload(mediaPartSize + 7)
	const clientFileID = int64(0x5ED10002)
	parts := uploadParts(t, a, clientFileID, payload)

	peerB := peerUser(a.id, b.id)
	doc := sendUploadedDocument(t, a, peerB, clientFileID, parts, "gate.bin", "here", 910002)

	// B, the recipient, can read it.
	if got := downloadDocument(t, b, doc); !bytes.Equal(got, payload) {
		t.Fatalf("B downloaded %d bytes, want %d identical bytes", len(got), len(payload))
	}

	// C was sent nothing, so the same pair is inert in its hands.
	err := execMedia(t, c.cmds, func(ctx context.Context, cl *tg.Client) error {
		_, gerr := cl.UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: &tg.InputDocumentFileLocation{
				ID:            doc.ID,
				AccessHash:    doc.AccessHash,
				FileReference: doc.FileReference,
			},
			Offset: 0,
			Limit:  mediaPartSize,
		})
		return gerr
	})
	assertRPCError(t, err, "LOCATION_INVALID")
}

// TestMediaInChatFanOut proves one stored file entitles every member of the
// chat it was sent to: B and C both download the same document.
func TestMediaInChatFanOut(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	clients := bootMediaEnv(t, ctx, "+15551312001", "+15551312002", "+15551312003")
	a, b, c := clients[0], clients[1], clients[2]

	var chatID int64
	execChat(t, a.cmds, func(ctx context.Context, cl *tg.Client) error {
		inv, err := cl.MessagesCreateChat(ctx, &tg.MessagesCreateChatRequest{
			Title: "Media",
			Users: []tg.InputUserClass{
				inputUser(a.id, b.id),
				inputUser(a.id, c.id),
			},
		})
		if err != nil {
			return err
		}
		ups, ok := inv.Updates.(*tg.Updates)
		if !ok {
			return fmt.Errorf("createChat updates type = %T, want *tg.Updates", inv.Updates)
		}
		if len(ups.Chats) != 1 {
			return errors.New("createChat: no chat in response")
		}
		chat, ok := ups.Chats[0].(*tg.Chat)
		if !ok {
			return fmt.Errorf("createChat chat type = %T, want *tg.Chat", ups.Chats[0])
		}
		chatID = chat.ID
		return nil
	})
	for _, m := range []struct {
		mc  *mediaClient
		who string
	}{{b, "B"}, {c, "C"}} {
		_, err := m.mc.coll.waitService(ctx, &tg.MessageActionChatCreate{})
		if err != nil {
			t.Fatalf("%s wait create service: %v", m.who, err)
		}
	}

	payload := mediaPayload(mediaPartSize + 7)
	const clientFileID = int64(0x5ED10003)
	parts := uploadParts(t, a, clientFileID, payload)
	doc := sendUploadedDocument(t, a,
		&tg.InputPeerChat{ChatID: chatID}, clientFileID, parts, "gate.bin", "here", 910003)

	// Both members receive the message live and download the same file.
	for _, m := range []struct {
		mc  *mediaClient
		who string
	}{{b, "B"}, {c, "C"}} {
		msg := recvOrCtx(t, ctx, m.mc.coll.newMsg, m.who+" chat media updateNewMessage")
		memberDoc := documentOf(t, msg)
		assertSameDocument(t, memberDoc, doc, m.who)
		if got := downloadDocument(t, m.mc, memberDoc); !bytes.Equal(got, payload) {
			t.Fatalf("%s downloaded %d bytes, want %d identical bytes", m.who, len(got), len(payload))
		}
	}
}
