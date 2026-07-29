package api

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// partsReader streams an in-flight upload's parts out of Postgres in index
// order, so assembling a 100 MiB file never holds more than one 512 KiB part in
// memory at a time.
type partsReader struct {
	ctx    context.Context
	store  *store.Store
	userID int64
	fileID int64
	total  int
	next   int
	buf    []byte
}

// Read fills b from the current part, fetching the next one when it runs out.
// The loop rather than an if matters: a zero-length part would otherwise make
// Read return (0, nil) forever.
func (p *partsReader) Read(b []byte) (int, error) {
	for len(p.buf) == 0 {
		if p.next >= p.total {
			return 0, io.EOF
		}
		payload, ok, err := p.store.UploadPart(p.ctx, p.userID, p.fileID, p.next)
		if err != nil {
			return 0, err
		}
		if !ok {
			return 0, fmt.Errorf("upload part %d missing", p.next)
		}
		p.buf = payload
		p.next++
	}
	n := copy(b, p.buf)
	p.buf = p.buf[n:]
	return n, nil
}

// sanitizeMIME constrains the client-supplied mime type before it is stored.
// The server never executes it or selects a code path on it, and it never
// appears in a storage key — the risk is downstream: it is echoed to every
// other client, and it is the field most likely to become a Content-Type the
// first time a CDN or webfile path exists, where a CR or LF in it is a
// header-splitting primitive. Constraining it here means it can never become
// that. Anything that does not qualify becomes the generic type rather than an
// error, so a client with an odd type still sends its file.
func sanitizeMIME(s string) string {
	const fallback = "application/octet-stream"
	slash := strings.IndexByte(s, '/')
	if len(s) == 0 || len(s) > 255 || slash <= 0 || slash == len(s)-1 {
		return fallback
	}
	if strings.IndexByte(s[slash+1:], '/') >= 0 {
		return fallback
	}
	for i := range len(s) {
		// Printable ASCII excluding space: no CR, no LF, no NUL, no control
		// bytes, nothing that needs an encoding to survive a round trip.
		if s[i] < 0x21 || s[i] > 0x7e {
			return fallback
		}
	}
	return s
}

// sanitizeFileName constrains the client-supplied file name. It is stored as an
// opaque column and echoed on download; it is never part of a blob key, which
// is derived from the file id alone, so path traversal is closed by
// construction rather than by this function. What is left is what Postgres
// cannot store and what corrupts a rendered name, and an unusable name becomes
// no name rather than an error.
//
// Spaces and non-ASCII are deliberately allowed: a file name is display text,
// not a header value, and forcing it to ASCII would mangle every non-English
// name.
func sanitizeFileName(s string) string {
	if len(s) > 255 || !validText(s) || strings.ContainsAny(s, "\r\n") {
		return ""
	}
	return s
}

// handleSendMedia serves messages.sendMedia: it assembles an in-flight upload
// into the blob store and sends it as a document message, to a user or to a
// chat. It reuses the two send paths sendMessage already uses.
//
// The file id written to messages.file_id is the one this handler just
// allocated under the caller's own account, never one the client named, so the
// download gate's "live message owned by caller" check can never be handed an
// entitlement to somebody else's file. The client-supplied id in the input file
// addresses upload parts only, and those are looked up under the caller's user
// id.
func (h *handlers) handleSendMedia(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.MessagesSendMediaRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	// Before the peer split, so the chat fan-out is guarded by the same check.
	if !validText(req.Message) {
		return nil, errMessageEmpty
	}
	peerType, toID, err := inputPeer(req.Peer)
	if err != nil {
		return nil, err
	}
	if peerType == store.PeerTypeChat {
		if err = h.requireMember(r.Ctx, toID, r.UserID); err != nil {
			return nil, err
		}
	}

	// One media type only. An uploaded photo is rejected too: serving a
	// tg.Photo requires pixel dimensions, which requires the server to decode
	// an uploaded image, and an image parser running on attacker-supplied
	// bytes in the main process is a decompression-bomb and CVE surface M5
	// deliberately declines. A client sending a photo sends it as a document.
	media, ok := req.Media.(*tg.InputMediaUploadedDocument)
	if !ok {
		return nil, errMediaInvalid
	}
	// M5 stores no thumbnails, and a thumbnail is a second file body this
	// handler has nowhere to put.
	if _, ok = media.GetThumb(); ok {
		return nil, errMediaInvalid
	}

	clientFileID, parts, name, err := inputFileParts(media.File)
	if err != nil {
		return nil, err
	}

	file, err := h.assembleFile(r.Ctx, r.UserID, clientFileID, parts, name, media.MimeType)
	if err != nil {
		return nil, err
	}
	files := map[int64]*tg.Document{file.ID: h.documentToTL(file)}

	if peerType == store.PeerTypeChat {
		return h.sendChatMedia(r, toID, &req, file, files)
	}

	sender, senderPts, _, dup, err := h.store.SendMessage(r.Ctx, r.UserID, toID, req.Message, req.RandomID, file.ID)
	if err != nil {
		h.log.Error("send media", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}
	if !dup {
		h.notify(r.Ctx, r.UserID)
		h.notify(r.Ctx, toID)
	}

	users, err := h.twoUsers(r.Ctx, r.UserID, toID)
	if err != nil {
		h.log.Error("send media users", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: int(sender.LocalID), RandomID: req.RandomID},
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, files), Pts: senderPts, PtsCount: 1},
		},
		Users: users,
		Date:  int(sender.Date.Unix()),
	}, nil
}

// sendChatMedia fans an assembled document out to every member of chatID, whose
// membership requireMember has already established, and returns the sender-side
// Updates.
func (h *handlers) sendChatMedia(
	r *mtproto.Request, chatID int64, req *tg.MessagesSendMediaRequest,
	file store.File, files map[int64]*tg.Document,
) (bin.Encoder, error) {
	sender, perOwner, dup, err := h.store.SendChatMessage(r.Ctx, store.FanOut{
		ChatID: chatID, FromID: r.UserID, Text: req.Message, RandomID: req.RandomID, FileID: file.ID,
	})
	if errors.Is(err, store.ErrNotMember) {
		return nil, errPeerIDInvalid
	}
	if err != nil {
		h.log.Error("send chat media", "user_id", r.UserID, "chat_id", chatID, "err", err)
		return nil, errInternal
	}
	if !dup {
		for uid := range perOwner {
			h.notify(r.Ctx, uid)
		}
	}

	recipients := make(map[int64]bool, len(perOwner))
	for uid := range perOwner {
		recipients[uid] = true
	}
	users, err := h.loadUsers(r.Ctx, recipients, r.UserID)
	if err != nil {
		h.log.Error("send chat media users", "err", err)
		return nil, errInternal
	}
	chats, err := h.loadChats(r.Ctx, map[int64]bool{chatID: true}, r.UserID)
	if err != nil {
		h.log.Error("send chat media chats", "err", err)
		return nil, errInternal
	}
	return &tg.Updates{
		Updates: []tg.UpdateClass{
			&tg.UpdateMessageID{ID: int(sender.LocalID), RandomID: req.RandomID},
			&tg.UpdateNewMessage{Message: messageToTL(sender, nil, files), Pts: perOwner[r.UserID], PtsCount: 1},
		},
		Users: users,
		Chats: chats,
		Date:  int(sender.Date.Unix()),
	}, nil
}

// inputFileParts reads the three fields assembly needs off an input file.
// tg.InputFile and tg.InputFileBig carry the same id, part count and name;
// every other input file names an already-stored file, which M5 does not send.
func inputFileParts(f tg.InputFileClass) (id int64, parts int, name string, err error) {
	switch v := f.(type) {
	case *tg.InputFile:
		id, parts, name = v.ID, v.Parts, v.Name
	case *tg.InputFileBig:
		id, parts, name = v.ID, v.Parts, v.Name
	default:
		return 0, 0, "", errMediaInvalid
	}
	if id == 0 || parts <= 0 {
		return 0, 0, "", errMediaInvalid
	}
	return id, parts, name, nil
}

// assembleFile turns an in-flight upload into a stored file. The order is the
// contract: the files row is created before the bytes are written and marked
// stored only after, so a crash between them leaves a row that no download can
// resolve rather than a file id serving whatever is at its key. Nothing cleans
// that row up — M5 has no deleter — so it stays counted against the uploader's
// quota, which is honest about what was consumed.
func (h *handlers) assembleFile(
	ctx context.Context, userID, clientFileID int64, parts int, name, mimeType string,
) (store.File, error) {
	n, maxIndex, total, err := h.store.UploadPartsSummary(ctx, userID, clientFileID)
	if err != nil {
		h.log.Error("assemble file", "user_id", userID, "err", err)
		return store.File{}, errInternal
	}
	// Part indexes are distinct and non-negative by the parts table's primary
	// key, so a count of parts with a maximum of parts-1 proves the set is
	// exactly {0 .. parts-1}: contiguous, no gaps, nothing past the end. An
	// upload that belongs to another account is simply not there, and fails
	// the same check.
	if n != int64(parts) || maxIndex != parts-1 || total <= 0 {
		return store.File{}, errMediaInvalid
	}

	file, err := h.store.AllocateFile(ctx, userID, total, sanitizeMIME(mimeType), sanitizeFileName(name), h.maxUserStorageBytes)
	if errors.Is(err, store.ErrStorageQuota) {
		return store.File{}, errFileQuota
	}
	if err != nil {
		h.log.Error("assemble file", "user_id", userID, "err", err)
		return store.File{}, errInternal
	}

	written, err := h.blobs.Put(ctx, blob.Key(file.ID), &partsReader{
		ctx: ctx, store: h.store, userID: userID, fileID: clientFileID, total: parts,
	})
	if err != nil {
		h.log.Error("assemble file", "user_id", userID, "file_id", file.ID, "err", err)
		return store.File{}, errInternal
	}
	// A mismatch means the parts changed under the read, so the blob does not
	// hold the file the row describes and must not be marked stored.
	if written != total {
		h.log.Error("assemble file", "user_id", userID, "file_id", file.ID,
			"err", fmt.Errorf("wrote %d bytes, expected %d", written, total))
		return store.File{}, errInternal
	}

	if err = h.store.MarkFileStored(ctx, file.ID); err != nil {
		h.log.Error("assemble file", "user_id", userID, "file_id", file.ID, "err", err)
		return store.File{}, errInternal
	}
	// The parts are redundant once the blob is stored, and the TTL sweeper
	// takes whatever this leaves behind, so a failure here does not fail a send
	// that has already succeeded.
	if _, err = h.store.DeleteUploadParts(ctx, userID, clientFileID); err != nil {
		h.log.Error("delete upload parts", "user_id", userID, "file_id", clientFileID, "err", err)
	}

	file.Stored = true
	return file, nil
}
