package api

import (
	"errors"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/blob"
	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// maxDownloadChunk caps one upload.getFile reply. It is the protocol maximum,
// and it is also what bounds the per-request buffer: the read window is sized
// from the request's limit and the remaining file bytes, never from the file's
// size, so serving 4 KiB out of a 100 MiB file allocates 4 KiB.
const maxDownloadChunk = 1024 * 1024

// beginDownload claims the caller's single download slot, reporting false when
// one is already in flight.
func (h *handlers) beginDownload(userID int64) bool {
	h.downloadsMu.Lock()
	defer h.downloadsMu.Unlock()
	if h.downloads[userID] {
		return false
	}
	h.downloads[userID] = true
	return true
}

func (h *handlers) endDownload(userID int64) {
	h.downloadsMu.Lock()
	defer h.downloadsMu.Unlock()
	delete(h.downloads, userID)
}

// handleGetFile serves upload.getFile: one byte range of one stored file, to a
// caller the store's gate says owns a live message referencing it.
func (h *handlers) handleGetFile(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.UploadGetFileRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	// Only this one location type. InputPhotoFileLocation is rejected because M5
	// stores no photos, and every other InputFileLocation* because it names
	// something that does not exist here.
	loc, ok := req.Location.(*tg.InputDocumentFileLocation)
	if !ok {
		return nil, errLocationInvalid
	}
	// M5 stores no thumbnails, so a thumb request has no answer and must not
	// silently return the full file.
	if loc.ThumbSize != "" {
		return nil, errLocationInvalid
	}
	if req.Limit <= 0 || req.Limit > maxDownloadChunk || req.Offset < 0 {
		return nil, errLocationInvalid
	}

	if !h.beginDownload(r.UserID) {
		return nil, errDownloadBusy
	}
	defer h.endDownload(r.UserID)

	// loc.FileReference is deliberately not read, not compared and not
	// validated: it is a placeholder echoed on output and ignored on input, and
	// half-validating it would make it an oracle. Do not "complete" it.
	file, err := h.store.FileForDownload(r.Ctx, loc.ID, loc.AccessHash, r.UserID)
	switch {
	case errors.Is(err, store.ErrFileNotFound):
		// A rejection is a client mistake, not a server event, and this path is
		// reachable by anyone: it is not logged.
		return nil, errLocationInvalid
	case err != nil:
		h.log.Error("file for download", "user_id", r.UserID, "err", err)
		return nil, errInternal
	}

	// A window running past the end is served short rather than rejected:
	// upload.getFile is how a client walks a file in fixed-size windows, and the
	// last window is short by definition. offset == size is legal and returns
	// zero bytes, so a client that has read to the end gets an empty reply.
	if req.Offset > file.Size {
		return nil, errLocationInvalid
	}
	n := int64(req.Limit)
	if remaining := file.Size - req.Offset; n > remaining {
		n = remaining
	}

	b, err := h.blobs.ReadAt(r.Ctx, blob.Key(file.ID), req.Offset, n)
	if err != nil {
		// ErrNotFound here means the row says stored but the body is gone: a
		// server fault, not a client one.
		h.log.Error("read file blob", "file_id", file.ID, "err", err)
		return nil, errInternal
	}

	return &tg.UploadFile{
		// storage.fileUnknown is the honest answer: the type field describes the
		// file's format, and the server never decodes an uploaded file, so it
		// cannot name one.
		Type:  &tg.StorageFileUnknown{},
		Mtime: int(file.Date.Unix()),
		Bytes: b,
	}, nil
}
