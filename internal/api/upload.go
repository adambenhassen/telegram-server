package api

import (
	"errors"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/mtproto"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// maxFileParts bounds a part index. It is derived rather than configured:
// TG_MAX_FILE_BYTES over the 512 KiB protocol part size is the most parts a
// legal file can have, so anything past it is rejected at save time instead of
// letting the parts table absorb the difference until assembly.
func (h *handlers) maxFileParts() int {
	return int((h.maxFileBytes + store.MaxPartBytes - 1) / store.MaxPartBytes)
}

// handleSaveFilePart serves upload.saveFilePart: it stores one part of an
// in-flight upload under the caller's account, bounded by the per-file and
// per-user caps the store enforces inside its transaction.
func (h *handlers) handleSaveFilePart(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.UploadSaveFilePartRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if req.FileID == 0 || req.FilePart < 0 || req.FilePart >= h.maxFileParts() {
		return nil, errFilePartInvalid
	}
	if err := h.saveUploadPart(r, req.FileID, req.FilePart, req.Bytes); err != nil {
		return nil, err
	}
	return &tg.BoolTrue{}, nil
}

// handleSaveBigFilePart serves upload.saveBigFilePart. It validates
// file_total_parts but does not store it: the per-file byte cap is enforced on
// every single save inside the store transaction, so a client that changes its
// declared total between calls cannot get more bytes into the parts table than
// the cap allows, and assembly re-derives the true part count from the rows
// rather than trusting anything the client said.
func (h *handlers) handleSaveBigFilePart(r *mtproto.Request) (bin.Encoder, error) {
	var req tg.UploadSaveBigFilePartRequest
	if err := req.Decode(r.Buf); err != nil {
		return nil, errMethodNotImpl
	}
	if r.UserID == 0 {
		return nil, errAuthKeyUnreg
	}
	if req.FileID == 0 || req.FilePart < 0 || req.FilePart >= h.maxFileParts() {
		return nil, errFilePartInvalid
	}
	if req.FileTotalParts <= 0 || req.FileTotalParts > h.maxFileParts() {
		return nil, errFilePartInvalid
	}
	if req.FilePart >= req.FileTotalParts {
		return nil, errFilePartInvalid
	}
	if err := h.saveUploadPart(r, req.FileID, req.FilePart, req.Bytes); err != nil {
		return nil, err
	}
	return &tg.BoolTrue{}, nil
}

// saveUploadPart is the rate limit, the store call and the error mapping both
// save handlers share.
//
// The limit is checked here rather than in each handler so the two surfaces
// cannot drift onto separate budgets — they write the same rows, and a budget
// each would let an account double its part rate by alternating. It is checked
// before the store call, so a denied part costs the parts table nothing: the
// caps are enforced by writing first and reading the sums back, so reaching the
// store at all is the write amplification this limit exists to bound.
func (h *handlers) saveUploadPart(r *mtproto.Request, fileID int64, part int, payload []byte) error {
	if err := h.checkRateLimit(r, "save_file_part", h.rateLimitSaveFilePart); err != nil {
		return err
	}
	err := h.store.SaveUploadPart(r.Ctx, r.UserID, fileID, part, payload, h.maxFileBytes)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, store.ErrPartTooLarge), errors.Is(err, store.ErrFileTooLarge):
		return errFilePartTooBig
	// ErrTooManyParts is the row-count half of the same outstanding cap as
	// ErrUploadQuota, and clears the same way, so it reports the same error.
	case errors.Is(err, store.ErrUploadQuota), errors.Is(err, store.ErrTooManyParts):
		return errStorageQuota
	default:
		h.log.Error("save file part", "user_id", r.UserID, "err", err)
		return errInternal
	}
}
