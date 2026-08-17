package api_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/blob"
)

// TestSaveBigFilePartRoundTripIsByteIdentical is criterion 1 for the
// saveBigFilePart surface: a multi-part upload followed by messages.sendMedia
// produces a downloadable file whose bytes are byte-identical to what was
// uploaded. The payload is position-dependent so a part-ordering or
// truncation bug cannot hide in repeated bytes.
func TestSaveBigFilePartRoundTripIsByteIdentical(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	blobs := newBlobs(t)
	a, err := s.CreateUser(ctx, "+15551297011")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551297012")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	const fileID = 7001
	const partSize = 512 * 1024
	const nParts = 3
	payload := make([]byte, partSize*(nParts-1)+7)
	for i := range payload {
		payload[i] = byte(i*31 + 7)
	}
	for i := range nParts {
		lo := i * partSize
		if lo >= len(payload) {
			break
		}
		chunk := payload[lo:min(lo+partSize, len(payload))]
		if _, err := api.SaveBigFilePartForTest(s, a.ID, &tg.UploadSaveBigFilePartRequest{
			FileID: fileID, FilePart: i, FileTotalParts: nParts, Bytes: chunk,
		}); err != nil {
			t.Fatalf("save big part %d: %v", i, err)
		}
	}

	enc, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(fileID, nParts, "big.bin", "application/octet-stream"),
		RandomID: 60,
	})
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	doc := documentOf(t, enc)
	if doc.Size != int64(len(payload)) {
		t.Fatalf("size = %d, want %d", doc.Size, len(payload))
	}

	body, err := blobs.ReadAt(ctx, blob.Key(doc.ID), 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("assembled bytes differ from the upload")
	}
}

// TestSaveFilePartRoundTripIsByteIdentical is criterion 1 for the
// saveFilePart surface.
func TestSaveFilePartRoundTripIsByteIdentical(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	blobs := newBlobs(t)
	a, err := s.CreateUser(ctx, "+15551297021")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551297022")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	const fileID = 7002
	payload := make([]byte, 512*1024+4096)
	for i := range payload {
		payload[i] = byte(i*13 + 3)
	}
	saveParts(t, s, a.ID, fileID, payload[:512*1024], payload[512*1024:])

	enc, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(fileID, 2, "file.bin", "application/octet-stream"),
		RandomID: 61,
	})
	if err != nil {
		t.Fatalf("send media: %v", err)
	}
	doc := documentOf(t, enc)
	body, err := blobs.ReadAt(ctx, blob.Key(doc.ID), 0, int64(len(payload)))
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if !bytes.Equal(body, payload) {
		t.Fatalf("assembled bytes differ from the upload")
	}
}

// TestResaveIsReflectedInAssembledFile is criterion 3 read end to end: the
// assembled file reflects the retry. The retry is the case that breaks if the
// recorded size stops describing the object the row names — a part re-saved
// smaller then has a row claiming more bytes than its object holds, the
// fail-closed reconciliation at assembly rejects it, and a legal upload can
// never be sent. The assertion is on the downloadable bytes, not on the part.
func TestResaveIsReflectedInAssembledFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	blobs := newBlobs(t)
	a, err := s.CreateUser(ctx, "+15551297051")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551297052")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	const fileID = 7006
	first := bytes.Repeat([]byte{'A'}, 512*1024)
	second := []byte("tail")
	saveParts(t, s, a.ID, fileID, first, second)

	// The retry: part 0 comes back far smaller than the bytes already stored
	// for it.
	retry := []byte("retried")
	if _, err := api.SaveFilePartForTest(s, a.ID, &tg.UploadSaveFilePartRequest{
		FileID: fileID, FilePart: 0, Bytes: retry,
	}); err != nil {
		t.Fatalf("re-save part 0: %v", err)
	}

	enc, err := api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(fileID, 2, "retry.bin", "application/octet-stream"),
		RandomID: 63,
	})
	if err != nil {
		t.Fatalf("send media after a shrinking retry: %v", err)
	}
	doc := documentOf(t, enc)
	want := append(append([]byte{}, retry...), second...)
	if doc.Size != int64(len(want)) {
		t.Fatalf("size = %d, want %d", doc.Size, len(want))
	}
	body, err := blobs.ReadAt(ctx, blob.Key(doc.ID), 0, int64(len(want)))
	if err != nil {
		t.Fatalf("read blob: %v", err)
	}
	if !bytes.Equal(body, want) {
		t.Fatalf("assembled file does not reflect the retry: got %q", body)
	}
}

// TestUploadPartsIsolatedAcrossAccounts is criterion 7: one account cannot
// read or overwrite another account's in-flight part, including when both
// chose the same file_id. The test tries both directions through the RPC
// surface: b saves under file_id F, a saves over the same (F, part), and the
// assertion is that b's bytes are untouched and a's bytes are a separate
// object.
func TestUploadPartsIsolatedAcrossAccounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	a, err := s.CreateUser(ctx, "+15551297031")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551297032")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	const fileID = 7003
	aWant := bytes.Repeat([]byte{'a'}, 2048)
	bWant := bytes.Repeat([]byte{'b'}, 4096)
	if _, err := api.SaveFilePartForTest(s, a.ID, &tg.UploadSaveFilePartRequest{FileID: fileID, FilePart: 0, Bytes: aWant}); err != nil {
		t.Fatalf("save a: %v", err)
	}
	// b names the same file_id and the same part index.
	if _, err := api.SaveFilePartForTest(s, b.ID, &tg.UploadSaveFilePartRequest{FileID: fileID, FilePart: 0, Bytes: bWant}); err != nil {
		t.Fatalf("save b: %v", err)
	}

	// Each account reads back its own bytes.
	aGot, ok, err := s.UploadPart(ctx, a.ID, fileID, 0)
	if err != nil || !ok {
		t.Fatalf("read a: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(aGot, aWant) {
		t.Fatalf("a read the wrong bytes: got %d bytes, want %d", len(aGot), len(aWant))
	}
	bGot, ok, err := s.UploadPart(ctx, b.ID, fileID, 0)
	if err != nil || !ok {
		t.Fatalf("read b: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(bGot, bWant) {
		t.Fatalf("b read the wrong bytes: got %d bytes, want %d", len(bGot), len(bWant))
	}

	// The two parts are separate objects: the keys differ.
	aKey, _, ok, err := s.UploadPartKey(ctx, a.ID, fileID, 0)
	if err != nil || !ok {
		t.Fatalf("a key: ok=%v err=%v", ok, err)
	}
	bKey, _, ok, err := s.UploadPartKey(ctx, b.ID, fileID, 0)
	if err != nil || !ok {
		t.Fatalf("b key: ok=%v err=%v", ok, err)
	}
	if aKey == "" || aKey == bKey {
		t.Fatalf("two accounts' parts share a blob key (%q): isolation is gone", aKey)
	}

	// a re-saving its part does not touch b's object.
	if _, err := api.SaveFilePartForTest(s, a.ID, &tg.UploadSaveFilePartRequest{FileID: fileID, FilePart: 0, Bytes: bytes.Repeat([]byte{'c'}, 1024)}); err != nil {
		t.Fatalf("re-save a: %v", err)
	}
	bGot, ok, err = s.UploadPart(ctx, b.ID, fileID, 0)
	if err != nil || !ok {
		t.Fatalf("read b after a re-save: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(bGot, bWant) {
		t.Fatalf("b's bytes changed after a re-save: got %d bytes, want %d", len(bGot), len(bWant))
	}
}

// TestAssemblyFailsClosedOnMissingBytes is criterion 16: the reconciliation of
// recorded accounting against bytes actually read back at assembly stays and
// stays fail-closed. A part whose bytes are gone — the crash window between a
// row commit and its byte write — fails the assembly rather than assembling a
// file the row does not describe, and it fails the same way an absent part
// does.
func TestAssemblyFailsClosedOnMissingBytes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s, _ := openStoreDSN(t)
	blobs := newBlobs(t)
	a, err := s.CreateUser(ctx, "+15551297041")
	if err != nil {
		t.Fatalf("user a: %v", err)
	}
	b, err := s.CreateUser(ctx, "+15551297042")
	if err != nil {
		t.Fatalf("user b: %v", err)
	}

	const fileID = 7005
	saveParts(t, s, a.ID, fileID, []byte("first "), []byte("second"))
	// Simulate the crash: part 1's bytes never land. The part bytes live in
	// the store's blob backend, so remove them from there.
	key, _, ok, err := s.UploadPartKey(ctx, a.ID, fileID, 1)
	if err != nil || !ok {
		t.Fatalf("part 1 key: ok=%v err=%v", ok, err)
	}
	if err := s.PartBlobs().Remove(ctx, key); err != nil {
		t.Fatalf("remove part 1 bytes: %v", err)
	}

	_, err = api.SendMediaForTest(s, a.ID, blobs, api.TestMaxUserStorageBytes, &tg.MessagesSendMediaRequest{
		Peer:     api.InputPeerUser(a.ID, b.ID),
		Media:    uploadedDocument(fileID, 2, "missing.txt", "text/plain"),
		RandomID: 62,
	})
	if err == nil {
		t.Fatal("assembly with a part whose bytes are gone: err=nil, want a failure (fail closed)")
	}
}
