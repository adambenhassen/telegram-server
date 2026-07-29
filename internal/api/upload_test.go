package api_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestSaveFilePartRejectsUnauthenticated(t *testing.T) {
	t.Parallel()
	s := openStore(t)
	_, err := api.SaveFilePartForTest(s, 0, &tg.UploadSaveFilePartRequest{
		FileID: 999, FilePart: 0, Bytes: []byte("hello world"),
	})
	rpcError(t, err, "AUTH_KEY_UNREGISTERED")
}

func TestSaveFilePartRejectsBadInput(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294001")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	tests := map[string]struct {
		req  tg.UploadSaveFilePartRequest
		want string
	}{
		"zero file id": {
			req:  tg.UploadSaveFilePartRequest{FileID: 0, FilePart: 0, Bytes: []byte("hello world")},
			want: "FILE_PART_INVALID",
		},
		"negative part": {
			req:  tg.UploadSaveFilePartRequest{FileID: 999, FilePart: -1, Bytes: []byte("hello world")},
			want: "FILE_PART_INVALID",
		},
		"part at max": {
			req:  tg.UploadSaveFilePartRequest{FileID: 999, FilePart: api.MaxFileParts(), Bytes: []byte("hello world")},
			want: "FILE_PART_INVALID",
		},
		"payload over protocol max": {
			req:  tg.UploadSaveFilePartRequest{FileID: 999, FilePart: 1, Bytes: bytes.Repeat([]byte{'a'}, store.MaxPartBytes+1)},
			want: "FILE_PART_TOO_BIG",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := api.SaveFilePartForTest(s, u.ID, &tc.req)
			rpcError(t, err, tc.want)
		})
	}
}

func TestSaveFilePartStoresPayload(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294002")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	want := []byte("hello world")
	res, err := api.SaveFilePartForTest(s, u.ID, &tg.UploadSaveFilePartRequest{
		FileID: 999, FilePart: 0, Bytes: want,
	})
	if err != nil {
		t.Fatalf("save file part: %v", err)
	}
	if _, ok := res.(*tg.BoolTrue); !ok {
		t.Fatalf("result = %T, want *tg.BoolTrue", res)
	}
	got, ok, err := s.UploadPart(ctx, u.ID, 999, 0)
	if err != nil || !ok {
		t.Fatalf("read back: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("payload = %q, want %q", got, want)
	}
}

// TestSaveFilePartAcceptsLastPart pins the boundary the rejection above sits on:
// one index below the derived maximum is a legal part of a max-size file.
func TestSaveFilePartAcceptsLastPart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294003")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	last := api.MaxFileParts() - 1
	if _, err := api.SaveFilePartForTest(s, u.ID, &tg.UploadSaveFilePartRequest{
		FileID: 999, FilePart: last, Bytes: []byte("hello world"),
	}); err != nil {
		t.Fatalf("save part %d: %v", last, err)
	}
	if _, ok, err := s.UploadPart(ctx, u.ID, 999, last); err != nil || !ok {
		t.Fatalf("read back part %d: ok=%v err=%v", last, ok, err)
	}
}

// TestSaveFilePartRejectedIndexWritesNothing is the aliasing guard: a rejected
// index must not land on another part's row. file_part is 32 bits on the wire,
// so an index past MaxInt32 cannot arrive here at all — that branch of
// store.partIndexOf is covered in the store's own test — but an index past the
// derived per-file maximum can, and it must leave part 0 untouched.
func TestSaveFilePartRejectedIndexWritesNothing(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294004")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	want := []byte("hello world")
	if _, err := api.SaveFilePartForTest(s, u.ID, &tg.UploadSaveFilePartRequest{
		FileID: 999, FilePart: 0, Bytes: want,
	}); err != nil {
		t.Fatalf("save part 0: %v", err)
	}
	for _, bad := range []int{-1, api.MaxFileParts()} {
		_, err := api.SaveFilePartForTest(s, u.ID, &tg.UploadSaveFilePartRequest{
			FileID: 999, FilePart: bad, Bytes: []byte("clobber"),
		})
		rpcError(t, err, "FILE_PART_INVALID")
	}
	got, ok, err := s.UploadPart(ctx, u.ID, 999, 0)
	if err != nil || !ok {
		t.Fatalf("read back: ok=%v err=%v", ok, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("part 0 was overwritten: got %q, want %q", got, want)
	}
}

func TestSaveBigFilePartValidatesTotalParts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)
	u, err := s.CreateUser(ctx, "+15551294005")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}

	tests := map[string]tg.UploadSaveBigFilePartRequest{
		"part past declared total": {FileID: 999, FilePart: 3, FileTotalParts: 3, Bytes: []byte("hello world")},
		"total parts zero":         {FileID: 999, FilePart: 0, FileTotalParts: 0, Bytes: []byte("hello world")},
		"total parts past max":     {FileID: 999, FilePart: 0, FileTotalParts: api.MaxFileParts() + 1, Bytes: []byte("hello world")},
	}
	for name, req := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := api.SaveBigFilePartForTest(s, u.ID, &req)
			rpcError(t, err, "FILE_PART_INVALID")
		})
	}

	if _, err := api.SaveBigFilePartForTest(s, u.ID, &tg.UploadSaveBigFilePartRequest{
		FileID: 999, FilePart: 2, FileTotalParts: 3, Bytes: []byte("hello world"),
	}); err != nil {
		t.Fatalf("save big file part: %v", err)
	}
	if _, ok, err := s.UploadPart(ctx, u.ID, 999, 2); err != nil || !ok {
		t.Fatalf("read back: ok=%v err=%v", ok, err)
	}
}
