package api_test

import (
	"context"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// TestSaveFilePartRateLimit proves part N+1 in the window is denied with a
// FLOOD_WAIT carrying the real remaining wait, and that the denied part is not
// stored: the cap-rollover write the limit exists to bound must not happen on
// the way to the refusal.
func TestSaveFilePartRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	u, err := s.CreateUser(ctx, "+15551298001")
	if err != nil {
		t.Fatal(err)
	}

	const fileID = 4001
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	for i := range 2 {
		if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
			FileID: fileID, FilePart: i, Bytes: []byte("part"),
		}); err != nil {
			t.Fatalf("part %d: %v", i, err)
		}
	}

	_, err = api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
		FileID: fileID, FilePart: 2, Bytes: []byte("part"),
	})
	wait := floodWaitSeconds(t, err)
	if wait < 1 || wait > 10 {
		t.Fatalf("part 3: got %v, want FLOOD_WAIT within the 10s window", err)
	}

	if _, ok, err := s.UploadPart(ctx, u.ID, fileID, 2); err != nil || ok {
		t.Fatalf("denied part stored: ok=%v err=%v", ok, err)
	}
	parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, fileID)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 2 {
		t.Fatalf("stored %d parts, want 2", parts)
	}
}

// TestSaveFilePartRateLimitWindowExpiry proves the account uploads again once
// the window rolls over — the limit paces a large upload, it does not end it.
func TestSaveFilePartRateLimitWindowExpiry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	u, err := s.CreateUser(ctx, "+15551298002")
	if err != nil {
		t.Fatal(err)
	}

	const fileID = 4002
	cfg := store.RateLimitConfig{Limit: 1, Window: 500 * time.Millisecond}
	if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
		FileID: fileID, FilePart: 0, Bytes: []byte("part"),
	}); err != nil {
		t.Fatalf("part 0: %v", err)
	}
	if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
		FileID: fileID, FilePart: 1, Bytes: []byte("part"),
	}); !isFloodWait(err) {
		t.Fatalf("part 1: expected FLOOD_WAIT, got %v", err)
	}

	time.Sleep(600 * time.Millisecond)

	if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
		FileID: fileID, FilePart: 1, Bytes: []byte("part"),
	}); err != nil {
		t.Fatalf("part 1 after the window: %v", err)
	}
	if _, ok, err := s.UploadPart(ctx, u.ID, fileID, 1); err != nil || !ok {
		t.Fatalf("post-window part not stored: ok=%v err=%v", ok, err)
	}
}

// TestSaveFilePartRateLimitRetryCounts pins what the limit counts: calls, not
// distinct parts. A client retrying one part spends budget, because the write
// amplification the limit bounds is per call.
func TestSaveFilePartRateLimitRetryCounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	u, err := s.CreateUser(ctx, "+15551298003")
	if err != nil {
		t.Fatal(err)
	}

	const fileID = 4003
	cfg := store.RateLimitConfig{Limit: 2, Window: 10 * time.Second}
	for i := range 2 {
		if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
			FileID: fileID, FilePart: 0, Bytes: []byte("retry"),
		}); err != nil {
			t.Fatalf("retry %d: %v", i, err)
		}
	}
	if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
		FileID: fileID, FilePart: 0, Bytes: []byte("retry"),
	}); !isFloodWait(err) {
		t.Fatalf("retry 3: expected FLOOD_WAIT, got %v", err)
	}
	// The two retries that were allowed still deduped onto one row.
	parts, _, _, err := s.UploadPartsSummary(ctx, u.ID, fileID)
	if err != nil {
		t.Fatalf("summary: %v", err)
	}
	if parts != 1 {
		t.Fatalf("stored %d parts, want 1", parts)
	}
}

// TestSaveBigFilePartSharesRateLimit proves both save surfaces spend one
// budget. They write the same rows, so a separate budget per surface would let
// an account double its part rate by alternating between them.
func TestSaveBigFilePartSharesRateLimit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	u, err := s.CreateUser(ctx, "+15551298004")
	if err != nil {
		t.Fatal(err)
	}

	const fileID = 4004
	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveFilePartRequest{
		FileID: fileID, FilePart: 0, Bytes: []byte("part"),
	}); err != nil {
		t.Fatalf("saveFilePart: %v", err)
	}
	if _, err := api.SaveBigFilePartForTestWithLimits(s, u.ID, cfg, &tg.UploadSaveBigFilePartRequest{
		FileID: fileID, FilePart: 1, FileTotalParts: 2, Bytes: []byte("part"),
	}); !isFloodWait(err) {
		t.Fatalf("saveBigFilePart: expected FLOOD_WAIT, got %v", err)
	}
	if _, ok, err := s.UploadPart(ctx, u.ID, fileID, 1); err != nil || ok {
		t.Fatalf("denied big part stored: ok=%v err=%v", ok, err)
	}
}

// TestSaveFilePartRateLimitIndependentAccounts proves one account's upload
// budget is its own.
func TestSaveFilePartRateLimitIndependentAccounts(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	alice, err := s.CreateUser(ctx, "+15551298005")
	if err != nil {
		t.Fatal(err)
	}
	bob, err := s.CreateUser(ctx, "+15551298006")
	if err != nil {
		t.Fatal(err)
	}

	cfg := store.RateLimitConfig{Limit: 1, Window: 10 * time.Second}
	req := func(part int) *tg.UploadSaveFilePartRequest {
		return &tg.UploadSaveFilePartRequest{FileID: 4005, FilePart: part, Bytes: []byte("part")}
	}
	if _, err := api.SaveFilePartForTestWithLimits(s, alice.ID, cfg, req(0)); err != nil {
		t.Fatalf("alice part 0: %v", err)
	}
	if _, err := api.SaveFilePartForTestWithLimits(s, alice.ID, cfg, req(1)); !isFloodWait(err) {
		t.Fatalf("alice part 1: expected FLOOD_WAIT, got %v", err)
	}
	if _, err := api.SaveFilePartForTestWithLimits(s, bob.ID, cfg, req(0)); err != nil {
		t.Fatalf("bob part 0: %v", err)
	}
}

// TestSaveFilePartRateLimitDisabled proves an unset limit enforces nothing, the
// same "zero disables" reading every other surface has.
func TestSaveFilePartRateLimitDisabled(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	u, err := s.CreateUser(ctx, "+15551298007")
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := api.SaveFilePartForTestWithLimits(s, u.ID, store.RateLimitConfig{}, &tg.UploadSaveFilePartRequest{
			FileID: 4006, FilePart: i, Bytes: []byte("part"),
		}); err != nil {
			t.Fatalf("part %d: %v", i, err)
		}
	}
}
