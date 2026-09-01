package store_test

import (
	"context"
	"testing"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestMetrics_Empty(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	ctx := context.Background()
	snap, err := st.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if snap.TotalUsers != 0 {
		t.Errorf("expected 0 users, got %d", snap.TotalUsers)
	}
	if snap.ActiveUsers1H != 0 {
		t.Errorf("expected 0 active users 1h, got %d", snap.ActiveUsers1H)
	}
	if snap.ActiveUsers24H != 0 {
		t.Errorf("expected 0 active users 24h, got %d", snap.ActiveUsers24H)
	}
	if snap.TotalChannels != 0 {
		t.Errorf("expected 0 channels, got %d", snap.TotalChannels)
	}
	if snap.TotalChats != 0 {
		t.Errorf("expected 0 chats, got %d", snap.TotalChats)
	}
	if snap.Messages1H != 0 {
		t.Errorf("expected 0 messages 1h, got %d", snap.Messages1H)
	}
	if snap.Messages24H != 0 {
		t.Errorf("expected 0 messages 24h, got %d", snap.Messages24H)
	}
	if snap.RateLimitHits1H != 0 {
		t.Errorf("expected 0 rate limit hits, got %d", snap.RateLimitHits1H)
	}
}

func TestMetrics_WithUsers(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	ctx := context.Background()

	// Create two users.
	if _, err := st.CreateUser(ctx, "15550000001"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateUser(ctx, "15550000002"); err != nil {
		t.Fatal(err)
	}

	snap, err := st.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if snap.TotalUsers != 2 {
		t.Errorf("expected 2 users, got %d", snap.TotalUsers)
	}
	if snap.StorageRows.Users != 2 {
		t.Errorf("expected 2 user rows, got %d", snap.StorageRows.Users)
	}
}

func TestMetrics_WithChats(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	ctx := context.Background()

	u, err := st.CreateUser(ctx, "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.CreateChat(ctx, u.ID, "Test Chat", []int64{u.ID}); err != nil {
		t.Fatal(err)
	}

	snap, err := st.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if snap.TotalChats != 1 {
		t.Errorf("expected 1 chat, got %d", snap.TotalChats)
	}
	if snap.StorageRows.Chats != 1 {
		t.Errorf("expected 1 chat row, got %d", snap.StorageRows.Chats)
	}
}

func TestMetrics_WithChannel(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	ctx := context.Background()

	u, err := st.CreateUser(ctx, "15550000001")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := st.CreateChannel(ctx, u.ID, "Test Channel", "", false); err != nil {
		t.Fatal(err)
	}

	snap, err := st.Metrics(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if snap.TotalChannels != 1 {
		t.Errorf("expected 1 channel, got %d", snap.TotalChannels)
	}
	if snap.StorageRows.Channels != 1 {
		t.Errorf("expected 1 channel row, got %d", snap.StorageRows.Channels)
	}
}

func TestMaxPtsGap_Empty(t *testing.T) {
	t.Parallel()

	dsn := pgtest.DSN(t)
	st, err := store.Open(context.Background(), dsn, pgtest.EncKey(), store.WithBlobStore(testBlobs(t)))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }() //nolint:errcheck // best-effort close in test

	ctx := context.Background()
	gap, err := st.MaxPtsGap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gap != 0 {
		t.Errorf("expected 0 gap, got %d", gap)
	}
}

func TestMaxPtsGap_ExcludesOfflineAccounts(t *testing.T) {
	t.Parallel()

	s := open(t)
	ctx := context.Background()
	offline := mustUser(t, s, "+15550000011")
	onlineA := mustUser(t, s, "+15550000012")
	onlineB := mustUser(t, s, "+15550000013")

	for i, userID := range []int64{offline.ID, onlineA.ID, onlineB.ID} {
		keyID := int64(1001 + i)
		if err := s.SaveAuthKey(ctx, keyID, []byte("test auth key")); err != nil {
			t.Fatalf("save auth key %d: %v", keyID, err)
		}
		if err := s.BindAuthKeyUser(ctx, keyID, userID); err != nil {
			t.Fatalf("bind auth key %d: %v", keyID, err)
		}
	}

	for userID, pts := range map[int64]int64{
		offline.ID: 1,
		onlineA.ID: 10,
		onlineB.ID: 12,
	} {
		if _, err := store.StorePool(s).Exec(ctx,
			`UPDATE update_state SET pts = $2 WHERE user_id = $1`, userID, pts,
		); err != nil {
			t.Fatalf("set pts for user %d: %v", userID, err)
		}
	}
	if err := s.SetUserStatus(ctx, onlineA.ID, true); err != nil {
		t.Fatalf("set user %d online: %v", onlineA.ID, err)
	}
	if err := s.SetUserStatus(ctx, onlineB.ID, true); err != nil {
		t.Fatalf("set user %d online: %v", onlineB.ID, err)
	}

	gap, err := s.MaxPtsGap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gap != 2 {
		t.Errorf("expected online pts spread 2, got %d", gap)
	}
}

func TestMaxPtsGap_NoOnlineAccounts(t *testing.T) {
	t.Parallel()

	s := open(t)
	ctx := context.Background()
	first := mustUser(t, s, "+15550000021")
	second := mustUser(t, s, "+15550000022")

	for i, userID := range []int64{first.ID, second.ID} {
		keyID := int64(2001 + i)
		if err := s.SaveAuthKey(ctx, keyID, []byte("test auth key")); err != nil {
			t.Fatalf("save auth key %d: %v", keyID, err)
		}
		if err := s.BindAuthKeyUser(ctx, keyID, userID); err != nil {
			t.Fatalf("bind auth key %d: %v", keyID, err)
		}
	}

	for userID, pts := range map[int64]int64{
		first.ID:  1,
		second.ID: 100,
	} {
		if _, err := store.StorePool(s).Exec(ctx,
			`UPDATE update_state SET pts = $2 WHERE user_id = $1`, userID, pts,
		); err != nil {
			t.Fatalf("set pts for user %d: %v", userID, err)
		}
	}

	gap, err := s.MaxPtsGap(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gap != 0 {
		t.Errorf("expected no online accounts to report 0, got %d", gap)
	}
}
