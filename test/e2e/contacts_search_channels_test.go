package e2e_test

import (
	"context"
	"errors"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/pgtest"
	"github.com/adambenhassen/telegram-server/internal/rsakey"
	"github.com/adambenhassen/telegram-server/internal/store"
)

// searchChannelIDs returns the channel ids named in a peer vector, ignoring
// user peers.
func searchChannelIDs(peers []tg.PeerClass) []int64 {
	out := make([]int64, 0, len(peers))
	for _, p := range peers {
		if ch, ok := p.(*tg.PeerChannel); ok {
			out = append(out, ch.ChannelID)
		}
	}
	return out
}

// searchChannel finds a rendered channel by id in a ContactsFound.
func searchChannel(found *tg.ContactsFound, id int64) *tg.Channel {
	for _, c := range found.Chats {
		if ch, ok := c.(*tg.Channel); ok && ch.ID == id {
			return ch
		}
	}
	return nil
}

// TestContactsSearchChannelDiscovery drives channel discovery through a real
// gotd client: a public channel is found by title by an account that is not a
// member, the access_hash it comes back with is enough to join, a private
// channel is invisible to a non-member and does not consume a row of the page,
// and membership — not the query — is what puts a channel in MyResults.
func TestContactsSearchChannelDiscovery(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

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
	addr, ok := ln.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("listener addr type = %T", ln.Addr())
	}
	stop := bootServerWithDelivery(t, ctx, key, dcID, st, dsn, codes.Logger(), ln)
	t.Cleanup(stop)

	const phoneA, phoneB = "+15551294001", "+15551294002"

	aCmds, bCmds := make(chan command), make(chan command)
	aID, bID := make(chan int64, 1), make(chan int64, 1)
	errA, errB := make(chan error, 1), make(chan error, 1)
	go func() {
		errA <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneA, codes), aID, aCmds)
	}()
	go func() {
		errB <- runInteractive(ctx, createClient(addr.Port, key, dcID, newUpdateCollector(), nil), flowFor(phoneB, codes), bID, bCmds)
	}()

	login := func(ch chan int64, who string) int64 {
		select {
		case id := <-ch:
			return id
		case <-ctx.Done():
			t.Fatalf("%s login timeout", who)
			return 0
		}
	}
	aUserID := login(aID, "A")
	bUserID := login(bID, "B")

	setUsername := func(chID int64, handle string) {
		t.Helper()
		execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
			_, err := c.ChannelsUpdateUsername(ctx, &tg.ChannelsUpdateUsernameRequest{
				Channel:  inputChannel(aUserID, chID),
				Username: handle,
			})
			return err
		})
	}
	search := func(cmds chan command, who, q string, limit int) *tg.ContactsFound {
		t.Helper()
		var found *tg.ContactsFound
		execChannel(t, ctx, cmds, func(ctx context.Context, c *tg.Client) error {
			var serr error
			found, serr = c.ContactsSearch(ctx, &tg.ContactsSearchRequest{Q: q, Limit: limit})
			return serr
		})
		if found == nil {
			t.Fatalf("%s contacts.search(%q) returned nil", who, q)
		}
		return found
	}

	// A creates two public channels and one private one, all matching the same
	// title token. Channel ids are random draws, so creation order fixes nothing
	// about where the private channel lands relative to the public two; what
	// this case still proves end to end is that the private channel is never
	// named and never shortens the page. The id-ordered version of that claim —
	// a private row placed deliberately inside the page — is asserted at the
	// handler level in TestContactsSearchPrivateChannelDoesNotConsumeLimit,
	// where the id can be seeded.
	publicID := createBroadcastChannel(t, ctx, aCmds, "Kayaking Club")
	setUsername(publicID, "kayakclub")
	privateID := createBroadcastChannel(t, ctx, aCmds, "Kayaking Private Crew")
	secondPublicID := createBroadcastChannel(t, ctx, aCmds, "Kayaking Digest")
	setUsername(secondPublicID, "kayakdigest")

	// Results page in id order, which creation order no longer predicts.
	wantPublic := []int64{publicID, secondPublicID}
	slices.Sort(wantPublic)

	// B is a member of nothing. Searching the shared title token returns both
	// public channels and neither names the private one.
	found := search(bCmds, "B", "Kayaking", 10)
	got := searchChannelIDs(found.Results)
	if !slices.Equal(got, wantPublic) {
		t.Fatalf("B Results channels = %v, want %v", got, wantPublic)
	}
	if ids := searchChannelIDs(found.MyResults); len(ids) != 0 {
		t.Fatalf("B MyResults channels = %v, want none (B is a member of nothing)", ids)
	}
	if searchChannel(found, privateID) != nil {
		t.Fatal("B was served the private channel in Chats")
	}

	rendered := searchChannel(found, publicID)
	if rendered == nil {
		t.Fatalf("public channel %d missing from Chats", publicID)
	}
	if rendered.Title != "Kayaking Club" {
		t.Errorf("Chats title = %q, want %q", rendered.Title, "Kayaking Club")
	}
	if rendered.Username != "kayakclub" {
		t.Errorf("Chats username = %q, want %q", rendered.Username, "kayakclub")
	}
	if !rendered.Left {
		t.Error("Chats Left = false, want true for a non-member")
	}
	if rendered.AccessHash == 0 {
		t.Fatal("Chats access_hash = 0, want the hash derived for B")
	}

	// A private channel matching the query must not shorten the page: with
	// limit=2 both public matches still come back, so nothing in the row count
	// says a third channel matched.
	limited := search(bCmds, "B", "Kayaking", 2)
	if ids := searchChannelIDs(limited.Results); len(ids) != 2 {
		t.Fatalf("B Results with limit=2 = %v (%d rows), want 2 — a private match consumed a row", ids, len(ids))
	}

	// Naming the private channel's own title is indistinguishable from naming
	// something that does not exist.
	private := search(bCmds, "B", "Kayaking Private Crew", 10)
	absent := search(bCmds, "B", "Nothingmatchesthisquery", 10)
	if len(private.Results) != 0 || len(private.MyResults) != 0 || len(private.Chats) != 0 || len(private.Users) != 0 {
		t.Fatalf("private-title search: Results=%d MyResults=%d Chats=%d Users=%d, want an empty response",
			len(private.Results), len(private.MyResults), len(private.Chats), len(private.Users))
	}
	if len(absent.Results) != 0 || len(absent.MyResults) != 0 || len(absent.Chats) != 0 || len(absent.Users) != 0 {
		t.Fatalf("no-match search is not empty: Results=%d MyResults=%d Chats=%d Users=%d",
			len(absent.Results), len(absent.MyResults), len(absent.Chats), len(absent.Users))
	}

	// The search result is usable as a real client would use it: B joins with
	// the id and access_hash the search handed back, nothing else.
	execChannel(t, ctx, bCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsJoinChannel(ctx, &tg.InputChannel{
			ChannelID:  rendered.ID,
			AccessHash: rendered.AccessHash,
		})
		return err
	})

	// Now a member, B sees the channel in MyResults, rendered as a member.
	afterJoin := search(bCmds, "B", "Kayaking", 10)
	if ids := searchChannelIDs(afterJoin.MyResults); len(ids) != 1 || ids[0] != publicID {
		t.Fatalf("B MyResults after join = %v, want [%d]", ids, publicID)
	}
	joined := searchChannel(afterJoin, publicID)
	if joined == nil {
		t.Fatalf("joined channel %d missing from Chats", publicID)
	}
	if joined.Left {
		t.Error("Chats Left = true after joining, want false")
	}

	// A owns the private channel, so its title is searchable for A.
	aFound := search(aCmds, "A", "Kayaking Private Crew", 10)
	if ids := searchChannelIDs(aFound.MyResults); len(ids) != 1 || ids[0] != privateID {
		t.Fatalf("A MyResults = %v, want [%d]", ids, privateID)
	}
	if ids := searchChannelIDs(aFound.Results); len(ids) != 0 {
		t.Fatalf("A Results = %v, want none — the private channel has no username", ids)
	}

	// A bans B from the public channel. The retained participant row must stop
	// yielding the channel in B's MyResults; the channel is still public, so it
	// stays in Results exactly as it does for any other non-member.
	execChannel(t, ctx, aCmds, func(ctx context.Context, c *tg.Client) error {
		_, err := c.ChannelsEditBanned(ctx, &tg.ChannelsEditBannedRequest{
			Channel:      inputChannel(aUserID, publicID),
			Participant:  peerUser(aUserID, bUserID),
			BannedRights: tg.ChatBannedRights{ViewMessages: true, UntilDate: 0},
		})
		return err
	})

	afterBan := search(bCmds, "B", "Kayaking", 10)
	if ids := searchChannelIDs(afterBan.MyResults); len(ids) != 0 {
		t.Fatalf("banned B MyResults = %v, want none", ids)
	}
	if ids := searchChannelIDs(afterBan.Results); len(ids) != 2 {
		t.Fatalf("banned B Results = %v, want both public channels", ids)
	}
	if banned := searchChannel(afterBan, publicID); banned == nil || !banned.Left {
		t.Fatal("banned B must see the public channel as a non-member does")
	}

	close(aCmds)
	close(bCmds)
	if err := <-errA; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client A run: %v", err)
	}
	if err := <-errB; err != nil && !errors.Is(err, context.Canceled) {
		t.Errorf("client B run: %v", err)
	}
}
