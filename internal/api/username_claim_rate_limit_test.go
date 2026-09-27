package api_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/gotd/td/tg"

	"github.com/adambenhassen/telegram-server/internal/api"
	"github.com/adambenhassen/telegram-server/internal/store"
)

func TestUsernameClaimProbesShareLookupQuotaAndConcurrentBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	s := openStore(t)

	caller, err := s.CreateUser(ctx, "15550000100")
	if err != nil {
		t.Fatalf("caller: %v", err)
	}

	const concurrentProbes = 30
	const occupiedHandles = concurrentProbes + 1
	for i := range occupiedHandles {
		owner, err := s.CreateUser(ctx, fmt.Sprintf("15550000%03d", i))
		if err != nil {
			t.Fatalf("owner %d: %v", i, err)
		}
		handle := fmt.Sprintf("occupied%02d", i)
		if _, err := api.UpdateUsernameForTest(s, owner.ID, handle); err != nil {
			t.Fatalf("claim %q for owner: %v", handle, err)
		}
	}

	channels := make([]store.Channel, 2)
	for i := range channels {
		channel, err := s.CreateChannel(ctx, caller.ID, fmt.Sprintf("Claims %d", i), "", false)
		if err != nil {
			t.Fatalf("channel %d: %v", i, err)
		}
		channels[i] = channel
	}

	if _, err := api.ResolveUsernameForTest(s, caller.ID, &tg.ContactsResolveUsernameRequest{Username: "occupied00"}); err != nil {
		t.Fatalf("resolve occupied handle: %v", err)
	}
	_, err = api.UpdateUsernameForTest(s, caller.ID, "OCCUPIED00")
	if got := rpcMessage(t, err); got != "USERNAME_OCCUPIED" {
		t.Fatalf("repeat occupied claim = %s, want USERNAME_OCCUPIED", got)
	}

	results := make([]error, concurrentProbes)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(concurrentProbes)
	for i := range concurrentProbes {
		go func(i int) {
			defer wg.Done()
			<-start
			handle := fmt.Sprintf("occupied%02d", i+1)
			switch i % 3 {
			case 0:
				_, results[i] = api.UpdateUsernameForTest(s, caller.ID, handle)
			case 1, 2:
				channel := channels[(i/3)%len(channels)]
				_, results[i] = api.EditChannelUsernameForTest(s, caller.ID, &tg.ChannelsUpdateUsernameRequest{
					Channel:  api.InputChannel(caller.ID, channel.ID),
					Username: handle,
				})
			}
		}(i)
	}
	close(start)
	wg.Wait()

	occupied, floodWait := 0, 0
	for i, err := range results {
		if err == nil {
			t.Errorf("probe %d unexpectedly succeeded", i)
			continue
		}
		switch msg := rpcMessage(t, err); msg {
		case "USERNAME_OCCUPIED":
			occupied++
		case "FLOOD_WAIT_86400":
			floodWait++
		default:
			t.Errorf("probe %d error = %s, want USERNAME_OCCUPIED or FLOOD_WAIT_86400", i, msg)
		}
	}

	if occupied != store.UsernameLookupBurstLimit-1 {
		t.Errorf("occupied responses = %d, want %d", occupied, store.UsernameLookupBurstLimit-1)
	}
	if floodWait != concurrentProbes-(store.UsernameLookupBurstLimit-1) {
		t.Errorf("quota responses = %d, want %d", floodWait, concurrentProbes-(store.UsernameLookupBurstLimit-1))
	}

	_, err = api.EditChannelUsernameForTest(s, caller.ID, &tg.ChannelsUpdateUsernameRequest{
		Channel:  api.InputChannel(caller.ID, channels[0].ID),
		Username: "availablechannel",
	})
	if got := rpcMessage(t, err); got != "FLOOD_WAIT_86400" {
		t.Fatalf("free channel claim after quota exhaustion = %s, want FLOOD_WAIT_86400", got)
	}
	channel, ok, err := s.ChannelByID(ctx, channels[0].ID)
	if err != nil || !ok {
		t.Fatalf("load channel after rejected claim: channel=%t err=%v", ok, err)
	}
	if channel.Username != nil {
		t.Errorf("channel username after rejected claim = %q, want unchanged empty username", *channel.Username)
	}

	_, err = api.UpdateUsernameForTest(s, caller.ID, "availableuser")
	if got := rpcMessage(t, err); got != "FLOOD_WAIT_86400" {
		t.Fatalf("free account claim after quota exhaustion = %s, want FLOOD_WAIT_86400", got)
	}
	user, ok, err := s.UserByID(ctx, caller.ID)
	if err != nil || !ok {
		t.Fatalf("load user after rejected claim: user=%t err=%v", ok, err)
	}
	if user.Username != nil {
		t.Errorf("user username after rejected claim = %q, want unchanged empty username", *user.Username)
	}
}
