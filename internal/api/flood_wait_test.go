package api_test

import (
	"fmt"
	"testing"

	"github.com/gotd/td/tgerr"

	"github.com/adambenhassen/telegram-server/internal/api"
)

func TestFloodWaitError(t *testing.T) {
	for _, secs := range []int{1, 5, 60, 86400} {
		t.Run("", func(t *testing.T) {
			e := api.FloodWaitError(secs)
			if e.Code != 420 {
				t.Errorf("code = %d, want 420", e.Code)
			}
			want := fmt.Sprintf("FLOOD_WAIT_%d", secs)
			if e.Message != want {
				t.Errorf("message = %q, want %q", e.Message, want)
			}
		})
	}
}

func TestFloodWaitErrorMinimumOne(t *testing.T) {
	e := api.FloodWaitError(0)
	if e.Code != 420 {
		t.Errorf("code = %d, want 420", e.Code)
	}
	if e.Message != "FLOOD_WAIT_1" {
		t.Errorf("message = %q, want %q", e.Message, "FLOOD_WAIT_1")
	}
}

func TestFloodWaitErrorNegative(t *testing.T) {
	e := api.FloodWaitError(-5)
	if e.Code != 420 {
		t.Errorf("code = %d, want 420", e.Code)
	}
	if e.Message != "FLOOD_WAIT_1" {
		t.Errorf("message = %q, want %q", e.Message, "FLOOD_WAIT_1")
	}
}

func TestFloodWaitErrorIsTgerr(t *testing.T) {
	e := api.FloodWaitError(42)
	if e.Message != "FLOOD_WAIT_42" {
		t.Errorf("message = %q, want %q", e.Message, "FLOOD_WAIT_42")
	}
	if e.Code != 420 {
		t.Errorf("code = %d, want 420", e.Code)
	}
	// Verify it is a tgerr.Error by type assertion.
	var _ = tgerr.Error{Code: e.Code, Message: e.Message}
}
