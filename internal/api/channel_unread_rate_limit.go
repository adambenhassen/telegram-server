package api

import (
	"time"

	"github.com/teagramhq/teagram-server/internal/mtproto"
	"github.com/teagramhq/teagram-server/internal/store"
)

const channelUnreadCountRateLimitSurface = "channel_unread_count"

// Keep exact account-wide summary aggregation within the existing 60-per-minute
// budget used by other authenticated dialog surfaces. The store keys this by
// account, so multiple sessions share the same allowance.
var channelUnreadCountRateLimit = store.RateLimitConfig{Limit: 60, Window: time.Minute}

func (h *handlers) checkChannelUnreadCountRateLimit(req *mtproto.Request) error {
	return h.checkRateLimit(req, channelUnreadCountRateLimitSurface, h.rateLimitChannelUnreadCounts)
}
