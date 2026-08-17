package api

import (
	"time"

	"github.com/gotd/td/tg"
)

// configTTL is how far ahead of the current time help.getConfig sets expires.
// It is the interval an idle client refetches the config on, so it trades
// staleness of the DC list against load from every connected client at once.
const configTTL = time.Hour

// DefaultConfig builds a minimal tg.Config advertising a single DC (ourselves).
// It carries no date or expires: handleGetConfig stamps both from the clock on
// every response, since a value stored here would be the process start time.
func DefaultConfig(dcID int, host string, port int) *tg.Config {
	return &tg.Config{
		ThisDC:   dcID,
		TestMode: false,
		DCOptions: []tg.DCOption{{
			ID:        dcID,
			IPAddress: host,
			Port:      port,
		}},
		DCTxtDomainName:      "",
		ChatSizeMax:          200,
		MegagroupSizeMax:     200000,
		ForwardedCountMax:    100,
		OnlineUpdatePeriodMs: 120000,
		OfflineBlurTimeoutMs: 5000,
		OfflineIdleTimeoutMs: 30000,
		OnlineCloudTimeoutMs: 300000,
		NotifyCloudDelayMs:   30000,
		NotifyDefaultDelayMs: 1500,
		PushChatPeriodMs:     60000,
		PushChatLimit:        2,
		CaptionLengthMax:     1024,
		MessageLengthMax:     4096,
		WebfileDCID:          dcID,
	}
}
