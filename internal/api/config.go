package api

import "github.com/gotd/td/tg"

// DefaultConfig builds a minimal tg.Config advertising a single DC (ourselves).
func DefaultConfig(dcID int, host string, port int) *tg.Config {
	return &tg.Config{
		Date:     0,
		Expires:  0,
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
