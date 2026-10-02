package api

import "testing"

func TestRouteConflictingUsernamesAreReserved(t *testing.T) {
	for _, username := range []string{
		"apiws", "ApIwS",
		"share", "ShArE",
		"stream", "StReAm",
		"download", "DoWnLoAd",
		"ping", "PiNg",
		"rtmp", "RtMp",
		"hls", "HlS",
		"hls_stream", "HlS_StReAm",
		"hls_quality_file", "HlS_QuAlItY_FiLe",
		"backgrounds", "BaCkGrOuNdS",
	} {
		if !isReservedUsername(username) {
			t.Errorf("isReservedUsername(%q) = false, want true", username)
		}
	}
}
