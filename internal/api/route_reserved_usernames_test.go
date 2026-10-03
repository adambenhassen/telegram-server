package api_test

func routeConflictingUsernameVariants() []string {
	return []string{
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
	}
}
