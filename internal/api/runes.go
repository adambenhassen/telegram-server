package api

// isDangerousRune reports whether r should be rejected from user-visible text.
// It covers C0/C1 control characters (which corrupt stored names and peer lists
// on every display surface) and bidi override/isolate marks (U+200E, U+200F,
// U+202A–202E, U+2066–2069), which allow visual spoofing — a U+202E override
// before "gnp.exe" renders as "...png" while the bytes remain .exe.
func isDangerousRune(r rune) bool {
	switch {
	case r <= 0x1f, r >= 0x7f && r <= 0x9f:
		return true
	case r == 0x200e, r == 0x200f, r >= 0x202a && r <= 0x202e, r >= 0x2066 && r <= 0x2069:
		return true
	default:
		return false
	}
}
