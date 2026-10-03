package mtproto

const maxSystemLangCodeHintBytes = 32

// SystemLangCodeHint returns the bounded hint captured from this connection's
// initConnection. It is read on the connection's serve goroutine.
func (c *Conn) SystemLangCodeHint() string {
	return c.systemLangCodeHint
}

func (c *Conn) setSystemLangCodeHint(hint string) {
	if len(hint) > maxSystemLangCodeHintBytes {
		c.systemLangCodeHint = ""
		return
	}
	for i := range len(hint) {
		char := hint[i]
		switch {
		case char >= 'A' && char <= 'Z':
		case char >= 'a' && char <= 'z':
		case char >= '0' && char <= '9':
		case char == '-', char == '_':
		default:
			c.systemLangCodeHint = ""
			return
		}
	}
	c.systemLangCodeHint = hint
}
