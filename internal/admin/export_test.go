package admin

import (
	"io"
	"time"
)

// EncodeFragment exposes the SSE wire encoding so the framing rules can be
// asserted without standing up a stream.
func EncodeFragment(f Fragment) []byte {
	return encodeFragment(f)
}

// SSEMaxStreamDuration returns the default bound on one SSE stream's lifetime.
func SSEMaxStreamDuration() time.Duration {
	return sseMaxStreamDuration
}

// IdleTimeout returns the admin session idle timeout.
func IdleTimeout() time.Duration {
	return idleTimeout
}

// RenderDashboard executes the dashboard template with data into w.
// Exposed for package-level integration tests that need to inspect the
// rendered HTML without going through the full HTTP handler.
func RenderDashboard(w io.Writer, data DashboardData) error {
	return dashboardTmpl.Execute(w, data)
}
