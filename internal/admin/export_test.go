package admin

import "io"

// RenderDashboard executes the dashboard template with data into w.
// Exposed for package-level integration tests that need to inspect the
// rendered HTML without going through the full HTTP handler.
func RenderDashboard(w io.Writer, data DashboardData) error {
	return dashboardTmpl.Execute(w, data)
}
