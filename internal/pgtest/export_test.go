package pgtest

// Test-only handles on the readiness internals, so the external test package can
// exercise them without the container-setup path around them.
var (
	ContainerOptions = containerOptions
	WaitAccepting    = waitAccepting
)

// AdminDSN returns the shared container's admin connection string. Valid only
// after a successful Prewarm.
func AdminDSN() string { return adminDSN }
